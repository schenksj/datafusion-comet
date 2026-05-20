/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet.contrib.delta

import java.util.Locale

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Attribute, AttributeReference, Coalesce, EqualTo, Expression, InputFileBlockLength, InputFileBlockStart, InputFileName, Literal}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.comet.CometScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, MapType, StructField, StructType}

import org.apache.comet.CometConf
import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.parquet.CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}
import org.apache.comet.rules.CometScanRule

/**
 * `CometScanRuleExtension` for Delta tables.
 *
 * Three responsibilities, ported from the pre-SPI `delta-kernel-phase-1` branch's in-core
 * `CometScanRule.scala`:
 *
 *   - [[preTransform]] runs `stripDeltaDvWrappers` -- undoes Delta's `PreprocessTableWithDVs`
 *     Catalyst-strategy rewrite for DV-bearing scans so the clean scan reaches [[transformV1]].
 *     Some scans must stay Spark-native (Delta's reader synthesises a
 *     `__delta_internal_is_row_deleted` column Comet's reader can't); those are tagged with
 *     [[DvProtectedTag]] for `transformV1` to decline.
 *   - [[matchesV1]] probes the relation's file format via reflection (no compile-time
 *     `io.delta.spark` dependency required).
 *   - [[transformV1]] runs `nativeDeltaScan`: schema / encryption / parquet-field-ID gates,
 *     column-mapping metadata re-attachment, row-tracking rewrite, and finally
 *     `CometScanExec(scan, session, CometDeltaNativeScan.ScanImpl)`. [[CometExecRule]] picks up
 *     the marker via [[DeltaOperatorSerdeExtension.matchOperator]] and routes it through
 *     [[CometDeltaNativeScan]].
 *
 * SPI surfaces used:
 *   - `CometScanRule.isSchemaSupported` (private[comet]) -- avoids duplicating ~25 lines of
 *     schema check + fallback-reason emission.
 *   - `CometParquetUtils.{encryptionEnabled, isEncryptionConfigSupported}` -- same.
 *   - `CometSparkSessionExtensions.withInfo` -- same.
 *   - Spark TreeNodeTag for cross-method (preTransform -> transformV1) state passing.
 *
 * The mutable.Set[FileSourceScanExec] of dv-protected scans on the pre-SPI branch is replaced
 * with the TreeNodeTag mechanism, which is the SPI's documented pattern.
 */
/**
 * Static entry points for Delta scan detection / transformation. Called via reflection
 * from core's `org.apache.comet.rules.DeltaIntegration` only when the contrib's classes
 * are bundled into `comet-spark.jar` (i.e. when Maven was invoked with `-Pcontrib-delta`).
 */
object DeltaScanRule {

  import DeltaScanRuleExtension._

  /** Convenience: returns `Some(plan)` if this is a Delta scan we handled. */
  def transformV1IfDelta(
      plan: SparkPlan,
      session: SparkSession,
      scanExec: FileSourceScanExec,
      relation: HadoopFsRelation): Option[SparkPlan] = {
    // Accept either: (a) the relation's fileFormat is DeltaParquetFileFormat, OR (b) the
    // relation's location is a Delta-internal FileIndex (e.g. PreparedDeltaFileIndex). The
    // latter shape occurs when Delta's PreprocessTableWithDVs strategy has already rewritten
    // the scan to plain Parquet over the Delta-internal index -- the underlying table is still
    // Delta, the contrib still needs to handle it.
    val isDeltaScan = DeltaReflection.isDeltaFileFormat(relation.fileFormat) ||
      DeltaReflection.isBatchFileIndex(relation.location)
    if (!isDeltaScan) return None
    val pre = preTransform(plan, session)
    val target = pre.find(_.fastEquals(scanExec)).getOrElse(scanExec).asInstanceOf[FileSourceScanExec]
    transformV1(pre, target, session)
  }

  def preTransform(plan: SparkPlan, session: SparkSession): SparkPlan = {
    if (!DeltaConf.COMET_DELTA_NATIVE_ENABLED.get()) return plan
    stripDeltaDvWrappers(plan)
  }

  private def stripDeltaDvWrappers(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case proj @ ProjectExec(projectList, FilterExec(cond, inner))
          if isDeltaDvFilterPattern(cond) =>
        val userOutput = projectList.map(_.toAttribute)
        if (scanBelowFallsBackForDvs(inner)) {
          collectDeltaScanBelow(inner).foreach(_.setTagValue(DvProtectedTag, ()))
          proj
        } else {
          findAndStripDeltaScanBelow(inner, userOutput).getOrElse(proj)
        }
    }
  }

  private def collectDeltaScanBelow(plan: SparkPlan): Option[FileSourceScanExec] = plan match {
    case scan: FileSourceScanExec
        if DeltaReflection.isDeltaFileFormat(scan.relation.fileFormat) ||
          DeltaReflection.isBatchFileIndex(scan.relation.location) =>
      // Either the fileFormat is `DeltaParquetFileFormat`, OR Delta's
      // `PreprocessTableWithDVs` strategy has already rewritten the scan to
      // plain `ParquetFileFormat` over a Delta-internal FileIndex (e.g.
      // `PreparedDeltaFileIndex`). Both shapes are Delta-originating.
      Some(scan)
    case other if other.children.size == 1 => collectDeltaScanBelow(other.children.head)
    case _ => None
  }

  /**
   * True when the child subtree contains a Delta `FileSourceScanExec` Comet's native path will
   * not apply the DV on. Two shapes both fall back: `TahoeBatchFileIndex` with DV-bearing
   * AddFiles, and any Delta scan whose schema already contains the synthetic
   * `__delta_internal_is_row_deleted` column.
   */
  private def scanBelowFallsBackForDvs(plan: SparkPlan): Boolean = {
    def check(p: SparkPlan): Boolean = p match {
      case scan: FileSourceScanExec
          if DeltaReflection.isDeltaFileFormat(scan.relation.fileFormat) =>
        // Both prior fallback cases are now handled natively:
        //  - `outputHasIsRowDeleted`: native synthesis via #144
        //    (DeltaSyntheticColumnsExec emits the column).
        //  - `batchFallback` (TahoeBatchFileIndex with DVs): the native path
        //    materialises DVs from pre-resolved AddFiles via
        //    `buildTaskListFromAddFiles` + `deletedRowIndexesByPath`. The convert
        //    path declines internally only when DV materialisation itself fails
        //    (CometDeltaNativeScan.scala:479-484), which is the precise failure
        //    mode that warrants a fallback -- not the structural "scan has a
        //    batch index" check this method used to apply unconditionally.
        false
      case other if other.children.size == 1 => check(other.children.head)
      case _ => false
    }
    check(plan)
  }

  /** Matches `__delta_internal_is_row_deleted = 0` (the filter Delta injects). */
  private def isDeltaDvFilterPattern(cond: Expression): Boolean = {
    def isRowDeletedRef(name: String): Boolean =
      name.equalsIgnoreCase(DeltaReflection.IsRowDeletedColumnName)
    cond match {
      case EqualTo(attr: AttributeReference, lit: Literal) if isRowDeletedRef(attr.name) =>
        lit.value != null && lit.value.toString == "0"
      case EqualTo(lit: Literal, attr: AttributeReference) if isRowDeletedRef(attr.name) =>
        lit.value != null && lit.value.toString == "0"
      case _ => false
    }
  }

  private def findAndStripDeltaScanBelow(
      plan: SparkPlan,
      userOutput: Seq[Attribute]): Option[SparkPlan] = plan match {
    case scan: FileSourceScanExec
        if DeltaReflection.isDeltaFileFormat(scan.relation.fileFormat) &&
          scan.output.exists(_.name.equalsIgnoreCase(DeltaReflection.IsRowDeletedColumnName)) =>
      Some(rebuildDeltaScanWithoutDvColumn(scan, userOutput))
    case other if other.children.size == 1 =>
      findAndStripDeltaScanBelow(other.children.head, userOutput)
    case _ => None
  }

  private def rebuildDeltaScanWithoutDvColumn(
      scan: FileSourceScanExec,
      userOutput: Seq[Attribute]): FileSourceScanExec = {
    val dvName = DeltaReflection.IsRowDeletedColumnName
    val scanByExprId = scan.output.map(a => a.exprId -> a).toMap
    val scanByName = scan.output.map(a => a.name.toLowerCase(Locale.ROOT) -> a).toMap
    val resolved = userOutput.map { u =>
      scanByExprId
        .get(u.exprId)
        .orElse(scanByName.get(u.name.toLowerCase(Locale.ROOT)))
        .getOrElse(u)
    }
    val newOutput = resolved.filterNot(_.name == dvName)
    val newRequiredSchema =
      StructType(newOutput.map(a => StructField(a.name, a.dataType, a.nullable)))
    val newDataSchema =
      StructType(scan.relation.dataSchema.fields.filterNot(_.name == dvName))
    val newRelation = scan.relation.copy(dataSchema = newDataSchema)(scan.relation.sparkSession)
    val newDataFilters = scan.dataFilters.filterNot { f =>
      f.references.exists(_.name == dvName)
    }
    scan.copy(
      relation = newRelation,
      output = newOutput,
      requiredSchema = newRequiredSchema,
      dataFilters = newDataFilters)
  }

  // ===========================================================================
  // transformV1: nativeDeltaScan body.
  // ===========================================================================

  def transformV1(
      plan: SparkPlan,
      scanExec: FileSourceScanExec,
      session: SparkSession): Option[SparkPlan] = {
    if (scanExec.getTagValue(DvProtectedTag).isDefined) {
      withInfo(
        scanExec,
        "Leaving scan to Delta so its DV filter above can apply deletion vectors")
      return None
    }
    // Detect references to `input_file_name()` / `input_file_block_*` anywhere in the
    // surrounding plan tree. When present, the contrib's serde MUST emit one task per
    // partition so `CometExecRDD`'s per-partition `InputFileBlockHolder` hook attributes
    // every row to the correct file path. Delta's UPDATE/DELETE/MERGE flows use
    // `input_file_name()` to find "touched files"; without this tag, multiple files
    // packed into one Spark partition share the FIRST task's file path, and Delta
    // rewrites the wrong files (or fails to find rows to rewrite at all). Triggered by
    // tests like `UpdateBaseMiscTests "data and partition predicates -
    // Partition=true Skipping=false"` which has multiple files in one partition.
    val needsInputFileName = plan.exists { node =>
      node.expressions.exists(_.exists {
        case _: InputFileName | _: InputFileBlockStart | _: InputFileBlockLength => true
        case _ => false
      })
    }
    val scanForDelta = if (needsInputFileName) {
      val taggedOptions = scanExec.relation.options +
        (DeltaConf.NeedsInputFileNameOption -> "true")
      val taggedRelation = scanExec.relation.copy(options = taggedOptions)(
        scanExec.relation.sparkSession)
      scanExec.copy(relation = taggedRelation)
    } else scanExec
    nativeDeltaScan(session, scanForDelta, scanForDelta.relation)
  }

  private def nativeDeltaScan(
      session: SparkSession,
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation): Option[SparkPlan] = {
    if (!DeltaConf.COMET_DELTA_NATIVE_ENABLED.get()) {
      withInfo(
        scanExec,
        s"Native Delta scan disabled because ${DeltaConf.COMET_DELTA_NATIVE_ENABLED.key} " +
          "is not enabled")
      return None
    }
    if (!CometConf.COMET_EXEC_ENABLED.get()) {
      withInfo(
        scanExec,
        s"Native Delta scan requires ${CometConf.COMET_EXEC_ENABLED.key} to be enabled")
      return None
    }
    val hadoopConf = r.sparkSession.sessionState.newHadoopConfWithOptions(r.options)
    if (encryptionEnabled(hadoopConf) && !isEncryptionConfigSupported(hadoopConf)) {
      withInfo(scanExec, s"${CometDeltaNativeScan.ScanImpl} does not support encryption config")
      return None
    }
    // CometScanRule.isSchemaSupported is private[comet]; inline the equivalent check
    // (schema check + fallback-reason emission) for the contrib's needs.
    if (!isSchemaCometCompatible(scanExec, r)) {
      return None
    }
    // General-purpose Parquet field-ID matching is now wired through the same path as
    // CM-id mode (#142 commit 7ace165e). When `spark.sql.parquet.fieldId.read.enabled`
    // is true and `scan.requiredSchema` carries the standard `parquet.field.id`
    // metadata, `CometDeltaNativeScan.convert` propagates field IDs into the proto via
    // `serializeDataType`'s StructType arm (which reads `ParquetUtils.hasFieldId`).
    // The convert path also sets `use_field_id=true` so the native parquet reader
    // matches by ID. No gate needed.
    val cmMode = DeltaReflection
      .extractMetadataConfiguration(r)
      .flatMap(_.get("delta.columnMapping.mode"))
    // Column mapping `id` mode is now wired: `CometDeltaNativeScan.convert` translates
    // Delta's `delta.columnMapping.id` -> `parquet.field.id` on every StructField and
    // sets `DeltaScanCommon.use_field_id = true`, which routes the native parquet reader
    // through `schema_adapter.rs` field-ID matching. No gate needed.
    // `checkLatestSchemaOnRead` controls whether Delta's reader does an at-read-time
    // consistency check between the cached DataFrame schema and the latest snapshot.
    // Our native path doesn't do a separate at-read check -- both `column_mappings` and
    // the parquet reads are pinned to the version we get from
    // `DeltaReflection.extractSnapshotVersion(relation)` (i.e. the SAME cached snapshot
    // Spark/Delta used to build scan.requiredSchema). So we're internally consistent
    // regardless of the flag; the user's choice to disable the check only affects
    // Delta's own at-read validation, which we don't perform. No gate needed.
    // Databricks-proprietary file-index variant. The class is not in OSS Delta -- it
    // only exists when running against Databricks Runtime's Delta fork. We don't have
    // an OSS reproducer for its behavior so we conservatively fall back to Spark's
    // Delta reader rather than risk reading via an unknown index that may rely on
    // DBR-only cloud-fetch APIs. If/when this variant is upstreamed (or a customer
    // surfaces a need with adequate test coverage), revisit.
    val fileIndexClassName = r.location.getClass.getName
    if (fileIndexClassName.endsWith(".TahoeLogFileIndexWithCloudFetch")) {
      withInfo(
        scanExec,
        s"Native Delta scan has not validated the cloud-fetch variant ($fileIndexClassName).")
      return None
    }
    val supportedSchemes =
      Set("file", "s3", "s3a", "gs", "gcs", "abfss", "abfs", "wasbs", "wasb", "oss")
    val rootPaths = scanExec.relation.location.rootPaths
    if (rootPaths.nonEmpty) {
      val schemes = rootPaths.map(p => p.toUri.getScheme).filter(_ != null).toSet
      val unsupported = schemes -- supportedSchemes
      if (unsupported.nonEmpty) {
        withInfo(
          scanExec,
          s"Native Delta scan does not support filesystem schemes: " + unsupported.mkString(", "))
        return None
      }
    }
    if (r.location.getClass.getName.contains("PreparedDeltaFileIndex")) {
      try {
        val sample = r.location.inputFiles.take(2)
        sample.foreach { p =>
          val colonSlash = p.indexOf(":/")
          if (colonSlash >= 0) {
            val afterColon = p.substring(colonSlash + 1)
            val scheme = p.substring(0, colonSlash)
            if (!afterColon.startsWith("//") && scheme != "file") {
              withInfo(
                scanExec,
                s"Native Delta scan declines: file path '$p' uses malformed URL form " +
                  s"'$scheme:/...' (real URLs are 'scheme://...'); likely a test-only " +
                  s"shallow-clone mock or cross-filesystem clone our reader can't open.")
              return None
            }
          }
        }
      } catch {
        case scala.util.control.NonFatal(_) => // best-effort; fall through
      }
    }
    val scanWithMappedSchema = withDeltaColumnMappingMetadata(scanExec)
    // Delta's `__delta_internal_row_index` / `__delta_internal_is_row_deleted` synthetic
    // columns are now synthesised natively via `DeltaSyntheticColumnsExec` -- see
    // CometDeltaNativeScan.convert for the schema stripping + proto emit flags, and
    // contrib/delta/native/src/synthetic_columns.rs for the exec.
    applyRowTrackingRewrite(scanWithMappedSchema, r, session).getOrElse {
      Some(CometScanExec(scanWithMappedSchema, session, CometDeltaNativeScan.ScanImpl))
    }
  }

  private def withDeltaColumnMappingMetadata(scanExec: FileSourceScanExec): FileSourceScanExec = {
    val r = scanExec.relation
    val snapshotSchemaOpt = DeltaReflection.extractSnapshotSchema(r)
    if (snapshotSchemaOpt.isEmpty) return scanExec
    val snapshotByName: Map[String, StructField] =
      snapshotSchemaOpt.get.fields.map(f => f.name -> f).toMap
    def attach(f: StructField): StructField =
      snapshotByName.get(f.name) match {
        case Some(meta) =>
          StructField(
            f.name,
            attachDataType(f.dataType, meta.dataType),
            f.nullable,
            meta.metadata)
        case None => f
      }
    def attachDataType(child: DataType, withMeta: DataType): DataType = (child, withMeta) match {
      case (cs: StructType, ms: StructType) =>
        val metaByName = ms.fields.map(f => f.name -> f).toMap
        StructType(cs.fields.map { f =>
          metaByName.get(f.name) match {
            case Some(mf) =>
              StructField(
                f.name,
                attachDataType(f.dataType, mf.dataType),
                f.nullable,
                mf.metadata)
            case None => f
          }
        })
      case (ca: ArrayType, ma: ArrayType) =>
        ArrayType(attachDataType(ca.elementType, ma.elementType), ca.containsNull)
      case (cm: MapType, mm: MapType) =>
        MapType(
          attachDataType(cm.keyType, mm.keyType),
          attachDataType(cm.valueType, mm.valueType),
          cm.valueContainsNull)
      case _ => child
    }
    val newDataFields = r.dataSchema.fields.map(attach)
    val newRequiredFields = scanExec.requiredSchema.fields.map(attach)
    val anyChange = !newDataFields.sameElements(r.dataSchema.fields) ||
      !newRequiredFields.sameElements(scanExec.requiredSchema.fields)
    if (!anyChange) return scanExec
    val newRelation = r.copy(dataSchema = StructType(newDataFields))(r.sparkSession)
    scanExec.copy(relation = newRelation, requiredSchema = StructType(newRequiredFields))
  }

  /**
   * Returns `Some(Some(plan))` when a row-tracking rewrite was applied, `Some(None)` when we
   * detected row-tracking columns we can't translate, and `None` when the scan has no
   * row-tracking columns. Caller uses the outer Option to distinguish "applied" / "decline" / "no
   * rewrite needed".
   */
  private def applyRowTrackingRewrite(
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation,
      session: SparkSession): Option[Option[SparkPlan]] = {
    val RowIdName = DeltaReflection.RowIdColumnName
    val RowCommitVersionName = DeltaReflection.RowCommitVersionColumnName
    val hasRowIdField = scanExec.requiredSchema.fieldNames.exists { n =>
      n.equalsIgnoreCase(RowIdName) || n.equalsIgnoreCase(RowCommitVersionName)
    }
    if (!hasRowIdField) return None

    val cfg = DeltaReflection.extractMetadataConfiguration(r).getOrElse(Map.empty)
    // When `delta.enableRowTracking=false`, the table doesn't track rows so
    // AddFile.baseRowId and AddFile.defaultRowCommitVersion are absent. Our native
    // synthesis path (DeltaSyntheticColumnsExec) handles this by emitting NULL row_id
    // and row_commit_version columns when the per-file base_row_id is None. That
    // matches Delta's own behaviour for these tables -- the column is queryable but
    // returns null. So we just fall through to nativeDeltaScan; CometDeltaNativeScan.convert
    // will detect the columns in scan.requiredSchema and set emit flags.
    if (cfg.get("delta.enableRowTracking").exists(_.equalsIgnoreCase("false"))) {
      return None
    }
    val rowIdPhysical = cfg.get(DeltaReflection.MaterializedRowIdColumnProp)
    val rowVerPhysical = cfg.get(DeltaReflection.MaterializedRowCommitVersionColumnProp)
    if (rowIdPhysical.isEmpty && rowVerPhysical.isEmpty) {
      // No materialised columns -- synthesise row_id (= baseRowId + physical row index)
      // and row_commit_version (= defaultRowCommitVersion) natively via
      // `DeltaSyntheticColumnsExec`. The synthesis path runs through the normal
      // CometDeltaNativeScan.convert flow with the standard `nativeDeltaScan` apply
      // (no rewrite needed here -- convert() detects the row_id / row_commit_version
      // columns in scan.requiredSchema and sets the proto emit flags).
      return None
    }

    val renames = scala.collection.mutable.ArrayBuffer.empty[(String, String)]
    def physicalFor(logical: String): Option[String] =
      if (logical.equalsIgnoreCase(RowIdName)) rowIdPhysical
      else if (logical.equalsIgnoreCase(RowCommitVersionName)) rowVerPhysical
      else None

    val newRequiredFields = scanExec.requiredSchema.fields.map { f =>
      physicalFor(f.name) match {
        case Some(phys) =>
          renames += ((f.name, phys))
          StructField(phys, f.dataType, nullable = true, f.metadata)
        case None => f
      }
    }
    val newDataFields = r.dataSchema.fields.map { f =>
      physicalFor(f.name) match {
        case Some(phys) => StructField(phys, f.dataType, nullable = true, f.metadata)
        case None => f
      }
    }
    if (renames.isEmpty) return None

    val RowIndexColName = "_tmp_metadata_row_index"
    val BaseRowIdColName = "__comet_base_row_id"
    val DefaultRowCommitVersionColName = "__comet_default_row_commit_version"
    val includeRowIdSynth = renames.exists { case (logical, _) =>
      logical.equalsIgnoreCase(RowIdName)
    }
    val includeRowVerSynth = renames.exists { case (logical, _) =>
      logical.equalsIgnoreCase(RowCommitVersionName)
    }
    val needSynth = includeRowIdSynth || includeRowVerSynth

    if (needSynth) {
      val existingNames =
        (r.dataSchema.fieldNames ++ r.partitionSchema.fieldNames)
          .map(_.toLowerCase(Locale.ROOT))
          .toSet
      val syntheticNames = Seq(RowIndexColName, BaseRowIdColName, DefaultRowCommitVersionColName)
      val collisions =
        syntheticNames.filter(n => existingNames.contains(n.toLowerCase(Locale.ROOT)))
      if (collisions.nonEmpty) {
        withInfo(
          scanExec,
          s"Native Delta scan: table has columns that collide with Comet row-tracking " +
            s"synthetic columns (${collisions.mkString(", ")}); falling back.")
        return Some(None)
      }
    }

    val infoByFileName: Map[String, DeltaReflection.RowTrackingFileInfo] =
      if (needSynth) DeltaReflection.extractRowTrackingInfoByFileName(r.location)
      else Map.empty

    val extraRequiredFields = scala.collection.mutable.ArrayBuffer.empty[StructField]
    val extraDataFields = scala.collection.mutable.ArrayBuffer.empty[StructField]
    if (includeRowIdSynth) {
      val rowIndexField = StructField(RowIndexColName, LongType, nullable = true)
      extraRequiredFields += rowIndexField
      extraDataFields += rowIndexField
    }

    val finalRequiredSchema = StructType(newRequiredFields ++ extraRequiredFields)
    val finalDataSchema = StructType(newDataFields ++ extraDataFields)
    val finalLocation =
      if (needSynth) {
        new RowTrackingAugmentedFileIndex(
          r.location,
          infoByFileName,
          BaseRowIdColName,
          DefaultRowCommitVersionColName)
      } else {
        r.location
      }

    val origOutput = scanExec.output
    val renameMap: Map[String, String] = renames.toMap
    val baseNewOutput = origOutput.map { a =>
      renameMap.get(a.name) match {
        case Some(phys) =>
          AttributeReference(phys, a.dataType, nullable = true, a.metadata)(qualifier =
            a.qualifier)
        case None => a
      }
    }
    val rowIndexAttr = AttributeReference(RowIndexColName, LongType, nullable = true)()
    val baseRowIdAttr = AttributeReference(BaseRowIdColName, LongType, nullable = true)()
    val defaultVerAttr =
      AttributeReference(DefaultRowCommitVersionColName, LongType, nullable = true)()
    val extraOutputAttrs = scala.collection.mutable.ArrayBuffer.empty[AttributeReference]
    if (includeRowIdSynth) extraOutputAttrs += rowIndexAttr
    if (needSynth) {
      extraOutputAttrs += baseRowIdAttr
      extraOutputAttrs += defaultVerAttr
    }
    val newOutput: Seq[Attribute] =
      if (extraOutputAttrs.isEmpty) baseNewOutput
      else baseNewOutput ++ extraOutputAttrs

    val newPartitionSchema =
      if (needSynth) {
        r.partitionSchema
          .add(StructField(BaseRowIdColName, LongType, nullable = true))
          .add(StructField(DefaultRowCommitVersionColName, LongType, nullable = true))
      } else {
        r.partitionSchema
      }

    val newRelation = r.copy(
      location = finalLocation,
      dataSchema = finalDataSchema,
      partitionSchema = newPartitionSchema)(r.sparkSession)
    val newScan = scanExec.copy(
      relation = newRelation,
      output = newOutput,
      requiredSchema = finalRequiredSchema)
    val cometScan = CometScanExec(newScan, session, CometDeltaNativeScan.ScanImpl)

    val projectExprs = origOutput.map { a =>
      renameMap.get(a.name).flatMap(phys => baseNewOutput.find(_.name == phys)) match {
        case Some(physAttr) if a.name.equalsIgnoreCase(RowIdName) && includeRowIdSynth =>
          val synth = Add(baseRowIdAttr, rowIndexAttr)
          Alias(Coalesce(Seq(physAttr, synth)), a.name)(
            exprId = a.exprId,
            qualifier = a.qualifier)
        case Some(physAttr)
            if a.name.equalsIgnoreCase(RowCommitVersionName) && includeRowVerSynth =>
          Alias(Coalesce(Seq(physAttr, defaultVerAttr)), a.name)(
            exprId = a.exprId,
            qualifier = a.qualifier)
        case Some(physAttr) =>
          Alias(physAttr, a.name)(exprId = a.exprId, qualifier = a.qualifier)
        case None => a
      }
    }
    Some(Some(ProjectExec(projectExprs, cometScan)))
  }

  /**
   * Inline schema check + fallback-reason emission, mirroring core's
   * `private[comet] CometScanRule.isSchemaSupported`. Kept local to the contrib so the
   * contrib doesn't need to widen core's visibility.
   */
  // Reused across scans -- CometScanTypeChecker is stateless w.r.t. its scanImpl and
  // doesn't mutate per-call state; the per-scan fallback-reasons ListBuffer is the only
  // per-call mutable input.
  private val typeChecker =
    org.apache.comet.rules.CometScanTypeChecker(CometDeltaNativeScan.ScanImpl)

  private def isSchemaCometCompatible(
      scanExec: FileSourceScanExec,
      r: HadoopFsRelation): Boolean = {
    val fallbackReasons = new scala.collection.mutable.ListBuffer[String]()
    val ok = typeChecker.isSchemaSupported(scanExec.requiredSchema, fallbackReasons) &&
      typeChecker.isSchemaSupported(r.partitionSchema, fallbackReasons)
    if (!ok) withInfo(scanExec, fallbackReasons.mkString("; "))
    ok
  }
}

/** Companion holding plan-tree tags used by the static `DeltaScanRule` object. */
object DeltaScanRuleExtension {

  /**
   * Plan-tree tag attached during `preTransform` to mark `FileSourceScanExec`s whose native
   * conversion `transformV1` must decline -- Comet's reader can't produce the
   * `__delta_internal_is_row_deleted` column the outer DV-filter wrapper requires.
   */
  val DvProtectedTag: TreeNodeTag[Unit] =
    TreeNodeTag[Unit]("org.apache.comet.contrib.delta.dv_protected")
}
