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

package org.apache.comet.serde.operator

import java.util.Locale

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, BoundReference, InterpretedPredicate}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometNativeExec, CometScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.apache.comet.{CometConf, ConfigEntry, Native}
import org.apache.comet.delta.DeltaReflection
import org.apache.comet.objectstore.NativeConfig
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel}
import org.apache.comet.serde.ExprOuterClass.Expr
import org.apache.comet.serde.OperatorOuterClass.{DeltaScan, DeltaScanCommon, DeltaScanTaskList, Operator}
import org.apache.comet.serde.QueryPlanSerde.exprToProto

/**
 * Validation and serde logic for the native Delta Lake scan.
 *
 * Unlike `CometNativeScan` / `CometIcebergNativeScan`, this serde does **all** the work
 * synchronously inside `convert()` - no split-mode, no lazy partition serialization. The driver
 * calls `Native.planDeltaScan` to enumerate files via `delta-kernel-rs`, then packs them into a
 * single `DeltaScan` proto alongside the common metadata. All file tasks end up on one Spark
 * partition in Phase 1; per-task parallelism arrives with Phase 5.
 */
object CometDeltaNativeScan extends CometOperatorSerde[CometScanExec] with Logging {

  /** Private lazy handle to the native library - one instance per JVM. */
  private lazy val nativeLib = new Native()

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_DELTA_NATIVE_ENABLED)

  override def getSupportLevel(operator: CometScanExec): SupportLevel = Compatible()

  override def convert(
      scan: CometScanExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {

    // Resolve the table root via the HadoopFsRelation API - standard Spark, no spark-delta
    // compile-time dep required.
    val relation = scan.relation
    val tableRoot = DeltaReflection.extractTableRoot(relation).getOrElse {
      logWarning(
        s"CometDeltaNativeScan: unable to extract table root from relation " +
          s"${relation.location}; falling back to Spark's Delta reader.")
      return None
    }

    // Belt-and-suspenders DV-rewrite gate. The primary gate runs earlier in
    // CometScanRule so the scan never becomes a CometScanExec in the first place.
    // This is a defensive check in case a caller constructs a DV-rewritten
    // CometScanExec by some other path.
    if (scan.requiredSchema.fieldNames.contains(DeltaReflection.IsRowDeletedColumnName)) {
      logWarning(
        "CometDeltaNativeScan: DV-rewritten schema reached serde; this should have " +
          "been caught in CometScanRule. Falling back.")
      return None
    }

    // Cloud storage options, keyed identically to NativeScan. Kernel's DefaultEngine picks
    // up aws_* / azure_* keys; anything else is ignored on the native side (for now).
    //
    // We key off the table root URI rather than `inputFiles.head` because data file names
    // can contain characters that aren't URI-safe when Spark's test harness injects
    // prefixes like `test%file%prefix-` (breaks `java.net.URI.create`). The table root
    // string comes straight from `HadoopFsRelation.location.rootPaths.head.toUri` inside
    // `DeltaReflection.extractTableRoot`, so it's already properly encoded. Storage options
    // are bucket-level anyway - any file under the same root resolves to the same config.
    val hadoopConf =
      relation.sparkSession.sessionState.newHadoopConfWithOptions(relation.options)
    val tableRootUri = java.net.URI.create(tableRoot)
    val storageOptions: java.util.Map[String, String] =
      NativeConfig.extractObjectStoreOptions(hadoopConf, tableRootUri).asJava

    // Honor Delta's time-travel options (versionAsOf / timestampAsOf) via the Delta-
    // resolved snapshot version sitting on the FileIndex. Delta's analysis phase pins
    // the exact snapshot before we ever see the plan, so by the time `CometScanExec` is
    // built, `relation.location` is a `PreparedDeltaFileIndex` whose toString looks like
    // `Delta[version=0, file:/...]`. We parse the version out via
    // `DeltaReflection.extractSnapshotVersion` and pass it through to kernel.
    //
    // When no version can be extracted (non-Delta file index, parser miss, etc.) we pass
    // -1 which asks kernel for the current latest snapshot.
    val snapshotVersion: Long =
      DeltaReflection.extractSnapshotVersion(relation).getOrElse(-1L)

    // --- 1. Ask kernel for the active file list ---
    val taskListBytes =
      try {
        nativeLib.planDeltaScan(tableRoot, snapshotVersion, storageOptions)
      } catch {
        case e: Throwable =>
          logWarning(s"CometDeltaNativeScan: delta-kernel-rs log replay failed for $tableRoot", e)
          return None
      }
    val taskList = DeltaScanTaskList.parseFrom(taskListBytes)

    // Phase 6 reader-feature gate. Kernel reports any Delta reader features that
    // are currently in use in this snapshot and that Comet's native path does NOT
    // correctly handle. Falling back is mandatory for correctness: reading through
    // the native path would silently produce wrong results (e.g. returning rows
    // that a deletion vector should have hidden). The gate becomes obsolete feature
    // by feature as later phases ship:
    //   deletionVectors -> Phase 3
    //   columnMapping   -> Phase 4
    //   typeWidening    -> future phase
    //   rowTracking     -> future phase
    val unsupportedFeatures = taskList.getUnsupportedFeaturesList.asScala.toSeq
    if (unsupportedFeatures.nonEmpty &&
      CometConf.COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE.get(scan.conf)) {
      logInfo(
        s"CometDeltaNativeScan: falling back for table $tableRoot " +
          s"due to unsupported reader features: ${unsupportedFeatures.mkString(", ")}")
      import org.apache.comet.CometSparkSessionExtensions.withInfo
      withInfo(
        scan,
        s"Native Delta scan does not yet support these features in use on this " +
          s"snapshot: ${unsupportedFeatures.mkString(", ")}. Falling back to Spark's " +
          s"Delta reader. Set ${CometConf.COMET_DELTA_FALLBACK_ON_UNSUPPORTED_FEATURE.key}=false " +
          s"to bypass this check (NOT recommended - may produce incorrect results).")
      return None
    }

    // Apply Spark's partition filters to the task list so that queries like
    // `WHERE partition_col = X` don't drag in files from other partitions. Kernel
    // itself is given the whole snapshot (no predicate yet - that lands in Phase 2),
    // so we do the pruning in Scala by evaluating each task's partition-value map
    // against Spark's `partitionFilters`. This is a single driver-side loop; filtered
    // tasks never go over the wire to executors.
    val filteredTasks =
      prunePartitions(taskList.getTasksList.asScala.toSeq, scan, relation.partitionSchema)

    // --- 2. Build the common block ---
    val commonBuilder = DeltaScanCommon.newBuilder()
    commonBuilder.setSource(scan.simpleStringWithNodeId())
    commonBuilder.setTableRoot(taskList.getTableRoot)
    commonBuilder.setSnapshotVersion(taskList.getSnapshotVersion)
    commonBuilder.setSessionTimezone(scan.conf.getConfString("spark.sql.session.timeZone"))
    commonBuilder.setCaseSensitive(scan.conf.getConf[Boolean](SQLConf.CASE_SENSITIVE))
    commonBuilder.setDataFileConcurrencyLimit(
      CometConf.COMET_DELTA_DATA_FILE_CONCURRENCY_LIMIT.get())

    // Schemas. Delta is different from vanilla Parquet: `relation.dataSchema` on a Delta
    // table INCLUDES partition columns, but the physical parquet files on disk do NOT.
    // So we compute the actual file schema by subtracting the partition columns from
    // `relation.dataSchema`. Mirrors what delta-kernel itself reports as the scan schema.
    val partitionNames =
      relation.partitionSchema.fields.map(_.name.toLowerCase(Locale.ROOT)).toSet
    val fileDataSchemaFields =
      relation.dataSchema.fields.filterNot(f =>
        partitionNames.contains(f.name.toLowerCase(Locale.ROOT)))

    val dataSchema = schema2Proto(fileDataSchemaFields)
    val requiredSchema = schema2Proto(scan.requiredSchema.fields)
    val partitionSchema = schema2Proto(relation.partitionSchema.fields)
    commonBuilder.addAllDataSchema(dataSchema.toIterable.asJava)
    commonBuilder.addAllRequiredSchema(requiredSchema.toIterable.asJava)
    commonBuilder.addAllPartitionSchema(partitionSchema.toIterable.asJava)

    // Projection vector maps output-schema positions to (file_data_schema ++
    // partition_schema) indices. Same convention as CometNativeScan.convert: first the
    // data-column indexes in file schema order, then ALL partition columns appended.
    val dataSchemaIndexes = scan.requiredSchema.fields.map { field =>
      val nameLower = field.name.toLowerCase(Locale.ROOT)
      fileDataSchemaFields.indexWhere(_.name.toLowerCase(Locale.ROOT) == nameLower)
    }
    val partitionSchemaIndexes =
      (0 until relation.partitionSchema.fields.length).map(i => fileDataSchemaFields.length + i)
    val projectionVector = dataSchemaIndexes ++ partitionSchemaIndexes
    commonBuilder.addAllProjectionVector(
      projectionVector.map(idx => idx.toLong.asInstanceOf[java.lang.Long]).toIterable.asJava)

    // Pushed-down data filters. Gated by Spark's parquet filter pushdown config, same as
    // CometNativeScan, so we behave consistently across scan implementations.
    if (scan.conf.getConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED) &&
      CometConf.COMET_RESPECT_PARQUET_FILTER_PUSHDOWN.get(scan.conf)) {
      val dataFilters = new ListBuffer[Expr]()
      scan.supportedDataFilters.foreach { filter =>
        exprToProto(filter, scan.output) match {
          case Some(proto) => dataFilters += proto
          case _ => logWarning(s"CometDeltaNativeScan: unsupported data filter $filter")
        }
      }
      commonBuilder.addAllDataFilters(dataFilters.asJava)
    }

    storageOptions.asScala.foreach { case (key, value) =>
      commonBuilder.putObjectStoreOptions(key, value)
    }

    // --- 3. Pack everything into a DeltaScan ---
    val deltaScanBuilder = DeltaScan.newBuilder()
    deltaScanBuilder.setCommon(commonBuilder.build())
    deltaScanBuilder.addAllTasks(filteredTasks.asJava)

    builder.clearChildren()
    Some(builder.setDeltaScan(deltaScanBuilder.build()).build())
  }

  /**
   * Filter `tasks` down to the subset whose partition values satisfy Spark's
   * `scan.partitionFilters`. Returns the original list unchanged when the scan has no partition
   * filters.
   *
   * Delta stores partition values as strings inside add actions, so we parse each value into the
   * correct Catalyst type using `castPartitionString` below before feeding it to an
   * `InterpretedPredicate`. Only values for fields actually referenced by the predicate need
   * parsing, but we do the full row for simplicity.
   */
  private def prunePartitions(
      tasks: Seq[OperatorOuterClass.DeltaScanTask],
      scan: CometScanExec,
      partitionSchema: StructType): Seq[OperatorOuterClass.DeltaScanTask] = {
    if (scan.partitionFilters.isEmpty || partitionSchema.isEmpty) return tasks

    // Build an `InterpretedPredicate` that expects a row whose schema matches
    // `partitionSchema`. Rewrite attribute references to `BoundReference`s keyed by
    // partition-schema column name so it can evaluate against a row we assemble below.
    val partitionAttrsByName =
      scan.partitionFilters.flatMap(_.references).groupBy(_.name.toLowerCase(Locale.ROOT))
    val combined = scan.partitionFilters.reduce(And)
    val bound = combined.transform {
      case a: org.apache.spark.sql.catalyst.expressions.AttributeReference =>
        val idx = partitionSchema.fieldIndex(a.name)
        BoundReference(idx, partitionSchema(idx).dataType, partitionSchema(idx).nullable)
    }
    val _ = partitionAttrsByName
    val predicate = InterpretedPredicate(bound)
    predicate.initialize(0)

    tasks.filter { task =>
      val row = InternalRow.fromSeq(partitionSchema.fields.toSeq.map { field =>
        val proto = task.getPartitionValuesList.asScala.find(_.getName == field.name)
        val strValue =
          if (proto.exists(_.hasValue)) Some(proto.get.getValue) else None
        castPartitionString(strValue, field.dataType)
      })
      predicate.eval(row)
    }
  }

  /**
   * Convert a Delta partition value (always a string, always stored in the add action) into a
   * Catalyst-internal representation for the given data type. Returns `null` for null/unset
   * values. Handles the subset of primitive types Delta supports for partition columns: string,
   * int, long, short, byte, float, double, boolean, date, timestamp, and decimal. Other types
   * fall back to a string representation.
   */
  private def castPartitionString(str: Option[String], dt: DataType): Any = str match {
    case None => null
    case Some(s) if s == null => null
    case Some(s) =>
      dt match {
        case StringType => UTF8String.fromString(s)
        case IntegerType => s.toInt
        case LongType => s.toLong
        case ShortType => s.toShort
        case ByteType => s.toByte
        case FloatType => s.toFloat
        case DoubleType => s.toDouble
        case BooleanType => s.toBoolean
        case DateType => DateTimeUtils.stringToDate(UTF8String.fromString(s)).get
        case _: TimestampType =>
          DateTimeUtils.stringToTimestamp(UTF8String.fromString(s), java.time.ZoneOffset.UTC).get
        case d: DecimalType =>
          val dec = org.apache.spark.sql.types.Decimal(new java.math.BigDecimal(s))
          dec.changePrecision(d.precision, d.scale)
          dec
        case _ => UTF8String.fromString(s)
      }
  }

  override def createExec(nativeOp: Operator, op: CometScanExec): CometNativeExec = {
    val tableRoot = DeltaReflection.extractTableRoot(op.relation).getOrElse("unknown")
    CometDeltaNativeScanExec(
      nativeOp,
      op.output,
      org.apache.spark.sql.comet.SerializedPlan(None),
      op.wrapped,
      tableRoot)
  }
}
