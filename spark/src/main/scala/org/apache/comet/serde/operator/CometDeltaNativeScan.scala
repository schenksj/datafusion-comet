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
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometNativeExec, CometScanExec}
import org.apache.spark.sql.internal.SQLConf

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

    // --- 1. Ask kernel for the active file list ---
    val taskListBytes =
      try {
        nativeLib.planDeltaScan(tableRoot, -1L, storageOptions)
      } catch {
        case e: Throwable =>
          logWarning(s"CometDeltaNativeScan: delta-kernel-rs log replay failed for $tableRoot", e)
          return None
      }
    val taskList = DeltaScanTaskList.parseFrom(taskListBytes)

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
    deltaScanBuilder.addAllTasks(taskList.getTasksList)

    builder.clearChildren()
    Some(builder.setDeltaScan(deltaScanBuilder.build()).build())
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
