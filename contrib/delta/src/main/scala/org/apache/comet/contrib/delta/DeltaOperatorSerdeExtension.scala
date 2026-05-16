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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.comet.{CometDeltaNativeScanExec, CometExecRDD, CometScanExec, DeltaInputFileBlockHolder, DeltaPlanDataInjector, PlanDataInjector}
import org.apache.spark.sql.execution.SparkPlan

import org.apache.comet.contrib.delta.proto.DeltaOperator
import org.apache.comet.serde.CometOperatorSerde
import org.apache.comet.spi.CometOperatorSerdeExtension

/**
 * Discovered via `META-INF/services/org.apache.comet.spi.CometOperatorSerdeExtension`. Three SPI
 * surfaces are exercised:
 *
 *   - `serdes`: class-keyed serde for `CometDeltaNativeScanExec` (the post-conversion native exec
 *     class the contrib defines). Picked up after the native plan is built.
 *   - `matchOperator`: predicate-keyed serde for the `CometScanExec` marker the contrib's
 *     `DeltaScanRuleExtension.transformV1` returns. `CometScanExec` is shared with core's generic
 *     file-scan dispatch, so we can't route on class alone -- `scanImpl ==
 *     CometDeltaNativeScan.ScanImpl` is the disambiguator.
 *   - `nativeParquetScanImpls`: declares that scans tagged with our scanImpl go through Comet's
 *     tuned ParquetSource. `CometScanExec.supportedDataFilters` consults this set to decide
 *     whether to drop dynamic-pruning filters + IsNull/IsNotNull on ArrayType columns the same
 *     way it does for `SCAN_NATIVE_DATAFUSION`.
 */
class DeltaOperatorSerdeExtension extends CometOperatorSerdeExtension with Logging {
  override def name: String = "delta"

  // No class-keyed serdes. The marker `CometScanExec(scanImpl=native_delta_compat)` is the only
  // shape that needs serde dispatch (handled by `matchOperator` below); the post-conversion
  // `CometDeltaNativeScanExec` is already a `CometNativeExec` and is preserved by core's
  // `case _: CometPlan` arm in `CometExecRule.convertNode`. Registering the post-conversion exec
  // here would cause re-dispatch with the wrong static type at `getSupportLevel(operator: T)`
  // (T = CometScanExec) and crash with ClassCastException.
  override def serdes: Map[Class[_ <: SparkPlan], CometOperatorSerde[_]] = Map.empty

  override def matchOperator(op: SparkPlan): Option[CometOperatorSerde[_]] = op match {
    case s: CometScanExec if s.scanImpl == CometDeltaNativeScan.ScanImpl =>
      Some(CometDeltaNativeScan)
    case _ => None
  }

  override def nativeParquetScanImpls: Set[String] = Set(CometDeltaNativeScan.ScanImpl)

  /**
   * Register the executor-side per-partition metadata handler. Wired through PR1's generic
   * SPI on `CometExecRDD` so core stays format-agnostic. The handler populates Spark's
   * `InputFileBlockHolder` from the per-partition `DeltaScan` payload so:
   *   - `input_file_name()` returns the file path Delta committed (not empty),
   *   - Delta's `_metadata.file_path` resolves to the AddFile its UPDATE/DELETE/MERGE
   *     `getTouchedFile` lookup expects (otherwise: `DELTA_FILE_TO_OVERWRITE_NOT_FOUND`).
   *
   * Conventions the handler honors:
   *   - No-op when the partition has no plan data, or when no key's payload parses as a
   *     `DeltaScan` proto with at least one task (another contrib may own the key).
   *   - Each Delta partition reads exactly one file when `input_file_name()` is requested
   *     (the rule's `oneTaskPerPartition` path), so reading the first task is correct.
   *   - Registers a `TaskCompletionListener` to `unset()` the holder so the value doesn't
   *     leak into subsequent tasks on the same executor thread.
   */
  override def init(): Unit = {
    CometExecRDD.registerPartitionMetadataHandler(
      DeltaOperatorSerdeExtension.setInputFileForDeltaScan)
    PlanDataInjector.registerInjector(DeltaPlanDataInjector)
  }
}

object DeltaOperatorSerdeExtension extends Logging {
  private[delta] def setInputFileForDeltaScan(
      planDataByKey: Map[String, Array[Byte]],
      context: TaskContext): Unit = {
    if (planDataByKey.isEmpty) return
    planDataByKey.values.view
      .flatMap { bytes =>
        try {
          val scan = DeltaOperator.DeltaScan.parseFrom(bytes)
          if (scan.getTasksCount > 0) {
            val t = scan.getTasks(0)
            Some((t.getFilePath, t.getFileSize))
          } else None
        } catch {
          // Parse failure is expected on partitions owned by another contrib whose proto
          // shape doesn't match -- silently skip. Debug-logging it would be too noisy in
          // a session that has multiple contribs registered.
          case _: Throwable => None
        }
      }
      .headOption
      .foreach { case (filePath, fileSize) =>
        DeltaInputFileBlockHolder.set(filePath, fileSize, context)
      }
  }
}
