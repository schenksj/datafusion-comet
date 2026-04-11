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

package org.apache.spark.sql.comet

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.FileSourceScanExec

import com.google.common.base.Objects

import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native Delta Lake scan operator.
 *
 * Leaf exec that wraps a pre-built `DeltaScan` protobuf operator. The Scala side builds the
 * operator once during plan conversion (in `CometDeltaNativeScan.convert`), which calls
 * `Native.planDeltaScan` on the driver to replay the Delta transaction log via `delta-kernel-rs`
 * and materialize the file list. The resulting native plan is executed via the standard
 * `CometExecRDD` machinery inherited from `CometNativeExec`.
 *
 * Phase 1 scope:
 *   - Single Spark partition (all file tasks in one task). Split-mode serialization and
 *     per-partition parallelism arrive with Phase 5.
 *   - No Dynamic Partition Pruning. Phase 5.
 *   - No deletion vectors. Phase 3.
 *   - No per-task residual predicates. Phase 5.
 *
 * Shape is intentionally tiny compared with `CometIcebergNativeScanExec` because everything
 * DPP-adjacent is deferred. When those features land we'll grow this class to mirror the Iceberg
 * exec more closely (lazy `serializedPartitionData`, `doPrepare` DPP hook, etc.).
 */
case class CometDeltaNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan,
    @transient originalPlan: FileSourceScanExec,
    tableRoot: String)
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = s"CometDeltaNativeScan $tableRoot"

  // Single-partition output for Phase 1 - we execute all Delta file tasks on one Spark task.
  // Phase 5 introduces per-file task parallelism via split-mode serialization, at which
  // point this becomes `UnknownPartitioning(numDeltaPartitions)`.
  override lazy val outputPartitioning: Partitioning = UnknownPartitioning(1)

  override lazy val outputOrdering: Seq[SortOrder] = Nil

  override def convertBlock(): CometDeltaNativeScanExec = {
    val newSerializedPlan = if (serializedPlanOpt.isEmpty) {
      val bytes = CometExec.serializeNativePlan(nativeOp)
      SerializedPlan(Some(bytes))
    } else {
      serializedPlanOpt
    }
    copy(serializedPlanOpt = newSerializedPlan)
  }

  override protected def doCanonicalize(): CometDeltaNativeScanExec = {
    copy(
      output = output.map(QueryPlan.normalizeExpressions(_, output)),
      serializedPlanOpt = SerializedPlan(None),
      originalPlan = null)
  }

  override def stringArgs: Iterator[Any] = Iterator(output, tableRoot)

  override def equals(obj: Any): Boolean = obj match {
    case other: CometDeltaNativeScanExec =>
      tableRoot == other.tableRoot &&
      output == other.output &&
      serializedPlanOpt == other.serializedPlanOpt
    case _ => false
  }

  override def hashCode(): Int =
    Objects.hashCode(tableRoot, output.asJava, serializedPlanOpt)
}
