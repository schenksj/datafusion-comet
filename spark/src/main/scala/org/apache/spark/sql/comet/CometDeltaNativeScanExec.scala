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

import org.apache.comet.serde.OperatorOuterClass
import org.apache.comet.serde.OperatorOuterClass.Operator

/**
 * Native Delta Lake scan operator with Phase 5 split-mode serialization.
 *
 * Common scan metadata (schemas, filters, projections, storage options, column mappings) is
 * serialized once at planning time in `nativeOp`. Per-partition file lists are materialized
 * lazily in `serializedPartitionData` at execution time so each Spark task receives only its own
 * slice of the file list, reducing driver memory.
 *
 * The `PlanDataInjector` machinery (via `DeltaPlanDataInjector`) merges common + per-partition
 * data at runtime on each executor before the native plan is executed.
 */
case class CometDeltaNativeScanExec(
    override val nativeOp: Operator,
    override val output: Seq[Attribute],
    override val serializedPlanOpt: SerializedPlan,
    @transient originalPlan: FileSourceScanExec,
    tableRoot: String,
    @transient taskListBytes: Array[Byte])
    extends CometLeafExec {

  override val supportsColumnar: Boolean = true

  override val nodeName: String = s"CometDeltaNativeScan $tableRoot"

  /**
   * Lazy split-mode partition serialization. Each element of `perPartitionData` contains a
   * serialized `DeltaScan` message with ONLY the tasks for that partition (no common block).
   * `commonData` contains the serialized `DeltaScanCommon`.
   */
  @transient private lazy val serializedPartitionData: (Array[Byte], Array[Array[Byte]]) = {
    val commonBytes = nativeOp.getDeltaScan.getCommon.toByteArray

    // Parse the full task list returned by planDeltaScan on the driver.
    val taskList = OperatorOuterClass.DeltaScanTaskList.parseFrom(taskListBytes)

    // Each file becomes its own Spark partition for maximum parallelism.
    // Future optimization: merge small files into larger partitions using
    // Spark's maxSplitBytes-based logic.
    val allTasks = taskList.getTasksList.asScala.toSeq
    val perPartitionBytes = if (allTasks.isEmpty) {
      Array.empty[Array[Byte]]
    } else {
      allTasks.map { task =>
        val partScan = OperatorOuterClass.DeltaScan
          .newBuilder()
          .addTasks(task)
          .build()
        partScan.toByteArray
      }.toArray
    }

    (commonBytes, perPartitionBytes)
  }

  def commonData: Array[Byte] = serializedPartitionData._1
  def perPartitionData: Array[Array[Byte]] = serializedPartitionData._2

  def numPartitions: Int = perPartitionData.length

  override lazy val outputPartitioning: Partitioning =
    UnknownPartitioning(math.max(1, numPartitions))

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
      originalPlan = null,
      taskListBytes = null)
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
