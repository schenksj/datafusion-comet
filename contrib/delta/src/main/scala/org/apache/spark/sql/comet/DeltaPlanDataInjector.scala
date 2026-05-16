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

import org.apache.comet.contrib.delta.CometDeltaNativeScan
import org.apache.comet.contrib.delta.proto.DeltaOperator
import org.apache.comet.serde.OperatorOuterClass.{ContribOp, Operator}

/**
 * `PlanDataInjector` for the Delta contrib's `ContribOp`-wrapped `DeltaScan`.
 *
 * The contrib serializes the Delta scan in two parts to keep the closure that gets sent to
 * every task small:
 *   - At planning time `CometDeltaNativeScan.serialize` emits a `DeltaScan` proto with the
 *     `common` block (schemas, table root, filters, ...) and NO tasks; this lands in the
 *     `Operator` tree wrapped in a `ContribOp{kind="delta-scan", payload=DeltaScan}`.
 *   - Per partition, `CometDeltaNativeScanExec.doExecuteColumnar` puts the partition's
 *     `DeltaScan` (tasks-only) bytes into `perPartitionByKey` under a `sourceKey` derived
 *     from the common block.
 *
 * `CometExecRDD.compute` calls `PlanDataInjector.injectPlanData` per partition, which
 * dispatches by injector; this implementation owns the `ContribOp(kind="delta-scan")` case,
 * merges the per-partition `tasks` list into the operator's `common`-only payload, and
 * returns the rebuilt envelope. Without this injection the native side decodes a tasks-empty
 * `DeltaScan` and returns `EmptyExec` (0 rows) for every Delta scan that takes the dispatched
 * native conversion path.
 *
 * Registered with `PlanDataInjector.registerInjector(...)` from
 * `DeltaOperatorSerdeExtension.init`. Visibility is package-internal (`org.apache.spark.sql.comet`)
 * so the injector can be referenced from the registration site without crossing a Scala-private
 * boundary -- same package as the rest of the contrib's exec-side bridge classes.
 */
object DeltaPlanDataInjector extends PlanDataInjector {

  override def canInject(op: Operator): Boolean = {
    if (!op.hasContribOp) return false
    val contribOp = op.getContribOp
    if (contribOp.getKind != CometDeltaNativeScan.DeltaScanKind) return false
    // The common-only proto produced at planning time has zero tasks. After injection
    // the operator carries the partition's tasks -- skip those (idempotent canInject).
    val scan = DeltaOperator.DeltaScan.parseFrom(contribOp.getPayload)
    scan.getTasksCount == 0
  }

  override def getKey(op: Operator): Option[String] = {
    // Mirror `CometDeltaNativeScanExec.computeSourceKey` so the driver-side key and the
    // executor-side lookup key match exactly.
    Some(CometDeltaNativeScanExec.computeSourceKey(op))
  }

  override def inject(
      op: Operator,
      commonBytes: Array[Byte],
      partitionBytes: Array[Byte]): Operator = {
    // `commonBytes` is the `DeltaScanCommon`'s bytes; the contrib doesn't actually send a
    // separate common payload per partition (the planning-time `ContribOp.payload` already
    // carries the common block in full). `partitionBytes` is the serialized `DeltaScan` that
    // packs only this partition's tasks plus the same common block. Merge by taking the
    // partition's payload verbatim and re-wrapping it in the ContribOp envelope.
    //
    // Note: the partition payload is a full `DeltaScan` (with `common` set) rather than a
    // tasks-only proto. This matches how `CometDeltaNativeScanExec.serializedPartitionData`
    // builds it -- the buildPerPartitionBytes routine emits a `DeltaScan.newBuilder()` per
    // partition, sets `tasks` for that partition, but does NOT carry the heavy common block
    // again to avoid duplicating the schemas across partitions. So the partition proto is
    // tasks-only on the wire; we have to splice the original `common` back in here before
    // handing to native.
    val tasksOnlyScan = DeltaOperator.DeltaScan.parseFrom(partitionBytes)
    val originalEnvelope = op.getContribOp
    val originalScan = DeltaOperator.DeltaScan.parseFrom(originalEnvelope.getPayload)
    val mergedScan = DeltaOperator.DeltaScan
      .newBuilder(originalScan)
      .addAllTasks(tasksOnlyScan.getTasksList)
      .build()
    val newEnvelope = ContribOp
      .newBuilder(originalEnvelope)
      .setPayload(mergedScan.toByteString)
      .build()
    op.toBuilder.setContribOp(newEnvelope).build()
  }
}
