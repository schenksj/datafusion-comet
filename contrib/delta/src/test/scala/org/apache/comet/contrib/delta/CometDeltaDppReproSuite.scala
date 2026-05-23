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

import org.apache.spark.sql.comet.CometDeltaNativeScanExec

// Repro for regression family F1 (DPP):
//   MergeIntoSuite "...isPartitioned: true" fails with
//   "CometSubqueryAdaptiveBroadcastExec (should have been converted by
//   CometPlanAdaptiveDynamicPruningFilters) does not support the execute()
//   code path."
//
// Trigger: a broadcast hash join where a partitioned Delta table is the probe
// side and the join key is the partition column, so AQE+DPP inserts a dynamic
// partition-pruning InSubquery over the scan. CometExecRule wraps it as
// CometSubqueryAdaptiveBroadcastExec; CometPlanAdaptiveDynamicPruningFilters
// must convert it but does not for CometDeltaNativeScanExec.
//
// useStats=false forces DPP insertion regardless of the cost-benefit estimate
// (small test tables otherwise skip DPP).
class CometDeltaDppReproSuite extends CometDeltaTestBase {

  test("DPP broadcast join over partitioned Delta scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withSQLConf(
      "spark.sql.optimizer.dynamicPartitionPruning.enabled" -> "true",
      "spark.sql.optimizer.dynamicPartitionPruning.useStats" -> "false",
      "spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly" -> "true",
      "spark.sql.exchange.reuse" -> "true",
      "spark.sql.autoBroadcastJoinThreshold" -> "10485760") {
      withDeltaTable("dpp_join") { tablePath =>
        val ss = spark
        import ss.implicits._
        (0 until 2000)
          .map(i => (i.toLong, (i % 50).toLong, s"v_$i"))
          .toDF("id", "pkey", "v")
          .write
          .format("delta")
          .partitionBy("pkey")
          .save(tablePath)
        spark.read.format("delta").load(tablePath).createOrReplaceTempView("fact")

        withDeltaTable("dpp_dim") { dimPath =>
          (0 until 50)
            .map(i => (i.toLong, if (Set(3, 7, 11).contains(i)) "keep" else "drop"))
            .toDF("dimkey", "flag")
            .write.format("delta").save(dimPath)
          spark.read.format("delta").load(dimPath).createOrReplaceTempView("dim")

          val df = spark.sql(
            """SELECT f.id, f.pkey, f.v
              |FROM fact f JOIN dim d ON f.pkey = d.dimkey
              |WHERE d.flag = 'keep'""".stripMargin)
          val rows = df.collect()
          val plan = df.queryExecution.executedPlan
          val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
          // Native scan must engage (not fall back to Spark's Delta reader) and
          // must not crash on the DPP subquery (the original F1 regression).
          assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan:\n$plan")
          // Correctness: result equals all fact rows whose pkey is a kept dim key.
          // (Holds whether or not DPP pruning fires -- if it doesn't, the join
          // still filters; the scan just reads more partitions.)
          val expected = (0 until 2000).count(i => Set(3, 7, 11).contains(i % 50))
          assert(rows.length == expected, s"got ${rows.length} want $expected")
        }
      }
    }
  }
}
