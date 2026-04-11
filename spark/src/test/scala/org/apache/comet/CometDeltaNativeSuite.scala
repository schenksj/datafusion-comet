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

package org.apache.comet

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Phase 1 smoke test for the native Delta Lake scan path.
 *
 * Exercises the full pipeline end-to-end through Spark:
 *   1. Enables `spark.comet.scan.deltaNative.enabled`. 2. Registers delta-spark's session
 *      extension + catalog so `df.write.format("delta")` is available in tests. 3. Writes a tiny
 *      Delta table to a tempdir. 4. Reads it back through Comet, asserting the plan contains
 *      `CometDeltaNativeScanExec` and the row contents match vanilla Spark+Delta.
 *
 * Skipped automatically if `delta-spark` is not on the test classpath (guards against running in
 * profiles where we haven't added the test dep yet).
 */
class CometDeltaNativeSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  private def deltaSparkAvailable: Boolean =
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set(CometConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf
  }

  test("read a tiny unpartitioned delta table via the native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")

    val tempDir = Files.createTempDirectory("comet-delta-smoke").toFile
    try {
      val tablePath = new java.io.File(tempDir, "t").getAbsolutePath
      val ss = spark
      import ss.implicits._

      // --- Write via delta-spark ---
      val rows = (0 until 10).map(i => (i.toLong, s"name_$i", i * 1.5))
      rows
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // --- Read via Comet (native Delta path) ---
      val df = spark.read.format("delta").load(tablePath)
      val executed = df.queryExecution.executedPlan

      // Plan must contain the native Delta scan exec.
      val hasDeltaScan = executed.collect { case s: CometDeltaNativeScanExec =>
        s
      }.nonEmpty
      assert(hasDeltaScan, s"expected CometDeltaNativeScanExec in plan, got:\n$executed")

      // Row contents must match the vanilla Spark+Delta read.
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark.read.format("delta").load(tablePath).collect().toSeq
        val native = df.collect().toSeq
        assert(
          native.sortBy(_.getLong(0)) == vanilla.sortBy(_.getLong(0)),
          s"native result did not match vanilla Spark result\n" +
            s"native=$native\nvanilla=$vanilla")
      }
    } finally {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
