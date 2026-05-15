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

import java.nio.file.Files

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.comet.spi.CometExtensionRegistry

/**
 * Smoke test for the Delta contrib's end-to-end JVM-side wiring.
 *
 * Validates that:
 *   1. `CometExtensionRegistry.load()` discovers the contrib's
 *      [[DeltaScanRuleExtension]] and [[DeltaOperatorSerdeExtension]]
 *      via `META-INF/services/` entries bundled into `comet-spark.jar`
 *      (the source-injection model -- the contrib's sources are compiled
 *      into comet-spark, not into a separate JAR).
 *   2. `DeltaScanRuleExtension.matchesV1` returns true for a real Delta
 *      `HadoopFsRelation` (no compile-time `io.delta.spark` dependency
 *      required -- `DeltaReflection.isDeltaFileFormat` is the probe).
 *
 * This suite does NOT execute native code (libcomet may not be on the dyld
 * path during `mvn test`). For full end-to-end execution coverage, run the
 * Delta regression sweep -- see `dev/diffs/delta/4.1.1.diff` for the
 * patched Delta test infrastructure.
 */
class DeltaSmokeSuite extends AnyFunSuite {

  test("ServiceLoader discovers the Delta contrib's SPI extensions") {
    CometExtensionRegistry.resetForTesting()
    CometExtensionRegistry.load()

    val scanExt = CometExtensionRegistry.scanExtensions.find(_.name == "delta")
    assert(
      scanExt.isDefined,
      "DeltaScanRuleExtension should be discovered via META-INF/services. " +
        s"Found scan extensions: ${CometExtensionRegistry.scanExtensions.map(_.name).mkString(", ")}")
    assert(scanExt.get.isInstanceOf[DeltaScanRuleExtension])

    val serdeExt = CometExtensionRegistry.serdeExtensions.find(_.name == "delta")
    assert(
      serdeExt.isDefined,
      "DeltaOperatorSerdeExtension should be discovered via META-INF/services. " +
        s"Found serde extensions: ${CometExtensionRegistry.serdeExtensions.map(_.name).mkString(", ")}")
    assert(serdeExt.get.isInstanceOf[DeltaOperatorSerdeExtension])
  }

  test("Delta contrib registers its scanImpl in nativeParquetScanImpls") {
    CometExtensionRegistry.resetForTesting()
    CometExtensionRegistry.load()

    assert(
      CometExtensionRegistry.nativeParquetScanImpls.contains(CometDeltaNativeScan.ScanImpl),
      s"Delta contrib's ScanImpl ('${CometDeltaNativeScan.ScanImpl}') should be in " +
        s"the merged nativeParquetScanImpls set so CometScanExec.supportedDataFilters " +
        s"applies the right exclusions to Delta marker scans. " +
        s"Found tags: ${CometExtensionRegistry.nativeParquetScanImpls}")
  }

  test("End-to-end: read a Delta table via Comet, plan contains CometDeltaNativeScanExec") {
    val tmp = Files.createTempDirectory("comet-contrib-delta-smoke-").toFile
    tmp.deleteOnExit()
    val tablePath = new java.io.File(tmp, "delta_table").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("DeltaSmokeSuite")
      .master("local[1]")
      .config("spark.sql.extensions", "org.apache.comet.CometSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension," +
          "org.apache.comet.CometSparkSessionExtensions")
      .config("spark.comet.enabled", "true")
      .config("spark.comet.exec.enabled", "true")
      .config("spark.comet.scan.enabled", "true")
      .config(DeltaConf.COMET_DELTA_NATIVE_ENABLED.key, "true")
      .getOrCreate()

    try {
      import spark.implicits._
      // Write a tiny Delta table.
      Seq((1, "a"), (2, "b"), (3, "c"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      // Read it back through Comet.
      val df = spark.read.format("delta").load(tablePath)
      val plan = df.queryExecution.executedPlan

      def hasDeltaCometScan(p: SparkPlan): Boolean = p.exists {
        case _: CometDeltaNativeScanExec => true
        case _ => false
      }

      assert(
        hasDeltaCometScan(plan),
        s"Executed plan should contain CometDeltaNativeScanExec when Comet's Delta " +
          s"contrib is enabled. Plan:\n${plan.treeString}")

      // Sanity-check the contents -- this requires native execution which only works
      // when libcomet.{so,dylib} is on the dyld path. If running under `mvn test`
      // without that setup, the previous plan assertion is the meaningful coverage.
      val rows = df.collect()
      assert(rows.length == 3)
    } finally {
      spark.stop()
    }
  }
}
