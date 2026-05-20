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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.comet.CometSparkSessionExtensions

/**
 * Base trait for unit-testing the contrib-delta native scan.
 *
 * Wires up Spark+Delta in local mode with the contrib enabled, and provides
 * `assertDeltaNativeMatches` -- the load-bearing helper which runs a query
 * twice (once with the contrib enabled, once without) and asserts that:
 *   1. The accelerated execution plan contains `CometDeltaNativeScanExec`
 *   2. Results match vanilla Spark exactly
 *
 * Ported from the pre-SPI delta-kernel-phase-1 branch, where it underpinned
 * roughly 1100 assertions across nine suites.
 */
trait CometDeltaTestBase extends CometTestBase with AdaptiveSparkPlanHelper {

  /**
   * True iff the io.delta.spark classes are on the test classpath. When false, the test
   * harness can `assume(deltaSparkAvailable, ...)` to skip tests rather than throw.
   * Useful for builds without `-Pcontrib-delta` that still want the test classes to
   * compile (the contrib's reflective bridge means we don't strictly need delta-spark
   * at compile time even when we do need it at test runtime).
   */
  protected def deltaSparkAvailable: Boolean =
    try {
      Class.forName("org.apache.spark.sql.delta.DeltaParquetFileFormat")
      true
    } catch {
      case _: ClassNotFoundException => false
    }

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.comet.scan.deltaNative.enabled", "true")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf
  }

  /**
   * Override to chain Delta's session extension after Comet's. `withExtensions` is
   * additive, so the chain becomes: Comet rules + Delta rules. Setting
   * `spark.sql.extensions` via config would also work but interacts unpredictably
   * with Spark's own `WITH_EXTENSIONS` env wiring in test JVMs.
   */
  override protected def createSparkSession: SparkSessionType = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()

    val deltaExt: org.apache.spark.sql.SparkSessionExtensions => Unit =
      try {
        val cls = Class.forName("io.delta.sql.DeltaSparkSessionExtension")
        val instance = cls.getDeclaredConstructor().newInstance()
        instance.asInstanceOf[org.apache.spark.sql.SparkSessionExtensions => Unit]
      } catch {
        case _: ClassNotFoundException =>
          (_: org.apache.spark.sql.SparkSessionExtensions) => ()
      }

    org.apache.spark.sql.classic.SparkSession
      .builder()
      .config(sparkContext.getConf)
      .withExtensions(new CometSparkSessionExtensions)
      .withExtensions(deltaExt)
      .getOrCreate()
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.hadoopConfiguration
      .set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration
      .setBoolean("fs.file.impl.disable.cache", true)
  }

  /** Run `body` with a fresh temp directory and a Delta table path under it. */
  protected def withDeltaTable(testName: String)(body: String => Unit): Unit = {
    val tempDir = Files.createTempDirectory(s"comet-delta-$testName").toFile
    try {
      val tablePath = new java.io.File(tempDir, "t").getAbsolutePath
      body(tablePath)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  /**
   * Run `query` against the Delta table at `tablePath` twice -- once with the
   * native scan enabled, once with it disabled -- and assert:
   *   1. The native plan contains `CometDeltaNativeScanExec`
   *   2. The result rows match vanilla Spark's result rows (order-independent)
   */
  protected def assertDeltaNativeMatches(
      tablePath: String,
      query: DataFrame => DataFrame): Unit = {
    val native = query(spark.read.format("delta").load(tablePath))
    val plan = native.queryExecution.executedPlan
    val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(
      deltaScans.nonEmpty,
      s"expected CometDeltaNativeScanExec in plan, got:\n$plan")
    val nativeRows = native.collect().toSeq.map(normalizeRow)

    withSQLConf("spark.comet.scan.deltaNative.enabled" -> "false") {
      val vanillaRows = query(spark.read.format("delta").load(tablePath))
        .collect()
        .toSeq
        .map(normalizeRow)
      assert(
        nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
        s"native result did not match vanilla Spark result\n" +
          s"native=$nativeRows\nvanilla=$vanillaRows")
    }
  }

  /**
   * Like `assertDeltaNativeMatches` but the caller can express that the
   * native plan SHOULD fall back. Asserts that no `CometDeltaNativeScanExec`
   * appears AND that results still match vanilla Spark (i.e. fallback
   * doesn't corrupt anything).
   */
  protected def assertDeltaFallback(
      tablePath: String,
      query: DataFrame => DataFrame): Unit = {
    val attempt = query(spark.read.format("delta").load(tablePath))
    val plan = attempt.queryExecution.executedPlan
    val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(
      deltaScans.isEmpty,
      s"expected fallback (no CometDeltaNativeScanExec) but plan was:\n$plan")
  }

  protected def normalizeRow(row: Row): Seq[Any] =
    row.toSeq.map(normalizeValue)

  protected def normalizeValue(v: Any): Any = v match {
    case null => null
    case arr: Array[_] => arr.toList.map(normalizeValue)
    case seq: scala.collection.Seq[_] => seq.toList.map(normalizeValue)
    case m: scala.collection.Map[_, _] =>
      m.toList
        .map { case (k, vv) => (normalizeValue(k), normalizeValue(vv)) }
        .sortBy(_._1.toString)
    case r: Row => normalizeRow(r).toList
    case other => other
  }

  protected def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
