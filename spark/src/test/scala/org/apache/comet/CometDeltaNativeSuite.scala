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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.comet.CometDeltaNativeScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * Phase 1 suite for the native Delta Lake scan path. Each test:
 *   1. Writes a Delta table via delta-spark's DataFrame API. 2. Reads it back through Comet's
 *      native scan. 3. Asserts `CometDeltaNativeScanExec` appears in the physical plan. 4.
 *      Asserts the native result matches vanilla Spark+Delta row-for-row.
 *
 * Auto-skips when delta-spark is not on the classpath so the suite is a no-op on Spark profiles
 * where we haven't added the test dep yet.
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
    // Explicitly pin local filesystem. See beforeAll for why.
    conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    // When DELTA_TESTING (or similar) is set in the JVM environment, Delta injects
    // `test%file%prefix-` onto every data-file name and `test%dv%prefix-` onto every
    // deletion-vector file name. These are test-only knobs meant to catch bugs in
    // Delta's own test suite. They break Comet's Delta path because delta-kernel-rs
    // reads the transaction log as-is (without applying the prefix at read time) and
    // then tries to open files by names that differ from what's actually on disk.
    // Explicitly clear the prefixes so production-shaped filenames are used.
    conf.set("spark.databricks.delta.testOnly.dataFileNamePrefix", "")
    conf.set("spark.databricks.delta.testOnly.dvFileNamePrefix", "")
    // Delta 3.x's default `useMetadataRowIndex=true` strategy rewrites DV-in-use
    // scans to read the parquet file with `_metadata.row_index` + other metadata
    // columns, and applies the DV *inside* `DeletionVectorBoundFileFormat` at read
    // time - no Filter is inserted in the plan. That makes the DV completely
    // opaque to any physical-plan rewrite: there's nothing to detect.
    // Setting this to false falls Delta back to its older strategy that DOES
    // insert `Project -> Filter(__delta_internal_is_row_deleted = 0) -> scan`,
    // which our `stripDeltaDvWrappers` rewrite can recognize and unwind.
    conf.set("spark.databricks.delta.deletionVectors.useMetadataRowIndex", "false")
    conf
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // CometTestBase installs Spark's DebugFilesystem as the `file://` filesystem
    // implementation. DebugFilesystem injects a `test%file%prefix-` prefix onto
    // every created file to detect read-after-close bugs in Spark tests. That
    // interacts badly with Delta's deletion-vector writes: Delta records the
    // *unprefixed* DV filename in the transaction log (because it calls
    // `FileSystem.create(path)` with a clean path and doesn't observe the
    // prefix the DebugFilesystem adds), so when kernel later fetches the DV by
    // the log-recorded path, the file doesn't exist under that name.
    //
    // Override the filesystem back to the plain local implementation for this
    // suite only, on both the session state conf (for future reads) AND the
    // hadoopConfiguration on the running context (for writes that are about to
    // happen). Production users never hit this because they don't use the
    // test harness.
    spark.sparkContext.hadoopConfiguration
      .set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.sparkContext.hadoopConfiguration
      .setBoolean("fs.file.impl.disable.cache", true)
    spark.sessionState.conf
      .setConfString("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
  }

  /** Run `body` with a fresh Delta table tempdir, cleaning up afterwards. */
  private def withDeltaTable(testName: String)(body: String => Unit): Unit = {
    val tempDir = Files.createTempDirectory(s"comet-delta-$testName").toFile
    try {
      val tablePath = new java.io.File(tempDir, "t").getAbsolutePath
      body(tablePath)
    } finally {
      deleteRecursively(tempDir)
    }
  }

  /**
   * Run `query` against the Delta table at `tablePath`, assert that `CometDeltaNativeScanExec` is
   * in the physical plan, and assert the returned rows match what vanilla Spark+Delta would
   * return.
   *
   * `collectDeltaScans` uses `AdaptiveSparkPlanHelper.collect` so AQE nodes
   * (`AdaptiveSparkPlanExec`) don't hide the inner scan from the match.
   */
  private def assertDeltaNativeMatches(tablePath: String, query: DataFrame => DataFrame): Unit = {
    val native = query(spark.read.format("delta").load(tablePath))
    val plan = native.queryExecution.executedPlan
    val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
    assert(deltaScans.nonEmpty, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")
    val nativeRows = native.collect().toSeq.map(normalizeRow)

    withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
      val vanillaRows = query(spark.read.format("delta").load(tablePath))
        .collect()
        .toSeq
        .map(normalizeRow)
      assert(
        nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
        s"native result did not match vanilla Spark result\n" +
          s"native=${nativeRows}\nvanilla=${vanillaRows}")
    }
  }

  /**
   * Deep-normalize a Spark `Row` into a Seq[Any] where every Array / WrappedArray / Array[Byte]
   * is turned into a `List`. Scala's default `==` on `Array[Byte]` is reference-equality, which
   * makes byte-for-byte equal binary columns compare unequal. Normalizing to `List` gives value
   * equality all the way down.
   */
  private def normalizeRow(row: Row): Seq[Any] =
    row.toSeq.map(normalizeValue)

  private def normalizeValue(v: Any): Any = v match {
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

  // ───────────────────────────────────────── tests ─────────────────────────────────────────

  test("read a tiny unpartitioned delta table via the native scan") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("smoke") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("multi-file delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multifile") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(3)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("projection pushdown reads only selected columns") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("projection") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5, i % 2 == 0))
        .toDF("id", "name", "score", "active")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.select("id", "score"))
    }
  }

  test("partitioned delta table surfaces partition column values") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("partitioned") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 12)
        .map(i => (i.toLong, s"name_$i", if (i < 6) "a" else "b"))
        .toDF("id", "name", "category")
        .write
        .partitionBy("category")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("filter pushdown returns correct rows") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id >= 5 AND id < 15"))
    }
  }

  test("complex types: array, map, and struct") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("complex") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 5).map { i =>
        (i.toLong, Seq(i, i + 1, i + 2), Map(s"k$i" -> s"v$i"), (s"inner_$i", i.toDouble))
      }
      rows
        .toDF("id", "tags", "props", "nested")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("predicate variety: eq, lt, gt, is null, in, and/or") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("predicates") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Mixed with some nulls in `name` so IS NULL / IS NOT NULL have real targets.
      val rows = (0 until 20).map { i =>
        val name: String = if (i % 5 == 0) null else s"name_$i"
        (i.toLong, name, i * 1.5)
      }
      rows
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.where("id = 7"))
      assertDeltaNativeMatches(tablePath, _.where("id < 5"))
      assertDeltaNativeMatches(tablePath, _.where("id > 15"))
      assertDeltaNativeMatches(tablePath, _.where("id <= 3 OR id >= 17"))
      assertDeltaNativeMatches(tablePath, _.where("id IN (2, 4, 6, 8)"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NULL"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NOT NULL AND score > 10.0"))
      assertDeltaNativeMatches(tablePath, _.where("NOT (id = 5)"))
    }
  }

  test("empty delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("empty") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Zero-row DataFrame still materializes the table schema + metadata commit,
      // producing a valid Delta table with zero add actions.
      Seq
        .empty[(Long, String)]
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("schema evolution: new column added in later commit") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("schema_evolution") { tablePath =>
      val ss = spark
      import ss.implicits._

      // Commit 1: two-column schema.
      (0 until 5)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      // Commit 2: append with a third column. Requires mergeSchema so Delta
      // accepts the wider schema.
      (5 until 10)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("id", "name", "score")
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(tablePath)

      // Reading back the evolved schema: rows from commit 1 should return null
      // for `score`; rows from commit 2 should have real values.
      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("time travel by version reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("time_travel") { tablePath =>
      val ss = spark
      import ss.implicits._

      // Commit 0: initial 5 rows.
      (0 until 5)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      // Commit 1: overwrite with 3 different rows. This makes the old files
      // still exist on disk but no longer in the latest snapshot's add actions.
      (100 until 103)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)

      // Reading with versionAsOf=0 must return the ORIGINAL 5 rows, not the
      // 3 rows from the latest snapshot.
      val native = spark.read.format("delta").option("versionAsOf", "0").load(tablePath)
      val plan = native.queryExecution.executedPlan
      val hasDeltaScan = collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty
      assert(hasDeltaScan, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")

      val nativeRows = native.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanillaRows = spark.read
          .format("delta")
          .option("versionAsOf", "0")
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"native time-travel result did not match vanilla\n" +
            s"native=$nativeRows\nvanilla=$vanillaRows")
      }
      // Extra sanity: we should have read 5 rows (commit 0), not 3 (commit 1).
      assert(nativeRows.size == 5, s"expected 5 rows from versionAsOf=0, got ${nativeRows.size}")
    }
  }

  test("time travel by timestamp reads the older snapshot") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("timestamp_travel") { tablePath =>
      val ss = spark
      import ss.implicits._

      // Commit 0.
      (0 until 5)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)

      // Capture commit 0's log file mtime with millisecond precision, then add a
      // half-second so we land strictly AFTER commit 0 and strictly BEFORE commit 1.
      // Delta's history resolution requires the requested timestamp to be >= the
      // earliest commit file mtime.
      val logFile = new java.io.File(tablePath, "_delta_log/00000000000000000000.json")
      val t0 = logFile.lastModified()
      val targetTs = t0 + 500L
      Thread.sleep(1100) // ensure commit 1 lands at least a second later

      // Commit 1: overwrite.
      (100 until 102)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)

      val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val tsString = fmt.format(new java.util.Date(targetTs))
      assertDeltaNativeMatches(
        tablePath,
        _ => spark.read.format("delta").option("timestampAsOf", tsString).load(tablePath))
    }
  }

  test("multi-column partitioning") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multipart") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = for {
        region <- Seq("us", "eu", "ap")
        tier <- Seq("free", "pro")
        i <- 0 until 4
      } yield (i.toLong, s"name_$region${tier}_$i", region, tier)

      rows
        .toDF("id", "name", "region", "tier")
        .write
        .partitionBy("region", "tier")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("region = 'us'"))
      assertDeltaNativeMatches(tablePath, _.where("region = 'eu' AND tier = 'pro'"))
    }
  }

  test("typed partition columns: int, long, date") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("typed_partitions") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 8).map { i =>
        (
          i.toLong,
          s"name_$i",
          i % 3, // int partition
          (1000L + i), // long partition
          java.sql.Date.valueOf(f"2024-01-${(i % 5) + 1}%02d") // date partition
        )
      }
      rows
        .toDF("id", "name", "p_int", "p_long", "p_date")
        .write
        .partitionBy("p_int", "p_long", "p_date")
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("p_int = 1"))
      assertDeltaNativeMatches(tablePath, _.where("p_date >= DATE'2024-01-03'"))
    }
  }

  test("aggregation and join over delta inputs") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("agg_join") { tablePath =>
      val ss = spark
      import ss.implicits._

      val leftPath = new java.io.File(tablePath, "left").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "right").getAbsolutePath

      (0 until 12)
        .map(i => (i.toLong, s"name_$i", (i % 3).toLong))
        .toDF("id", "name", "grp")
        .write
        .format("delta")
        .save(leftPath)

      (0 until 3)
        .map(i => (i.toLong, s"group_$i"))
        .toDF("grp_id", "grp_label")
        .write
        .format("delta")
        .save(rightPath)

      // COUNT(*) + GROUP BY over the left table.
      val grouped =
        spark.sql(s"SELECT grp, COUNT(*) AS n, SUM(id) AS s FROM delta.`$leftPath` GROUP BY grp")
      val groupedPlan = grouped.queryExecution.executedPlan
      val hasLeftDeltaScan =
        collect(groupedPlan) { case s: CometDeltaNativeScanExec => s }.nonEmpty
      assert(hasLeftDeltaScan, s"expected CometDeltaNativeScanExec in plan, got:\n$groupedPlan")

      val nativeGrouped = grouped.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"SELECT grp, COUNT(*) AS n, SUM(id) AS s FROM delta.`$leftPath` GROUP BY grp")
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeGrouped.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
          s"native=$nativeGrouped\nvanilla=$vanilla")
      }

      // Inner join between two Delta tables.
      val joined = spark.sql(s"""
           |SELECT l.id, l.name, r.grp_label
           |FROM delta.`$leftPath` l
           |JOIN delta.`$rightPath` r ON l.grp = r.grp_id
           |""".stripMargin)
      val joinPlan = joined.queryExecution.executedPlan
      val deltaScanCount =
        collect(joinPlan) { case s: CometDeltaNativeScanExec => s }.size
      assert(
        deltaScanCount == 2,
        s"expected 2 CometDeltaNativeScanExec in join plan, got $deltaScanCount:\n$joinPlan")

      val nativeJoined = joined.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
                  |SELECT l.id, l.name, r.grp_label
                  |FROM delta.`$leftPath` l
                  |JOIN delta.`$rightPath` r ON l.grp = r.grp_id
                  |""".stripMargin)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeJoined.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
          s"native=$nativeJoined\nvanilla=$vanilla")
      }
    }
  }

  test("multiple appends produce many files, native scan reads them all") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("multi_append") { tablePath =>
      val ss = spark
      import ss.implicits._

      // Five append commits, each a single file of 4 rows. Comet's native scan
      // must enumerate tasks across all of them.
      (0 until 5).foreach { commit =>
        (0 until 4)
          .map(i => ((commit * 10 + i).toLong, s"c${commit}_r$i"))
          .toDF("id", "name")
          .repartition(1)
          .write
          .format("delta")
          .mode(if (commit == 0) "overwrite" else "append")
          .save(tablePath)
      }

      val df = spark.read.format("delta").load(tablePath)
      assertDeltaNativeMatches(tablePath, identity)
      assert(df.count() == 20, s"expected 20 rows, got ${df.count()}")
    }
  }

  test("column name case insensitivity") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("case_insensitive") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 6)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("Id", "Name")
        .write
        .format("delta")
        .save(tablePath)

      // Query with lowercase names against a camel-cased schema. Spark's default
      // `caseSensitive=false` should make this work, with the native Comet reader
      // honoring the same setting.
      assertDeltaNativeMatches(tablePath, df => df.select("id", "name"))
      assertDeltaNativeMatches(tablePath, df => df.where("id > 2"))
    }
  }

  test("reordered and duplicated projections") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("reordered_proj") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 8)
        .map(i => (i.toLong, s"name_$i", i * 1.5))
        .toDF("a", "b", "c")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // Reordered columns.
      assertDeltaNativeMatches(tablePath, df => df.select("c", "a", "b"))
      // Duplicated column.
      assertDeltaNativeMatches(tablePath, df => df.selectExpr("a", "a AS a2", "b"))
    }
  }

  test("deeply nested complex types") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("nested_complex") { tablePath =>
      val ss = spark
      import ss.implicits._
      // array<struct<name, score>>, map<string, array<int>>, struct<inner: struct<x, y>>
      // Using tuples all the way down so the implicit encoder can resolve without
      // top-level case class declarations.
      val rows = (0 until 4).map { i =>
        (
          i.toLong,
          Seq((s"a_$i", i * 1.5), (s"b_$i", i * 2.5)),
          Map(s"k_$i" -> Seq(i, i + 1, i + 2)),
          ((i, s"inner_$i"), (i * 10).toLong))
      }
      rows
        .toDF("id", "entries", "props", "nested")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  test("order by and limit over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("order_limit") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, s"name_$i", (19 - i) * 1.5))
        .toDF("id", "name", "score")
        .repartition(4)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, df => df.orderBy("score").limit(5))
      assertDeltaNativeMatches(tablePath, df => df.orderBy(df("id").desc).limit(3))
    }
  }

  test("filter that yields an empty result") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("empty_filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // No rows have id > 999; the filtered DataFrame is empty.
      assertDeltaNativeMatches(tablePath, _.where("id > 999"))
    }
  }

  test("COUNT(*) over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("count_star") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 25)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(3)
        .write
        .format("delta")
        .save(tablePath)

      // Delta short-circuits `SELECT COUNT(*)` via add-action numRecords stats, so
      // the plan ends up as a `LocalTableScan` with the precomputed value and our
      // Delta scan never runs at all. That's correct behavior, so we just assert the
      // count itself. To verify the native scan actually works, issue a COUNT that
      // requires reading (e.g. with a filter on a non-stats column).
      val count = spark.sql(s"SELECT COUNT(*) AS n FROM delta.`$tablePath`").collect()
      assert(count.length == 1 && count(0).getLong(0) == 25L, s"COUNT(*) returned $count")

      // This COUNT needs a predicate Delta can't resolve from the add-action stats,
      // so it falls through to a real file read via Comet.
      val filteredCount =
        spark.sql(s"SELECT COUNT(*) AS n FROM delta.`$tablePath` WHERE name LIKE 'name_1%'")
      val plan = filteredCount.queryExecution.executedPlan
      val hasDeltaScan = collect(plan) { case s: CometDeltaNativeScanExec => s }.nonEmpty
      assert(
        hasDeltaScan,
        s"expected CometDeltaNativeScanExec in filtered-count plan, got:\n$plan")
      val expected = (0 until 25).count(i => s"name_$i".startsWith("name_1"))
      val actual = filteredCount.collect()(0).getLong(0)
      assert(actual == expected.toLong, s"expected $expected, got $actual")
    }
  }

  test("union of two delta tables") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("union") { tablePath =>
      val ss = spark
      import ss.implicits._
      val leftPath = new java.io.File(tablePath, "l").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "r").getAbsolutePath
      (0 until 5)
        .map(i => (i.toLong, s"l_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(leftPath)
      (5 until 10)
        .map(i => (i.toLong, s"r_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(rightPath)

      val df =
        spark.sql(s"SELECT * FROM delta.`$leftPath` UNION ALL SELECT * FROM delta.`$rightPath`")
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.size == 2, s"expected 2 Delta scans, got ${scans.size}:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"SELECT * FROM delta.`$leftPath` UNION ALL SELECT * FROM delta.`$rightPath`")
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("self-join on delta table") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("self_join") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 10)
        .map(i => (i.toLong, (i / 2).toLong, s"name_$i"))
        .toDF("id", "grp", "name")
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.sql(s"""
        SELECT a.id, a.name, b.name AS partner
        FROM delta.`$tablePath` a
        JOIN delta.`$tablePath` b ON a.grp = b.grp AND a.id < b.id
      """)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT a.id, a.name, b.name AS partner
            FROM delta.`$tablePath` a
            JOIN delta.`$tablePath` b ON a.grp = b.grp AND a.id < b.id
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("struct field access and array element in SELECT") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("struct_access") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 6).map { i =>
        (i.toLong, (s"first_$i", s"last_$i"), Seq(i * 10, i * 20, i * 30))
      }
      rows
        .toDF("id", "name", "values")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      // struct.field access
      assertDeltaNativeMatches(
        tablePath,
        _.selectExpr("id", "name._1 AS first", "name._2 AS last"))
      // array element access
      assertDeltaNativeMatches(
        tablePath,
        _.selectExpr("id", "values[0] AS v0", "values[2] AS v2"))
    }
  }

  test("distinct and group by having") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("distinct_having") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 30)
        .map(i => (i.toLong, (i % 5).toLong, s"name_${i % 7}"))
        .toDF("id", "grp", "name")
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, _.select("grp").distinct())
      assertDeltaNativeMatches(
        tablePath,
        df =>
          df.groupBy("grp")
            .count()
            .where("count > 5"))
    }
  }

  test("null values throughout the data") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("nulls") { tablePath =>
      val ss = spark
      import ss.implicits._
      val rows = (0 until 12).map { i =>
        (
          if (i % 3 == 0) null else Long.box(i.toLong),
          if (i % 4 == 0) null else s"name_$i",
          if (i % 5 == 0) null else Double.box(i * 1.5))
      }
      rows
        .toDF("id", "name", "score")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
      assertDeltaNativeMatches(tablePath, _.where("id IS NULL"))
      assertDeltaNativeMatches(tablePath, _.where("name IS NOT NULL AND score IS NULL"))
    }
  }

  test("window function over delta") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("window") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 20)
        .map(i => (i.toLong, (i % 4).toLong, i * 1.5))
        .toDF("id", "grp", "score")
        .write
        .format("delta")
        .save(tablePath)

      val df = spark.sql(s"""
        SELECT id, grp, score,
               ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn
        FROM delta.`$tablePath`
      """)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.nonEmpty, s"expected CometDeltaNativeScanExec in plan, got:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT id, grp, score,
                   ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn
            FROM delta.`$tablePath`
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("LEFT OUTER JOIN of two delta tables") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("left_join") { tablePath =>
      val ss = spark
      import ss.implicits._
      val leftPath = new java.io.File(tablePath, "l").getAbsolutePath
      val rightPath = new java.io.File(tablePath, "r").getAbsolutePath
      // Left has 5 rows with ids 0..4; right has 3 rows with ids 0,1,2 — so the join
      // has 2 rows with NULL right-side columns (for ids 3 and 4).
      (0 until 5)
        .map(i => (i.toLong, s"l_$i"))
        .toDF("id", "lname")
        .write
        .format("delta")
        .save(leftPath)
      (0 until 3)
        .map(i => (i.toLong, s"r_$i"))
        .toDF("id", "rname")
        .write
        .format("delta")
        .save(rightPath)

      val df = spark.sql(s"""
        SELECT l.id, l.lname, r.rname
        FROM delta.`$leftPath` l
        LEFT JOIN delta.`$rightPath` r ON l.id = r.id
      """)
      val plan = df.queryExecution.executedPlan
      val scans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(scans.size == 2, s"expected 2 Delta scans, got ${scans.size}:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow).sortBy(_.mkString("|"))
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanilla = spark
          .sql(s"""
            SELECT l.id, l.lname, r.rname
            FROM delta.`$leftPath` l
            LEFT JOIN delta.`$rightPath` r ON l.id = r.id
          """)
          .collect()
          .toSeq
          .map(normalizeRow)
          .sortBy(_.mkString("|"))
        assert(nativeRows == vanilla, s"native=$nativeRows\nvanilla=$vanilla")
      }
    }
  }

  test("partitioned table with filter on non-partition column") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("part_data_filter") { tablePath =>
      val ss = spark
      import ss.implicits._
      (0 until 24)
        .map(i => (i.toLong, s"name_$i", if (i < 12) "hot" else "cold"))
        .toDF("id", "name", "tier")
        .write
        .partitionBy("tier")
        .format("delta")
        .save(tablePath)

      // Filter on `id` (non-partition). Should still return the right rows.
      assertDeltaNativeMatches(tablePath, _.where("id % 2 = 0"))
      // Combined predicate on both a partition col and a data col.
      assertDeltaNativeMatches(tablePath, _.where("tier = 'hot' AND id >= 4"))
    }
  }

  test("write, overwrite, append, read full history") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("history") { tablePath =>
      val ss = spark
      import ss.implicits._
      // v0: initial.
      (0 until 5)
        .map(i => (i.toLong, s"v0_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .save(tablePath)
      // v1: overwrite.
      (0 until 3)
        .map(i => (i.toLong, s"v1_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("overwrite")
        .save(tablePath)
      // v2: append.
      (10 until 13)
        .map(i => (i.toLong, s"v2_$i"))
        .toDF("id", "name")
        .write
        .format("delta")
        .mode("append")
        .save(tablePath)

      // Each version has a different row set; we test time-travel for all three.
      (0 to 2).foreach { v =>
        val native = spark.read
          .format("delta")
          .option("versionAsOf", v.toString)
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
          val vanilla = spark.read
            .format("delta")
            .option("versionAsOf", v.toString)
            .load(tablePath)
            .collect()
            .toSeq
            .map(normalizeRow)
          assert(
            native.sortBy(_.mkString("|")) == vanilla.sortBy(_.mkString("|")),
            s"v$v mismatch\nnative=$native\nvanilla=$vanilla")
        }
      }
    }
  }

  test("deletion vectors: accelerates DV-in-use tables via native DV filter") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("dv_accel") { tablePath =>
      val ss = spark
      import ss.implicits._

      // DV-capable table: requires protocol >= reader 3 / writer 7 and the
      // `delta.enableDeletionVectors` table property.
      (0 until 20)
        .map(i => (i.toLong, s"name_$i"))
        .toDF("id", "name")
        .repartition(1)
        .write
        .format("delta")
        .option("delta.enableDeletionVectors", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      // Pre-DELETE: no files have DVs, Comet accelerates normally.
      assertDeltaNativeMatches(tablePath, identity)

      // DELETE via vanilla Spark+Delta writes a DV for the affected file.
      // Comet doesn't accelerate writes (matches Iceberg behavior).
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id % 3 = 0")

      // Post-DELETE: Comet's Phase 3 native DV support kicks in. The scan
      // still goes through Comet (no fallback) and DeltaDvFilterExec drops
      // the deleted rows on the output stream.
      val df = spark.read.format("delta").load(tablePath)
      val plan = df.queryExecution.executedPlan
      val deltaScans = collect(plan) { case s: CometDeltaNativeScanExec => s }
      assert(
        deltaScans.nonEmpty,
        s"expected Comet to accelerate a DV-in-use table, but plan has no " +
          s"CometDeltaNativeScanExec:\n$plan")

      val nativeRows = df.collect().toSeq.map(normalizeRow)
      withSQLConf(CometConf.COMET_DELTA_NATIVE_ENABLED.key -> "false") {
        val vanillaRows = spark.read
          .format("delta")
          .load(tablePath)
          .collect()
          .toSeq
          .map(normalizeRow)
        assert(
          nativeRows.sortBy(_.mkString("|")) == vanillaRows.sortBy(_.mkString("|")),
          s"native=$nativeRows\nvanilla=$vanillaRows")
      }
      // Rows with id in {0,3,6,9,12,15,18} are deleted -> 13 rows remain.
      assert(nativeRows.size == 13, s"expected 13 rows after DELETE, got ${nativeRows.size}")

      // Second DELETE exercises DV replacement across commits.
      spark.sql(s"DELETE FROM delta.`$tablePath` WHERE id >= 18")
      val df2 = spark.read.format("delta").load(tablePath)
      val rows2 = df2.collect().toSeq.map(normalizeRow)
      assert(rows2.size == 12, s"expected 12 rows after second DELETE, got ${rows2.size}")
      val plan2 = df2.queryExecution.executedPlan
      assert(
        collect(plan2) { case s: CometDeltaNativeScanExec => s }.nonEmpty,
        s"expected Comet to still accelerate after second DELETE, got:\n$plan2")
    }
  }

  test("wider primitive type coverage") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("primitives") { tablePath =>
      val ss = spark
      import ss.implicits._

      val rows = (0 until 8).map { i =>
        (
          i % 2 == 0, // boolean
          i.toByte, // byte
          i.toShort, // short
          i, // int
          i.toLong, // long
          i.toFloat * 0.5f, // float
          i.toDouble * 0.25, // double
          s"str_$i", // string
          Array[Byte](i.toByte, (i + 1).toByte), // binary
          java.sql.Date.valueOf(f"2024-01-${(i % 28) + 1}%02d"), // date
          new java.sql.Timestamp(1700000000000L + i * 1000L), // timestamp
          BigDecimal(i) + BigDecimal("0.125") // decimal
        )
      }
      rows
        .toDF("b", "i8", "i16", "i32", "i64", "f32", "f64", "s", "bin", "d", "ts", "dec")
        .repartition(1)
        .write
        .format("delta")
        .save(tablePath)

      assertDeltaNativeMatches(tablePath, identity)
    }
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
