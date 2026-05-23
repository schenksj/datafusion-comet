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

import org.apache.spark.sql.functions._

// Local reproductions for the Delta 4.1 own-suite regression families that are
// not yet fixed on this branch. One faithful, minimal repro per root cause so
// each can be diagnosed and fixed (and kept as a regression guard afterward).
//
// Status when written (after F1 DPP / F2 time-travel / #198 fixes landed):
//   F3 (row tracking)      -- PENDING; dominant family (all RowTracking* suites)
//   F4 (protobuf recursion)-- PENDING
//   F6 (corrupted file)    -- PENDING
//
// Each test asserts the EXPECTED (correct) behavior and is marked `ignore` so
// CI stays green while the bug is open. It is a confirmed repro: un-`ignore` it
// (change `ignore(` -> `test(`) to watch it fail today and pass once the
// corresponding fix lands. Verified failing at the time of writing (see commit
// message / docs/08-known-limitations.md).
class CometDeltaPendingReproSuite extends CometDeltaTestBase {

  // === F3: row tracking -- materialized stable row IDs ======================
  //
  // Mirrors RowTracking{Merge,Compaction,ReadWrite}Suite. When a file is
  // rewritten (OPTIMIZE / z-order / compaction / MERGE), Delta preserves stable
  // row IDs by MATERIALIZING them into real parquet columns
  // `_row-id-col-<uuid>` / `_row-commit-version-col-<uuid>`. The Spark plan then
  // reads `coalesce(_metadata.row_id, base_row_id + row_index)`. The native scan
  // classifies those names as synthetic (`isExtraSyntheticName`) and synthesizes
  // from `base_row_id + row_index` instead of reading the persisted values --
  // so after a rewrite the row IDs CHANGE (new file => new base+index) instead
  // of staying stable. Expected: row IDs are stable across OPTIMIZE.

  ignore("F3: row IDs are stable across OPTIMIZE (materialized row-id columns)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("f3_rowid_optimize") { tablePath =>
      val ss = spark
      import ss.implicits._
      // Multiple files so OPTIMIZE actually compacts/rewrites.
      (0 until 100)
        .map(i => (i.toLong, s"v_$i"))
        .toDF("id", "v")
        .repartition(5)
        .write
        .format("delta")
        .option("delta.enableRowTracking", "true")
        .option("delta.minReaderVersion", "3")
        .option("delta.minWriterVersion", "7")
        .save(tablePath)

      def rowIdsByValue(): Map[String, Long] =
        spark.read.format("delta").load(tablePath)
          .select(col("v"), col("_metadata.row_id").as("rid"))
          .collect()
          .map(r => r.getString(0) -> r.getLong(1))
          .toMap

      val before = rowIdsByValue()
      // OPTIMIZE compacts the 5 files into 1, rewriting + materializing row IDs.
      spark.sql(s"OPTIMIZE delta.`$tablePath`")
      val after = rowIdsByValue()

      assert(before.nonEmpty && before.size == 100, s"setup: ${before.size} rows")
      val changed = before.keys.filter(k => before(k) != after.getOrElse(k, -1L)).toSeq.sorted
      assert(
        changed.isEmpty,
        s"row IDs must be stable across OPTIMIZE; changed for ${changed.size} rows, " +
          s"e.g. ${changed.take(3).map(k => s"$k: ${before(k)} -> ${after.get(k)}").mkString(", ")}")
    }
  }

  // === F4: deeply-nested data-skipping expression -> protobuf recursion =====
  //
  // Mirrors DataSkippingDeltaTests "remove redundant stats column references in
  // data skipping expression". A WHERE with ~101 AND'd conditions builds a very
  // deep boolean expression; serializing it to Comet's native proto exceeds
  // protobuf's default recursion limit (100), throwing
  // "Protocol message had too many levels of nesting" from BinaryExpr.mergeFrom.
  // Expected: the query runs.

  ignore("F4: deeply-nested data-skipping filter does not overflow protobuf nesting") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    val tbl = "f4_deep_filter"
    withTable(tbl) {
      val colNames = (0 to 100).map(i => s"col_$i")
      spark.sql(
        s"CREATE TABLE $tbl (${colNames.map(_ + " INT").mkString(", ")}) USING delta")
      spark.sql(
        s"INSERT INTO $tbl VALUES (${colNames.map(_ => "0").mkString(", ")})")
      val whereClause = colNames.map(c => s"$c != 1").mkString(" AND ")
      // Must not throw a protobuf recursion error.
      val rows = spark.sql(s"SELECT col_0 FROM $tbl WHERE $whereClause").collect()
      assert(rows.length == 1, s"expected the single all-zero row, got ${rows.length}")
    }
  }

  // === F6: corrupted / empty file (SC-8810) =================================
  //
  // Mirrors DeltaSuite "SC-8810: skipping deleted file still throws on corrupted
  // file". With one data file truncated to 0 bytes, vanilla Spark+Delta throws a
  // `[FAILED_READ_FILE.NO_HINT]` SparkException. Comet's native reader instead
  // throws `CometNativeException: ... Requested range was invalid`. Expected:
  // the error is the Spark-compatible one (so user-facing error handling and the
  // Delta test pass). Repro asserts the message contains the Spark marker.

  ignore("F6: reading a corrupted file surfaces a Spark-compatible error (SC-8810)") {
    assume(deltaSparkAvailable, "delta-spark not on the test classpath; skipping")
    withDeltaTable("f6_corrupt") { tablePath =>
      val ss = spark
      import ss.implicits._
      Seq(1).toDF().write.format("delta").mode("append").save(tablePath)
      Seq(2, 2).toDF().write.format("delta").mode("append").save(tablePath)
      Seq(4).toDF().write.format("delta").mode("append").save(tablePath)

      // Truncate one data file to 0 bytes to simulate corruption.
      val dir = new java.io.File(tablePath)
      val parquet = dir.listFiles()
        .filter(f => !f.getName.startsWith("_") && f.getName.endsWith(".parquet"))
        .sortBy(_.getName)
        .head
      val ch = new java.io.FileOutputStream(parquet)
      try ch.getChannel.truncate(0) finally ch.close()

      val ex = intercept[Exception] {
        spark.read.format("delta").load(tablePath).collect()
      }
      val msg = Option(ex.getMessage).getOrElse("") +
        Option(ex.getCause).map(c => Option(c.getMessage).getOrElse("")).getOrElse("")
      assert(
        msg.contains("FAILED_READ_FILE"),
        s"expected a Spark-compatible FAILED_READ_FILE error, got: $msg")
    }
  }
}
