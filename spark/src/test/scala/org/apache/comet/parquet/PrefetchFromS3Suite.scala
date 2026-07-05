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

package org.apache.comet.parquet

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.comet.{CometNativeScanExec, CometScanExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.{col, sum}

import org.apache.comet.{CometConf, CometS3TestBase, Native}

/**
 * End-to-end verification of the asynchronous scan prefetch (Phase 3a) against a real remote (s3a
 * -> MinIO) object store, which — unlike local `file://` — is wrapped by the data cache and
 * therefore engages the prefetcher. Exercises the full JVM -> JNI -> native path: config
 * serialization, prefetch-task spawn at plan creation, and the `getDataCacheStats` prefetch
 * counters.
 */
class PrefetchFromS3Suite extends CometS3TestBase with AdaptiveSparkPlanHelper {

  override protected val testBucketName = "prefetch-bucket"

  private val rows = 3000000L
  private def expectedSum: Long = rows * (rows - 1) / 2

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    // The data cache is the prefetch buffer; enable both. Small blocks so a few-MB file spans
    // several cache blocks (footer read warms only the tail, leaving data chunks to prefetch).
    conf.set(CometConf.COMET_DATA_CACHE_ENABLED.key, "true")
    conf.set(CometConf.COMET_DATA_CACHE_BLOCK_SIZE.key, "1m")
    conf.set(CometConf.COMET_PREFETCH_ENABLED.key, "true")
    conf
  }

  /** Write a multi-column, multi-MB Parquet file so the projected column spans several blocks. */
  private def writeData(path: String): Unit = {
    spark
      .range(0, rows)
      .selectExpr("id", "cast(id as string) as s", "id * 2 as d")
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  test("prefetch warms the cache from S3 and returns identical results") {
    val path = s"s3a://$testBucketName/data/prefetch-test.parquet"
    writeData(path)

    val native = new Native()
    val before = native.getDataCacheStats()
    assert(
      before.length == 14,
      s"native lib must expose the prefetch counters (getDataCacheStats len=${before.length})")

    val result = spark.read.parquet(path).agg(sum(col("id")))
    val scans = collect(result.queryExecution.executedPlan) {
      case p: CometNativeScanExec => p
      case p: CometScanExec => p
    }
    assert(scans.nonEmpty, "expected a Comet native scan over s3a")
    val sumValue = result.first().getLong(0)

    val after = native.getDataCacheStats()
    // Column order (data_cache.rs): 0 hits, 1 misses, 2 fetches, 3 bytes_fetched, 4 evictions,
    // 5 invalidations, 6 ssd_hits, 7 ssd_writes, 8 prefetch_bytes_fetched,
    // 9 prefetch_fetch_requests, 10 prefetch_blocks_consumed, 11 prefetch_blocks_wasted,
    // 12 prefetch_errors, 13 prefetch_files_skipped.
    val fetches = after(2) - before(2)
    val prefetchBytes = after(8) - before(8)
    val prefetchReqs = after(9) - before(9)
    val consumed = after(10) - before(10)
    val wasted = after(11) - before(11)
    val errors = after(12) - before(12)
    val skipped = after(13) - before(13)
    info(s"data cache: upstream_fetches=$fetches | prefetch: bytes_fetched=$prefetchBytes " +
      s"requests=$prefetchReqs consumed=$consumed wasted=$wasted errors=$errors skipped=$skipped")

    // (3) result parity — the aggregate is exact.
    assert(sumValue == expectedSum, s"sum mismatch: $sumValue")
    // The cache engaged (s3a was wrapped and served the reads).
    assert(fetches > 0, "expected upstream fetches through the data cache")
    // (1)+(2) prefetch engaged: it fetched blocks ahead of the decoder and/or the scan consumed
    // prefetched blocks. Either is proof the async prefetcher ran end-to-end.
    assert(
      prefetchBytes > 0 || consumed > 0,
      s"prefetch did not engage (bytes_fetched=$prefetchBytes consumed=$consumed reqs=$prefetchReqs)")
    assert(skipped == 0, "no files should be skipped for a valid Parquet scan")
  }

  test("prefetch disabled leaves the prefetch counters untouched") {
    // A distinct (cold) file so the prefetch-on run above cannot have warmed it.
    val path = s"s3a://$testBucketName/data/no-prefetch-test.parquet"
    writeData(path)

    val native = new Native()
    val before = native.getDataCacheStats()

    // `withSQLConf` returns Unit, so capture the result into a var inside the block.
    var sumValue = 0L
    withSQLConf(CometConf.COMET_PREFETCH_ENABLED.key -> "false") {
      sumValue = spark.read.parquet(path).agg(sum(col("id"))).first().getLong(0)
    }

    val after = native.getDataCacheStats()
    val fetches = after(2) - before(2)
    val prefetchBytes = after(8) - before(8)
    val prefetchReqs = after(9) - before(9)
    info(
      s"prefetch OFF: upstream_fetches=$fetches prefetch_bytes=$prefetchBytes reqs=$prefetchReqs")

    assert(sumValue == expectedSum, s"sum mismatch: $sumValue")
    // The cold read still goes through the cache, but no prefetch work is done — proving the
    // prefetch counters are attributable to the prefetcher, not the cache.
    assert(fetches > 0, "cold read still fetches through the data cache")
    assert(prefetchBytes == 0, s"prefetch must not fetch when disabled (got $prefetchBytes)")
    assert(
      prefetchReqs == 0,
      s"prefetch must issue no requests when disabled (got $prefetchReqs)")
  }
}
