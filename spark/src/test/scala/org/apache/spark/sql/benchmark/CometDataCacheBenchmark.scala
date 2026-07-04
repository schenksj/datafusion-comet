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

package org.apache.spark.sql.benchmark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.comet.{CometConf, CometSparkSessionExtensions, Native}

/**
 * Benchmark for the object-store data cache (memory tier): measures the win from re-reading a
 * remote Parquet dataset out of the local cache instead of re-fetching its bytes from the object
 * store.
 *
 * The cache only wraps remote object stores (never local `file://`), and it is process-global and
 * initialized once ("first plan wins"), so the meaningful single-process measurement is cold vs
 * warm against a real object store: the first scan fetches from S3 into the cache; subsequent
 * scans are served from local memory.
 *
 * ==Configuration (nothing is hardcoded)==
 * The S3 location comes entirely from the environment, so no bucket name or credential is baked
 * into the source:
 *   - `COMET_DATA_CACHE_BENCH_S3_PATH` (required): an `s3a://` base path the running identity can
 *     read and write, e.g. `s3a://my-bucket/comet-cache-bench`. When unset, the S3 benchmark is
 *     skipped so CI stays green.
 *   - `COMET_DATA_CACHE_BENCH_S3_CRED_PROVIDER` (optional): the `fs.s3a.aws.credentials.provider`
 *     class. Defaults to the AWS SDK v2 profile provider, which reads `~/.aws/credentials`.
 *   - `AWS_REGION` (optional): sets `fs.s3a.endpoint.region` when present. Any
 *     `spark.hadoop.fs.s3a.*` you pass as a `-D` system property is also honored.
 *
 * To run:
 * {{{
 *   export COMET_DATA_CACHE_BENCH_S3_PATH=s3a://<your-bucket>/comet-cache-bench
 *   SPARK_GENERATE_BENCHMARK_FILES=1 \
 *     make benchmark-org.apache.spark.sql.benchmark.CometDataCacheBenchmark
 * }}}
 */
object CometDataCacheBenchmark extends CometBenchmarkBase {

  // `final val` so Scala inlines these as compile-time constants. The base benchmark trait calls
  // getSparkSession during its own initialization, before this object's plain vals are assigned,
  // so a non-final val referenced there would still be null (NPE at startup).
  private final val S3_PATH_ENV = "COMET_DATA_CACHE_BENCH_S3_PATH"
  private final val S3_CRED_PROVIDER_ENV = "COMET_DATA_CACHE_BENCH_S3_CRED_PROVIDER"
  private final val DEFAULT_CRED_PROVIDER =
    "software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider"

  private final val rows: Long = 4 * 1024 * 1024

  private def env(name: String): Option[String] =
    sys.env.get(name).orElse(sys.props.get(name)).map(_.trim).filter(_.nonEmpty)

  private def s3BasePath: Option[String] = env(S3_PATH_ENV)

  override def getSparkSession: SparkSession = {
    val conf = new SparkConf()
      .setAppName("CometDataCacheBenchmark")
      .set("spark.master", "local[4]")
      .setIfMissing("spark.driver.memory", "4g")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager")
      .set(CometConf.COMET_ENABLED.key, "true")
      .set(CometConf.COMET_ONHEAP_ENABLED.key, "true")
      .set(CometConf.COMET_EXEC_ENABLED.key, "true")
      .set(CometConf.COMET_NATIVE_SCAN_ENABLED.key, "true")
      .set(CometConf.COMET_ONHEAP_MEMORY_OVERHEAD.key, "4g")
      // Enable the data cache in the session so the first plan initializes the process-global
      // cache (first-plan-wins). Comparing against cache-off requires a separate run with
      // spark.comet.scan.dataCache.enabled=false, since the global cache cannot be toggled per
      // query.
      .set(CometConf.COMET_DATA_CACHE_ENABLED.key, "true")
      .set(CometConf.COMET_DATA_CACHE_MEMORY_LIMIT.key, "2g")

    // s3a credentials/region come from the environment (see class doc). The provider defaults to
    // the profile provider so ~/.aws/credentials is used; no secrets are read here.
    conf.set(
      "spark.hadoop.fs.s3a.aws.credentials.provider",
      env(S3_CRED_PROVIDER_ENV).getOrElse(DEFAULT_CRED_PROVIDER))
    env("AWS_REGION").foreach(r => conf.set("spark.hadoop.fs.s3a.endpoint.region", r))

    SparkSession.builder
      .config(conf)
      .withExtensions(new CometSparkSessionExtensions)
      .getOrCreate()
  }

  // Rows in the fact table (configurable). Combined with `valueCols` random-double columns this
  // targets a ~0.5 GB on-disk dataset (4M rows * 16 cols * 8 bytes ~= 512 MB).
  private def factRows: Long = env("COMET_DATA_CACHE_BENCH_ROWS").map(_.toLong).getOrElse(rows)
  // Number of value columns, wide enough that a single-column projection fetches a small fraction
  // of the file.
  private final val valueCols: Int = 16
  // Data shape toggle. Default (false) = uniform random doubles: incompressible, so the on-disk
  // size reflects the logical size and projections fetch a real fraction -- the primary case.
  // true = low-cardinality id-derived columns that Parquet dictionary/RLE crush to a tiny file:
  // shows the cache still wins (cold cost over WAN is S3-round-trip-latency-bound, not throughput),
  // but too small to surface the projection selective-read benefit.
  private def compressible: Boolean =
    env("COMET_DATA_CACHE_BENCH_COMPRESSIBLE").exists(_.toBoolean)
  // Distinct join keys (also the dimension table's row count).
  private final val dimKeys: Long = 4000
  // Warm iterations timed per scenario (median reported).
  private final val warmIters: Int = 4

  private lazy val nativeLib: Native = new Native()

  /** One benchmark scenario: a category, a label, and the SQL to run. */
  private case class Scenario(category: String, name: String, sql: String)

  private def sumOf(cols: Seq[Int]): String = cols.map(i => s"sum(v$i)").mkString(" + ")

  private val scenarios: Seq[Scenario] = Seq(
    Scenario(
      "scan",
      s"full scan ($valueCols cols)",
      s"SELECT ${sumOf(1 to valueCols)} FROM fact"),
    Scenario("scan", "projection (1 col)", "SELECT sum(v1) FROM fact"),
    Scenario("scan", "projection (4 cols)", s"SELECT ${sumOf(1 to 4)} FROM fact"),
    // v2 is uniform in [0, 1), so this reads ~all of v1 + v2 and keeps ~25% of rows.
    Scenario("scan", "filter (v2 < 0.25)", "SELECT sum(v1) FROM fact WHERE v2 < 0.25"),
    Scenario(
      "agg",
      "group-by aggregation",
      "SELECT k, sum(v1), avg(v2), count(1) FROM fact GROUP BY k"),
    Scenario("agg", "count distinct key", "SELECT count(DISTINCT k) FROM fact"),
    Scenario("join", "join fact-dim", "SELECT sum(f.v1) FROM fact f JOIN dim d ON f.k = d.k"),
    Scenario(
      "join",
      "join + group-by",
      "SELECT d.name, sum(f.v1) FROM fact f JOIN dim d ON f.k = d.k GROUP BY d.name"))

  private def run(sql: String): Unit = spark.sql(sql).noop()

  private def timeMillis(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    (System.nanoTime() - start) / 1000000L
  }

  /** Bytes fetched from the object store so far (native counter). 0 if the cache is disabled. */
  private def bytesFetched(): Long = {
    val s = nativeLib.getDataCacheStats()
    if (s != null && s.length > 3) s(3) else 0L
  }

  private def prepareData(base: String): Unit = {
    val factPath = s"$base/fact"
    val dimPath = s"$base/dim"
    // `id` and `k` (the join key) are derived and compress well. The value columns are either
    // seeded uniform random doubles (incompressible; the file lands near its logical size) or
    // low-cardinality id-derived doubles (compressible; Parquet crushes the file), per the toggle.
    val valueExprs =
      if (compressible) {
        // A handful of distinct values per column -> dictionary/RLE -> tiny file.
        (1 to valueCols).map(i => s"cast(id % ${100 + i} as double) AS v$i")
      } else {
        (1 to valueCols).map(i => s"rand($i) AS v$i")
      }
    spark
      .range(factRows)
      .selectExpr(Seq("id", s"cast(id % $dimKeys as long) AS k") ++ valueExprs: _*)
      .write
      .mode("overwrite")
      .parquet(factPath)

    spark
      .range(dimKeys)
      .selectExpr("id AS k", "concat('name_', cast(id % 50 as string)) AS name")
      .write
      .mode("overwrite")
      .parquet(dimPath)

    spark.read.parquet(factPath).createOrReplaceTempView("fact")
    spark.read.parquet(dimPath).createOrReplaceTempView("dim")
  }

  private case class Result(
      category: String,
      name: String,
      coldMs: Long,
      warmMs: Long,
      coldMb: Double,
      warmMb: Double) {
    def speedup: Double = if (warmMs > 0) coldMs.toDouble / warmMs.toDouble else 0.0
  }

  private def measure(s: Scenario): Result = {
    // Force a cold cache so the first run genuinely fetches this scenario's bytes from S3.
    nativeLib.clearDataCache()
    val b0 = bytesFetched()
    val cold = timeMillis(run(s.sql))
    val coldMb = (bytesFetched() - b0).toDouble / (1024 * 1024)

    // Warm: one warm-up (populate any not-yet-cached blocks), then time `warmIters` runs.
    run(s.sql)
    val bWarm = bytesFetched()
    val warmTimes = (0 until warmIters).map(_ => timeMillis(run(s.sql))).sorted
    val warmMs = warmTimes(warmTimes.length / 2) // median
    val warmMb = (bytesFetched() - bWarm).toDouble / (1024 * 1024) / warmIters

    Result(s.category, s.name, cold, warmMs, coldMb, warmMb)
  }

  private def printResults(base: String, results: Seq[Result]): Unit = {
    // scalastyle:off println
    val header =
      f"${"Category"}%-8s ${"Scenario"}%-24s ${"Cold(ms)"}%9s ${"Warm(ms)"}%9s " +
        f"${"Speedup"}%8s ${"ColdFetch"}%10s ${"WarmFetch"}%10s"
    val rule = "-" * header.length
    val shape = if (compressible) "compressible (id-derived)" else "incompressible (random)"
    println("")
    println(
      s"Object-store data cache: cold vs warm by operation  " +
        s"($base, fact=$factRows rows x $valueCols cols, $shape)")
    println(rule)
    println(header)
    println(rule)
    for (r <- results) {
      println(
        f"${r.category}%-8s ${r.name}%-24s ${r.coldMs}%9d ${r.warmMs}%9d " +
          f"${r.speedup}%7.1fx ${r.coldMb}%8.1fMB ${r.warmMb}%8.1fMB")
    }
    println(rule)
    val avgSpeedup = if (results.nonEmpty) results.map(_.speedup).sum / results.size else 0.0
    println(f"mean warm speedup: $avgSpeedup%.1fx across ${results.size} scenarios")
    println(
      "ColdFetch/WarmFetch = bytes read from S3 per run (native counter); warm should be ~0.")
    // scalastyle:on println
  }

  private def runS3CacheBenchmark(base: String): Unit = {
    val dataBase = s"${base.stripSuffix("/")}/comet-data-cache-bench"
    prepareData(dataBase)
    val results = scenarios.map(measure)
    printResults(dataBase, results)
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    s3BasePath match {
      case Some(base) =>
        runBenchmark("Object-store data cache: cold vs warm (scan / agg / join)") {
          runS3CacheBenchmark(base)
        }
      case None =>
        // scalastyle:off println
        println(
          s"Skipping S3 data cache benchmark: set $S3_PATH_ENV to an s3a:// base path " +
            "(e.g. s3a://<bucket>/comet-cache-bench) whose identity has read/write access, " +
            "with AWS credentials available (e.g. in ~/.aws/credentials).")
      // scalastyle:on println
    }
  }
}
