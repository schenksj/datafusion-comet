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
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.SparkSession

import org.apache.comet.{CometConf, CometSparkSessionExtensions}

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

  private final val rows: Long = 8 * 1024 * 1024

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

  private def scan(path: String): Unit =
    spark.read.parquet(path).selectExpr("sum(v1)", "sum(v2)", "count(1)").noop()

  private def timeMillis(body: => Unit): Long = {
    val start = System.nanoTime()
    body
    (System.nanoTime() - start) / 1000000L
  }

  private def prepareDataset(path: String): Unit = {
    // A moderately wide dataset so that byte-fetch time dominates and the cache's effect is
    // visible. Overwrite so reruns are idempotent.
    spark
      .range(rows)
      .selectExpr(
        "id",
        "cast(id as double) AS v1",
        "cast(id % 1000 as double) AS v2",
        "cast(id % 997 as double) AS v3",
        "cast(id % 991 as double) AS v4",
        "cast(id % 977 as double) AS v5")
      .write
      .mode("overwrite")
      .parquet(path)
  }

  private def runS3CacheBenchmark(base: String): Unit = {
    val dataPath = s"${base.stripSuffix("/")}/comet-data-cache-bench"
    prepareDataset(dataPath)

    // scalastyle:off println
    val cold = timeMillis(scan(dataPath))
    val warm1 = timeMillis(scan(dataPath))
    val warm2 = timeMillis(scan(dataPath))
    println(s"  data cache cold vs warm ($dataPath):")
    println(f"    cold (first scan, fetched from S3): $cold%,d ms")
    println(f"    warm (served from data cache):      $warm1%,d ms, then $warm2%,d ms")
    if (warm1 > 0) {
      println(f"    warm speedup vs cold: ${cold.toDouble / warm1.toDouble}%.1fx")
    }
    println("    (native hit/miss counts are logged by the cache; run with cache logging on)")
    // scalastyle:on println

    val benchmark =
      new Benchmark("S3 Parquet scan served from Comet data cache (warm)", rows, output = output)
    benchmark.addCase("Comet native scan + data cache") { _ =>
      scan(dataPath)
    }
    benchmark.run()
  }

  override def runCometBenchmark(mainArgs: Array[String]): Unit = {
    s3BasePath match {
      case Some(base) =>
        runBenchmark("Object-store data cache: cold vs warm S3 scan") {
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
