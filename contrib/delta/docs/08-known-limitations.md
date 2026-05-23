<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Known limitations & deliberate tradeoffs

This document tracks deliberate limitations, workarounds, and known-failing
behaviors in the contrib-delta native scan, so they can be opened as GitHub
issues once the work merges. Each entry notes: the behavior, why it's that way,
the correctness impact, the guarding test (if any), and the work needed to close
it.

Two kinds of entries:

- **Tradeoffs** — places where we deliberately accept reduced acceleration (or
  decline to native-scan) to preserve correctness. These are stable and
  intentional; the "fix" is a future enhancement.
- **Pending regression failures** — Delta own-suite tests that still fail under
  Comet and are not yet fixed. These are bugs to close, grouped by root cause.

The Delta own-suite regression is run via
`contrib/delta/dev/run-regression.sh 4.1.0 full` (see
`.github/workflows/delta_regression_test.yml`).

---

## Part A — Deliberate tradeoffs (open as enhancement issues)

### A1. DPP pruning not applied when the scan is inside a native block (MERGE/join)

- **Behavior:** A dynamic-partition-pruning (DPP) broadcast join over a
  partitioned Delta table keeps the native scan and returns correct results, and
  prunes to the required partitions **when the scan runs standalone**. When the
  scan is buried inside a parent Comet native block (e.g. MERGE, or a broadcast
  hash join), DPP pruning is **not** applied — the scan reads all partitions
  (correct, just unpruned; the surrounding join still filters).
- **Why:** Two layers.
  1. The AQE DPP subquery arrives as an unexecutable
     `CometSubqueryAdaptiveBroadcastExec` placeholder.
     `CometPlanAdaptiveDynamicPruningFilters` is meant to rewrite it to an
     executable form, but the rewritten scan copy is **orphaned**: when
     `transformUp` rebuilds the enclosing native block, `TreeNode.makeCopy`
     drops the converted child (the `@transient`-field / makeCopy issue,
     base-Comet `#3510`). Verified: the converted node is not reachable in the
     rule's output.
  2. The native scan's partition **count** is fixed at planning. When executed
     via a parent block, the parent's `findAllPlanData` reads
     `CometDeltaNativeScanExec.perPartitionData` — a planning-time snapshot
     memoized **before** the broadcast is materialized (to keep `numPartitions`
     stable). So runtime pruning doesn't reach that path.
- **Correctness:** Always correct. Tradeoff is performance (reads all partitions
  in the join case).
- **Fix done so far:** `CometDeltaNativeScanExec` no longer crashes on the
  placeholder — it skips it in `ensureSubqueriesResolved` and converts it on the
  fly to an executable `SubqueryBroadcastExec` during `applyDppFilters` (where
  the broadcast is materialized). See commit `64cd878a`.
- **To close:** Keep the partition count fixed at planning but **empty** the
  DPP-pruned partitions at execution (recompute `perPartitionData` with
  broadcast-resolved values, emitting empty task lists for pruned files; needs
  native-side tolerance of empty file groups).
- **Guard:** `CometDeltaDppReproSuite` (asserts correctness + native engagement;
  extend to assert the fact scan reads ~120 not 2000 rows once closed).
- **Tracking:** internal task #198.

### A2. Cloud credential plumbing gaps

Surfaced by the credential audit (`CometDeltaCredentialAuditSuite`,
`jni::tests::extract_storage_config_known_gaps`). Each is asserted as a gap today
and flips to a failure when closed.

- **A2a. GCS (`gs://`) not supported native-side.** `NativeConfig` extracts
  `fs.gs.*` keys, but `DeltaStorageConfig` (native `jni.rs`) has no `gcp_*`
  fields and `create_object_store` has no `gs`/`gcs` arm, so the keys are
  dropped. GCS-backed Delta tables can't be natively read with credentials.
- **A2b. Per-bucket S3 keys not bridged.** `NativeConfig` extracts
  `fs.s3a.bucket.<name>.*`, but native only maps the global `fs.s3a.access.key` /
  `fs.s3a.secret.key`; per-bucket creds silently fall back to global.
- **A2c. `abfs` / `abfss` drop `fs.azure.*`.**
  `NativeConfig.objectStoreConfigPrefixes` registers only `fs.abfs.` /
  `fs.abfss.` for those schemes, not `fs.azure.` — so Hadoop-style Azure account
  keys / OAuth / MSI creds (historically under `fs.azure.*`) are not extracted
  for ADLS Gen2 URIs.
- **Correctness:** Reads fail (no creds) rather than producing wrong data.
- **Guard:** `CometDeltaCredentialAuditSuite`, `jni::tests`.

### A3. Path-based CDF reads decline to native (`DeltaCDFRelation`)

- **Behavior:** Table-API CDC reads engage native (`CometDeltaCdcSuite`).
  **Path-based** `readChangeFeed` (`spark.read.format("delta").option("readChangeFeed", true).load(path)`)
  routes through `DeltaCDFRelation`, which `DeltaScanRule` (matching
  `CometScanExec` over `HadoopFsRelation`) does not intercept — so it runs on
  Spark's reader.
- **Correctness:** Correct (Spark handles it); no acceleration.
- **To close:** Teach the rule to handle `DeltaCDFRelation`.
- **Guard:** `CometDeltaScanConfAuditSuite` ("GAP CDF: path-based readChangeFeed
  does not engage native").

### A4. VARIANT type

- **Behavior:** Tolerant guard — if native engages on a VARIANT column it must
  match vanilla; today it may decline. Documented so a future silent-corruption
  regression is caught.
- **Guard:** `CometDeltaTypeRoundTripAuditSuite` ("GAP: VARIANT").

### A5. Decline gates (intentional fallbacks)

`DeltaScanRule` deliberately declines to native-scan in these cases (correct via
Spark's reader; no acceleration). These are not bugs but are worth an issue if we
want to accelerate them:

- CDC delete/insert event reads with inverted DV semantics
  (`hasInvertedRowIndexFilters`).
- DV-bearing reads when
  `spark.databricks.delta.deletionVectors.useMetadataRowIndex=false`.
- `TahoeLogFileIndexWithCloudFetch` (Databricks-proprietary, no OSS reproducer).
- Tables/queries that fail the schema/encryption compatibility checks.

---

## Part B — Pending regression failures (open as bug issues)

From the full Delta 4.1 own-suite run (`full-4.1.0-20260523-113856.log`, 70
distinct failing tests at the time of the run). Some families below have since
been fixed on this branch; status noted per family. Re-run the suite after the
in-flight fixes to get the current list before opening issues.

### B1. Time travel / snapshot version — FIXED (commit `ccde0058`)

Root cause: `DeltaReflection` refreshed the snapshot to **head** even for
`versionAsOf` / `timestampAsOf` reads, returning current data for a historical
query. Fixed by reading `preparedScan.scannedSnapshot` (what vanilla reads).
Tests: "Time travel with schema changes", "time travel with partition changes",
"don't time travel a valid delta path with @ syntax", "scans on different
versions of same table", "SPARK-41154 ... time travel spec", "Dataframe-based
time travel ... timestamp precisions", "clone a time traveled source
(version/timestamp)", "cloneAtTimestamp/cloneAtVersion API", "vacuumed version",
"cold snapshot initialization", "snapshot is updated properly when owner
changes". Guard: `CometDeltaTimeTravelReproSuite`. **Re-run to confirm all clear.**

### B2. DPP MERGE crash on partitioned tables — FIXED (commit `64cd878a`)

Root cause + fix: see A1. Tests (isPartitioned: true): "basic case - local
predicates - ... updates and inserts", "extended syntax - ...", "unlimited
clauses - ...". Guard: `CometDeltaDppReproSuite`. Pruning in the join case
remains (A1 / #198).

### B3. Row tracking — materialized row-id / row-commit-version columns — PENDING

- **Tests (~18):** "z-order {un,}partitioned table with {fresh,stable} row IDs
  (+ filter)", "z-order preserves row tracking on backfill enabled tables",
  "auto-compact ..." (same matrix), "write and read table with {no-nulls,mixed}
  materialized columns", "read mixed materialized columns with filter", "write
  and read with column names similar to row tracking columns", "write and read
  with conflicting columns".
- **Symptom:** Projecting `_metadata.row_id` / `_metadata.row_commit_version`
  returns wrong values (e.g. expected `[0,0,1]` got `[0,500,4]`).
- **Root cause (from plan):** After z-order/compaction/explicit materialization,
  Delta persists stable row IDs into real parquet columns
  `_row-id-col-<uuid>` / `_row-commit-version-col-<uuid>`. The downstream
  projection is `coalesce(_metadata.row_id, base_row_id + row_index)`. The native
  scan classifies those names as synthetic (`isExtraSyntheticName`) and
  synthesizes from `base_row_id + row_index` instead of reading the persisted
  values, so the coalesce falls back to the wrong synthesized id.
- **Fix direction:** When the file has a materialized row-id /
  row-commit-version column, read it from parquet (don't synthesize).
- **Tracking:** internal task #197 (F3).

### B4. Other untriaged failures — PENDING

Not yet root-caused; need triage (some may share a root cause with B2/B3 or be
test-harness/transaction-visibility artifacts):

- MERGE family (not `isPartitioned`-gated): "DELETE MATCHED only MERGE",
  "UPDATE MATCHED only MERGE", "DELETE/UPDATE WHEN NOT MATCHED BY SOURCE MERGE",
  "UPDATE + DELETE WHEN NOT MATCHED BY SOURCE MERGE", "{DELETE,UPDATE} only with
  source rows matching multiple target rows", "Multiple merges into the same
  table", "Source and target referencing to the same table", "Target is accessed
  through a view", "MERGE preserves Row Tracking on tables enabled using
  backfill", "schema evolution, extra nested column in source - update".
- DELETE with persistent DVs disabled (isPartitioned true/false).
- Optimized writes (partitioned / unpartitioned / disabled).
- Data skipping: "data skipping shouldn't use expressions involving a subquery",
  "remove redundant stats column references in data skipping expression"
  (and "old behavior with DataFrame schema" variants).
- "SC-8810: skipping deleted file still throws on corrupted file".

---

## How to use this doc

1. Before merge, re-run the full regression to refresh Part B (B1/B2 should be
   clear; confirm B3/B4 status).
2. For each remaining Part A and Part B entry, open a GitHub issue with the
   description here and a link to the guarding test.
3. As entries are closed, flip the corresponding GAP-marker test to a positive
   assertion and remove the entry.
