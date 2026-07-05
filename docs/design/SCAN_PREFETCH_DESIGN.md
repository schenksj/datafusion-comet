# Design: Asynchronous Scan Prefetch for DataFusion Comet

*2026-07-05. Targets datafusion-comet `main` with DataFusion 54.0.0 / parquet 58.3.0,
iceberg-rust rev `80a30d3`, and the Delta contrib on the stacked fork PRs
(`contrib-delta-direct`, delta-kernel 0.24). Companion to the parent proposal
[apache/datafusion-comet#4695](https://github.com/apache/datafusion-comet/issues/4695)
and to `OBJECT_STORE_CACHE_DESIGN.md` (the data-cache design), whose Phase 3 this
document specifies in full — including the cache/prefetch integration design for the
Iceberg and Delta native scan paths (§4). Spark internals cited against a 3.5.3
checkout; the cited mechanisms (shuffle fetch throttling, executor task threading,
delay scheduling) are stable across 3.4–4.1.*

## 1. What it is

The object-store data cache (parent design) removes *repeat* fetches. This design
removes the *first-read* penalty: today a cold Comet scan strictly serializes
object-store I/O with decode, twice over:

- **Within a file**, arrow-rs's async Parquet reader buffers all selected pages of row
  group N, decodes them, and only then issues the fetch for row group N+1
  (`parquet-58.3.0/src/arrow/async_reader/mod.rs:695-701`; the `next_row_group`
  decode/I-O separation hook at `mod.rs:224` exists but DataFusion's opener does not
  drive it concurrently).
- **Across files**, DataFusion 54's `FileStream` scan state machine deliberately keeps
  "only ... a single I/O outstanding at any time" per partition stream
  (`datafusion-datasource-54.0.0/src/file_stream/scan_state.rs:42-43`).

Cold-scan wall time is therefore ≈ Σ(fetch latency) + Σ(decode), when it could be
≈ max(Σ fetch, Σ decode). Against S3, where a single range GET costs tens of
milliseconds and a partition touches dozens of column chunks, the fetch term dominates
— and it is also served **one request at a time**, leaving almost all of the store's
request parallelism unused (the concern behind
[#3817](https://github.com/apache/datafusion-comet/issues/3817)).

**Asynchronous prefetch** closes the gap optimistically: when a native scan plan is
created on the executor, a background prefetcher — running entirely on Comet's tokio
runtime, never on the Spark task thread — computes the byte ranges the scan is about
to read (filter- and projection-aware where possible) and pulls them into the block
cache ahead of the decoder, several requests in parallel, under a strict
bytes-in-flight budget. The demand read path is unchanged: it reads through the same
`CachingObjectStore`, hits blocks the prefetcher already landed, or joins the
in-flight fetch via the cache's existing single-flight dedup. Correctness never
depends on the prefetcher; a missing, failed, or cancelled prefetch only costs
latency.

Reference points:

- **Velox** (`velox/dwio/common/CachedBufferedInput.cpp`): schedules `CoalescedLoad`s
  onto a background executor for columns whose tracked read density ≥ 80%, loading
  ahead of the decoder into `AsyncDataCache`. This design adopts the
  load-ahead-into-the-cache shape but replaces density heuristics with exact
  plan-driven ranges (Comet has the plan natively; Velox's TableScan sees one split at
  a time), and replaces pin-based handoff with single-flight composition.
- **Spark's shuffle fetcher** (`core/src/main/scala/org/apache/spark/storage/
  ShuffleBlockFetcherIterator.scala:93-95,109-112,1162`): the well-tested credit
  model this design's pacing mirrors — a `maxBytesInFlight` budget (default 48 MiB,
  `spark.reducer.maxSizeInFlight`), requests sized to budget/5 so several fetches run
  in parallel, and `fetchUpToMaxBytes()` refilling the window as results are
  *consumed*, giving natural backpressure.
- **Databricks Predictive I/O** is the commercial analog of the same lever.

Why this shape satisfies the "no task time" requirement by construction: for
pure-native-scan plans Comet already runs the entire DataFusion stream on tokio worker
threads while the Spark task thread parks in `blocking_recv`
(`jni_api.rs:780-820` — batches cross an `mpsc::channel(2)`). The executor's task
slots (one `TaskRunner` thread per running task, `Executor.scala:121,363`; slot count
= offered cores / `spark.task.cpus`, `TaskSchedulerImpl.scala:558`) stay busy running
the query itself. The prefetcher adds only more I/O-bound tokio tasks beside the
stream's own; the only work on the Spark task thread is an O(1) spawn at plan
creation.

## 2. How it works

### 2.1 Architecture

Three additions, one per existing layer — the layering discipline of the parent
design is preserved (the core stays storage-API-neutral; only the wrapper touches
`object_store`; only `native/core` knows about plans):

```
   ┌──────────────────────────────────────────────────────────────────────┐
   │ native/core — ScanPrefetcher (new: execution/prefetch.rs)            │
   │  per-scan background task on TOKIO_RUNTIME:                          │
   │  footer read → row-group prune (range + stats, best-effort) →        │
   │  projected column-chunk ranges → paced block prefetch                │
   │  spawned from the planner's NativeScan/CsvScan arms; handle stored   │
   │  in ExecutionContext; cancelled in releasePlan                       │
   └────────────▲─────────────────────────────────────────────────────────┘
                │ CachingObjectStore::prefetch_ranges(path, ranges)
   ┌────────────┴─────────────────────────────────────┐
   │ native/object-store-cache                        │
   │  inherent method (NOT on the ObjectStore trait): │
   │  routes to BlockCache::prefetch_ranges           │
   └────────────▲─────────────────────────────────────┘
                │
   ┌────────────┴─────────────────────────────────────────────────────────┐
   │ native/block-cache (still storage-API-neutral)                       │
   │  prefetch_ranges (tagged insert, no range assembly) ·                │
   │  PrefetchLedger (unconsumed-bytes credit accounting) ·               │
   │  consumption semantics on hit (§2.5) · waiter retry-once (§2.6)      │
   └──────────────────────────────────────────────────────────────────────┘
   JVM side: configuration + metrics; with the (default-off) host warm-set, also
   a per-scan manifest broadcast, a task-side self-lookup, and the stage-kick
   plugin pair (driver registry + executor `comet-warm-agent` RPC endpoint,
   §2.10). No scheduler or RDD changes — cache-affinity locality (§2.10 of the
   parent design) already routes repeat reads; prefetch accelerates whatever host
   the task actually lands on, and the stage kick starts that host's warming
   before its first task arrives.
```

The cache **is** the handoff structure. There is no separate prefetch buffer, pin
protocol, or completion channel between prefetcher and scan: the prefetcher fills
blocks through the same single-flight path demand reads use, so whichever side asks
first drives the fetch and the other side awaits the same shared future
(`block-cache/src/cache.rs:283-397`). This is the design's central simplification
relative to Velox's `CoalescedLoad` pinning.

Core API additions (sketch):

```rust
/// Why a block was inserted ahead of demand. Drives release semantics on hit and
/// eviction (§2.5 for `Prefetch`, §2.10 for `Warm` — eviction of a Warm block goes
/// to the SSD tier as intended placement, not waste).
pub enum PrefetchIntent { Prefetch, Warm }

impl BlockCache {
    /// Ensure the blocks covering `ranges` of `file` are cached, fetching misses
    /// through `fetcher` under prefetch accounting. Unlike `get_ranges` it does not
    /// assemble or return range bytes; already-cached blocks cost one shard probe.
    /// Newly inserted blocks carry `intent` and are charged to `ledger` until first
    /// demand hit or eviction. Returns per-call fetch stats.
    pub async fn prefetch_ranges(&self, file: &FileKey, ranges: &[Range<u64>],
                                 fetcher: &dyn RangeFetcher, intent: PrefetchIntent,
                                 ledger: &PrefetchLedger) -> Result<PrefetchStats>;
}

/// Per-plan credit ledger (one per prefetch task; the warm agent holds its own with
/// the §2.10 pacing): `charge(bytes)` awaits while the unconsumed-prefetched total
/// would exceed the budget; `release(bytes, outcome)` is called by the cache on
/// first demand hit (consumed), eviction (wasted for Prefetch), or eviction-to-SSD
/// (placed, Warm only). Wasted releases are counted (§2.9); adaptive
/// throttling on top of them is post-MVP (§5, phase 3c).
pub struct PrefetchLedger { /* budget: AtomicU64, outstanding, Notify, waste ctrs */ }
```

### 2.2 Trigger, lifecycle, cancellation

**Trigger.** The planner's `NativeScan` arm (`planner.rs:1358-1485`) already
materializes everything the prefetcher needs, before the first batch is ever polled:
the partition's `PartitionedFile`s (path, object size, split `range`), the projection
vector, the compiled `data_filters`, the schemas, the encryption flag, and the
`object_store_url` whose store — when the data cache is enabled — is the wrapped
`CachingObjectStore` (`parquet_support.rs`, parent §2.7). At the end of that arm
(and the `CsvScan` arm, §2.3), when prefetch is enabled, the planner builds a
`PrefetchSpec` and accumulates it on the `PhysicalPlanner`.

**Spawn.** `createPlan` returns before any plan exists natively; Comet builds the
DataFusion plan lazily inside the first `executePlan` call (`jni_api.rs:752-762`).
Immediately after `planner.create_plan(..)` returns, `jni_api` takes the accumulated
specs and spawns one prefetch task per scan onto the existing `TOKIO_RUNTIME`
(`jni_api.rs:121` — already sized to `spark.executor.cores` workers,
`jni_api.rs:359-362`). The spawn is the *only* prefetch work executed on the Spark
task thread: building a spec (cheap clones of `Arc`s and file lists) plus
`tokio::spawn` — single-digit microseconds. The handles are stored in a new
`ExecutionContext::prefetch_handles: Vec<PrefetchHandle>` field
(`jni_api.rs:271-325`).

Starting at plan creation rather than `createPlan` costs nothing in practice —
`CometExecIterator` calls `executePlan` for the first batch immediately — and avoids
deserializing scan specs twice.

**Cancellation is cooperative, never `JoinHandle::abort`.** Each `PrefetchHandle`
holds a `CancellationToken`; the prefetch loop checks it between fetch units (per
coalesced run, i.e. at most every `max_coalesce_bytes` = 16 MiB of I/O) and between
files. `releasePlan` (`jni_api.rs:914-943`), reached from
`CometExecIterator.close()`'s task-completion listener
(`CometExecIterator.scala:231-239`), cancels every handle before dropping the
context; normal completion, LIMIT-satisfied early close, task failure, and task kill
all funnel through the same listener. After cancellation, at most the in-flight
requests complete (≤ `maxConcurrentRequests` × 16 MiB), publish their blocks —
future queries still benefit — and the task exits.

Hard abort is rejected because the cache's single-flight protocol makes the fetch
owner's future the delivery vehicle: dropping an owner mid-fetch currently surfaces
as an error to every concurrent waiter (`cache.rs:314-322` — "fetch owner dropped
before delivering block"), and a demand read racing a prefetch of the same block is
the *common* case here, not a corner. §2.6 additionally hardens the waiter side.

### 2.3 What gets prefetched: plan-driven, filter-aware range computation

Per file, in the exact order the scan consumes them (DataFusion's `FileStream` pops
files from its work queue in partition order,
`file_stream/work_source.rs:40,103`):

1. **Footer.** Prefetch the tail range `file_size − metadata_size_hint .. file_size`
   (Comet pins the hint at 512 KiB, `parquet_exec.rs:135`) through the cache — this
   is byte-identical to what the scan's own metadata read will request, so it lands
   the same blocks (the parent design's §2.6 already block-caches footer bytes; the
   prefetcher just gets there first). Then parse the metadata **from cached bytes**
   with `ParquetMetaDataReader` — zero additional GETs; if the footer is larger than
   the hint, the reader's follow-up ranges also flow through the cache.
2. **Row-group pruning, reusing the opener's own machinery.** DataFusion 54 exports
   the exact types its opener uses (`datafusion-datasource-parquet-54.0.0/src/
   mod.rs:47,54`): build `RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(..))`,
   then `prune_by_range(rg_metadata, split_range)` — the same call as
   `opener/mod.rs:904` — so only row groups whose midpoint falls inside this task's
   split are considered.
3. **Statistics pruning, best-effort (`filterAware`).** Build a `PruningPredicate`
   from the scan's `data_filters` against the physical file schema
   (`datafusion_pruning::build_pruning_predicate`, as `opener/mod.rs:62,910-916`
   does) and call `prune_by_statistics`. *Any* failure — predicate not convertible,
   schema mismatch across evolution, missing stats — degrades silently to step 2's
   result. The prefetcher may over-select relative to the real scan (which
   additionally applies page-index, bloom-filter, and limit pruning,
   `opener/mod.rs:919+`); it must never under-select correctness-wise (it cannot —
   it only warms a cache) and over-selection is bounded by the pacing budget (§2.4).
4. **Projected column chunks.** Map the scan's projection to leaf columns of the
   Parquet schema; for each surviving row group emit each projected column chunk's
   `byte_range()`. Quantize to cache blocks, dedup, and enqueue **in file offset
   order** — the order the record-batch stream will request them.

Degraded modes, chosen for exactness over cleverness:

- **Encrypted files** (`encryption_enabled`): footer prefetch only (step 1 — the
  encrypted footer bytes are still what the scan reads first). Plaintext metadata is
  unavailable without threading key material into the prefetcher; deferred (§4).
- **CSV scans**: the split `[start, start + length)` *is* what the reader consumes,
  so sequential block prefetch of the split range is exact — no metadata step at
  all. This is why the prefetcher lives behind a generic "compute ranges → paced
  fetch" split: the paced fetcher is format-agnostic.
- **Footer parse failure**: skip the file's data prefetch (count it, §2.9). A
  sequential fallback over a Parquet split would over-read unboundedly for selective
  projections, so it is deliberately not attempted.

**Optional seeding (verify-point, phase 3b):** the prefetcher parses each footer
anyway; inserting the parsed `CachedParquetMetaData` into the plan's
`file_metadata_cache` — the same cache `CachedParquetFileReaderFactory` consults
(`parquet_exec.rs:152-157`) — would let the opener skip even the re-parse. Deferred
until the insert-key/validation contract (path + size/etag match) is verified against
DataFusion's cache semantics; the win is milliseconds of CPU, not I/O.

### 2.4 Pacing: the credit model

Direct transliteration of the shuffle fetcher's bounded/credit design onto the cache:

- **Ahead budget (the credit pool).** A per-plan `PrefetchLedger` with budget
  `prefetch.aheadBudget` (default 32 MiB). Before issuing a fetch unit, the
  prefetcher `charge()`s its byte size, awaiting while
  `outstanding > budget`. `outstanding` counts **in-flight + fetched-but-unconsumed**
  prefetched bytes; credit returns when the scan actually consumes a prefetched
  block (first demand hit) or when the cache evicts one. This is the analog of
  `fetchUpToMaxBytes()` refilling as `next()` drains results
  (`ShuffleBlockFetcherIterator.scala:715,1067,1162`): a stalled consumer — most
  importantly a satisfied `LIMIT` upstream that stops polling the stream — freezes
  the window, and plan teardown then cancels the task (§2.2). Backpressure requires
  no plan-shape knowledge at all.
- **Request parallelism.** At most `prefetch.maxConcurrentRequests` (default 3)
  upstream fetches in flight per plan — this is what actually fixes the
  single-outstanding-I/O ceiling — plus a process-wide semaphore (constant, 16
  permits) so many concurrent tasks cannot pile hundreds of prefetch requests onto
  the store client's connection pool ahead of demand reads. The per-request size cap
  reuses the cache's coalescing (`max_coalesce_bytes` = 16 MiB,
  `cache.rs:335-343`), landing near the shuffle fetcher's philosophy of several
  mid-sized parallel requests (`targetRemoteRequestSize = maxBytesInFlight / 5`,
  `ShuffleBlockFetcherIterator.scala:109-112`).
- **Waste is counted, not (yet) acted on.** A wasted release (tagged block evicted
  unconsumed) means the prefetcher outran the cache's ability to hold its output.
  The MVP's protections are structural: the fixed ahead budget bounds how far the
  prefetcher can run, and SIEVE evicts unconsumed prefetch before anything ever
  hit (§2.5), so the damage of over-running is capped at re-fetching the
  prefetcher's own output. Waste is surfaced in the counters (§2.9) — including
  after a unified-memory shrink (`set_memory_budget`, parent §2.9), which evicts
  tagged blocks and shows up as a waste spike. An **adaptive throttle** (shrink
  the effective budget when the wasted fraction climbs, decay it back on clean
  consumption) is deliberately post-MVP (§5, phase 3c): it should be tuned against
  the waste distributions real workloads produce, not guessed at.

  Note the SSD tier's effect on how much any of this matters: with SSD present,
  the largest waste class — warm bytes fetched *too early* for the memory tier to
  hold — is not waste at all but intended placement (§2.10), so the throttle's
  residual constituency is memory-only deployments (`ssd.limit=0`, the default)
  and genuinely *mispredicted* bytes, which no tier can repair (the S3 requests
  were spent, and on SSD they pollute regions and burn write cycles). The
  counters stay in the MVP either way — they are a handful of atomics and the
  only visibility into that residue.

Defaults rationale: 32 MiB ahead ≈ 8 default blocks ≈ 2–3 coalesced requests — enough
to cover one row group's projected chunks for typical row-group sizes, i.e. the
decoder's next unit of demand, without hoarding the memory tier (default 512 MiB)
across the several concurrent tasks an executor runs.

### 2.5 Cache interaction: tags, SIEVE, SSD admission

Prefetched blocks are ordinary cache blocks plus a two-bit intent tag
(`none | prefetch | warm`, beside `visited`/`ever_visited`, `sieve.rs:33-48`).
This section covers the `prefetch` intent (intra-task read-ahead); the `warm`
intent inverts the SSD rule below for reasons argued in §2.10:

- **Single-flight composition.** Demand racing prefetch on the same block joins the
  same in-flight future — never a duplicate GET, in either direction. A demand read
  that arrives first simply means that block's prefetch becomes a no-op probe.
- **Eviction order protects the cache from its prefetcher.** Prefetch inserts with
  `visited = false`, so under pressure SIEVE's hand evicts not-yet-consumed prefetch
  before anything that has ever been hit — pollution is self-limiting and the waste
  counters (§2.9) make it visible.
- **Consumption is not "reuse": SSD admission stays honest.** The parent design's SSD
  admission gate writes only blocks with `ever_visited = true` (accessed ≥ 2 times)
  to filter single-pass scan traffic (`cache.rs:439-448`, parent §2.5). A prefetched
  block consumed exactly once by the scan is precisely single-pass traffic — so the
  *first* demand hit on a tagged block clears the tag, releases ledger credit, sets
  `visited` (SIEVE keeps it) but does **not** set `ever_visited`. Subsequent hits
  behave normally. Without this rule, enabling prefetch would silently convert every
  one-pass scan into SSD writes and wear.
- **Memory accounting**: prefetch fills are ordinary fills inside the cache budget —
  nothing new to size. The ledger bounds how much of the budget prefetch can hold
  unconsumed (≤ `aheadBudget` per plan), not a separate pool.

### 2.6 Failure semantics

Invariants, in order of importance:

1. **Prefetch can never fail or wedge a query.** All prefetch errors are logged at
   debug, counted (§2.9), and abandoned: after 3 failed fetch units for a file the
   file is skipped; after 3 skipped files the plan's prefetch task exits. Fetch
   errors are never cached (existing core guarantee, `cache.rs:361-374`), so the
   demand path retries from scratch exactly as today.
2. **Waiter retry-once.** Today a waiter awaiting another task's in-flight fetch
   propagates that owner's error as its own (`cache.rs:378-391`). With a prefetcher
   in the mix, an owner error is no longer evidence the waiter's own fetch would
   fail (different retry timing, cancellation windows). Change: on owner error *or*
   owner drop, a waiter re-enters the claim phase once — becoming the new owner and
   fetching itself — before surfacing an error. This is a strict robustness
   improvement for the existing demand/demand race too, and it is what makes
   cooperative cancellation airtight from the reader's perspective.
3. **Byte-identity is structural.** The prefetcher writes to the cache only through
   the same block/fetch path as demand reads, against the same `FileKey` namespace
   and ETag reconciliation (parent §2.4); it cannot introduce a data path that
   demand reads don't already exercise.

### 2.7 Task/executor time budget

Accounting of where prefetch work runs:

| Work | Where | Cost |
|---|---|---|
| Build `PrefetchSpec` + `tokio::spawn` | Spark task thread (once per plan) | µs — the only task-thread cost |
| Footer parse, pruning, block math | tokio worker (prefetch task) | ~ms per file, amortized across the scan |
| Range GETs | object-store client (I/O, awaited on tokio) | zero CPU while in flight |
| Credit accounting, tag transitions | inline in existing cache hit/evict paths | one atomic load on hit (tag check); a CAS + notify only on the *first* hit of a tagged block |

The tokio runtime is shared with the scan's own stream-driving tasks
(`jni_api.rs:791`), sized to executor cores. Prefetch tasks are await-dominated;
their CPU slices (footer parse, block math) are bounded and yield at every fetch
unit. No new threads, no new runtime, no change to how many Spark tasks the executor
runs — Spark's scheduler keeps offering the same slots
(`TaskSchedulerImpl.scala:558`) and the query occupies them exactly as before.

### 2.8 Configuration

Same `CometConf` conventions and JNI plumbing as the parent design (§2.8): all
`CATEGORY_SCAN`, force-added to the protobuf `ConfigMap` in
`CometExecIterator.serializeCometSQLConfs` so defaults reach native code.

| Config | Type / default | Meaning |
|---|---|---|
| `spark.comet.scan.dataCache.prefetch.enabled` | boolean, **`false`** | master switch; requires `dataCache.enabled` (the cache is the prefetch buffer). Warn-and-ignore if set without the cache |
| `spark.comet.scan.dataCache.prefetch.aheadBudget` | `bytesConf(ByteUnit.MiB)`, 32 | per-plan credit budget: max in-flight + unconsumed prefetched bytes (§2.4) |
| `spark.comet.scan.dataCache.prefetch.maxConcurrentRequests` | int, 3 | per-plan cap on concurrent upstream prefetch requests |
| `spark.comet.scan.dataCache.prefetch.filterAware` | boolean, `true` | apply row-group statistics pruning when computing ranges (§2.3.3); off = split-range pruning only |
| `spark.comet.scan.dataCache.prefetch.hostWarmSet.enabled` | boolean, `false` | cross-task warming (§2.10): per-host manifests at scan planning, the executor warm agent, and the task-side retry lists. Requires `prefetch.enabled` (warming runs through the same `ScanPrefetcher`); warn-and-ignore otherwise |
| `spark.comet.scan.dataCache.prefetch.hostWarmSet.stageKick.enabled` | boolean, `true` | with the warm-set on, push warm assignments to hosts at scan planning over the Comet RPC endpoint (§2.10); off = warming enters only through the task-side retry path. Kill switch for the RPC surface specifically |

Deliberately constants, not configs: the global request semaphore (16), the
per-file/per-plan error caps. Deployment note for the docs:
a "read-ahead only" deployment — wanting overlap but not cross-query caching — is
simply the cache with a modest `memoryLimit` plus prefetch enabled (warm-set off);
blocks are evicted after consumption by normal SIEVE pressure, and single-pass
traffic is never SSD-admitted (§2.5 — the warm intent's SSD path, §2.10, only
exists when the warm-set is on).

### 2.9 Metrics & observability

Extend `MetricsSnapshot` (`block-cache/src/metrics.rs:50-61`) and the
`getDataCacheStats` JNI array (`jni_api.rs:966`, `data_cache.rs:59-76`):

- `prefetch_bytes_fetched`, `prefetch_fetch_requests` — volume and request shape.
- `prefetch_blocks_consumed` / `prefetch_blocks_wasted` — the coverage story:
  consumed/(consumed+wasted) is the single number that says whether prefetch is
  paying (target ≳ 0.9), and it is the input the post-MVP adaptive throttle
  (§5, phase 3c) will be tuned against.
- `prefetch_errors`, `prefetch_files_skipped`.
- `warm_bytes_fetched`, `warm_blocks_consumed` / `warm_blocks_wasted`,
  `warm_blocks_to_ssd` — the warm-set (§2.10) gets its own consumed/wasted split
  (with eviction-to-SSD counted as intended placement, not waste) so a misfiring
  run-order prediction is visible separately from intra-task prefetch health.
- `warm_kicks_received`, `warm_kick_lead_ms` (time from kick to first demand read
  of a kicked file) — the stage-kick's whole value is lead time; this measures it
  directly and is the go/no-go number for the RPC surface.

`CometDataCacheBenchmark` grows a prefetch column set (§5). Native periodic stats
logging (parent phase 1 behavior) includes the coverage ratio.

### 2.10 Host placement and cross-task ordering

Two questions any cache-warming design must answer explicitly: *how does prefetch
work land on the host whose cache matters*, and *in what order should it run to stay
ahead of the tasks that will actually execute there*.

**Placement: prefetch is never dispatched to a host — it is born there.** There is
deliberately no driver-side dispatch of prefetch work. The prefetch task is spawned
*inside the executor process* by the plan that is already running there (§2.2), so
its placement is inherited from Spark's task scheduling — which the cache-affinity
locality manager already steers: the driver assigns each file a sticky owner host at
scan planning (`CometFileLocalityManager`, parent §2.10), exposes it through
`getPreferredLocations`, and delay scheduling (`spark.locality.wait`, 3s) routes the
task to that host. Whichever host actually runs the task is, by definition, the host
whose cache both the task's demand reads and its prefetcher fill. Even when delay
scheduling falls back to a non-preferred host, the prefetcher warms the fallback
host — exactly where that task's reads happen. The locality manager *is* the
dispatcher; it dispatches tasks, and prefetch rides along. This is what makes
intra-task prefetch locality-correct with zero new channels, zero staleness windows,
and zero misdirected warms.

**Ordering within a task** is already exact (§2.3): files in `FileStream`
consumption order, ranges in file-offset order, paced so the fetch frontier stays
one credit-window ahead of the decoder.

**Ordering across tasks: the host warm-set (default off,
`prefetch.hostWarmSet.enabled`).** Staying ahead of tasks that have not started yet
requires predicting *which* partitions will run on this host and *in what order*.
Both are knowable at scan planning, on the driver:

- *Which*: the driver decided it — the locality manager's per-query assignment,
  computed where `assignFilesForQuery` already runs
  (`CometNativeScanExec.scala:260-269`).
- *What order*: Spark's `TaskSetManager` enqueues pending tasks in **reverse**
  index order precisely "so that tasks with low indices get launched first"
  (`TaskSetManager.scala:215-222`) and dequeues by walking the per-host pending
  list from the end (`TaskSetManager.scala:309-321`) — so a host's pending tasks
  launch in **ascending partition-index order**. That is the "likely ordering of
  actual task runs", and it is stable enough to build on (perturbations —
  failures, speculation, fair-scheduler interleaving — only reorder a hint whose
  worst case is a cache fill consumed later than hoped).

Warming is **one mechanism**: a driver-pushed **stage kick** driving a per-host
executor warm agent. Its retry path is the tasks themselves — each task
re-presents its host's remaining warm list to the same agent, so a kick that
never arrived is repaired without any second delivery system.

**The stage kick.** The moment `doExecuteColumnar` has resolved the post-DPP
file partitions and run `assignFilesForQuery`, the driver pushes each host its
warm assignment — *before* any task of the stage has been scheduled. Everything a
task would eventually deliver is already known here: the per-host manifest
`[(partitionIndex, files, sizes)]` ascending by index, and the scan's common spec
(the already-serialized `NativeScan` common proto — schemas, projection, filters,
store options — the same `commonBytes` every task carries). Lead time is the whole
window between planning and each task's launch: tens of milliseconds on an idle
cluster, but entire *stage lifetimes* when scan tasks queue behind other stages'
tasks on busy slots (the multi-join and busy-cluster cases) — and for later waves
of a multi-wave stage, the time until wave K's slots free up.

- **Channel.** `CometPlugin.executorPlugin()` is `null` today (`Plugins.scala:158`);
  stage-kick adds a `CometExecutorPlugin` that registers a `comet-warm-agent`
  `RpcEndpoint` on the executor's `SparkEnv.rpcEnv` and announces its
  `RpcEndpointRef` to the driver through the *supported* executor→driver path
  (`PluginContext.send`, received by `CometDriverPlugin.receive`). The driver
  plugin keeps a `host → endpointRef` registry and pushes kicks over those refs —
  fire-and-forget, best-effort. The `private[spark]` `rpcEnv` access is the same
  posture Comet already takes for `CometTaskMemoryManager` (parent §2.9); the
  registration handshake itself stays on public plugin API.
- **Executor warm agent.** The kick crosses JNI once
  (`Native.warmScan(commonBytes, files, sizes)`) into a process-global warm agent:
  it resolves the (wrapped) store through the existing global instance cache and
  runs the same `ScanPrefetcher` front-end (§2.3) over the manifest in ascending
  partition-index order, under warm pacing (below). A per-scan seen-set dedups
  against task prefetchers and repeated kicks; single-flight absorbs the rest.
- **Warm pacing is demand-yielding greedy within a capacity-anchored ledger —
  this is where "free-bandwidth mode" went.** Two bounds compose:
  - *Rate*: the agent yields absolutely to demand — warm fetches acquire from the
    global semaphore only when demand traffic is absent. "Demand traffic" is
    measured by the cache core itself: a process-wide gauge of in-flight
    demand-initiated fetches (incremented in `fill_missing`, decremented on
    publish), which the agent samples to drop warm concurrency to one, then
    zero, as demand appears. Per-request sizing is unchanged.
  - *Depth*: a single **process-wide warm ledger** caps unconsumed warm bytes at
    a capacity-derived budget — **50% of `ssd.limit`** when the SSD tier is on,
    **25% of the memory tier** when it is off — shared across all active kicks.
    Charge on fetch; release when a warm block is consumed (first demand hit) or
    terminally wasted (evicted *from SSD* unconsumed, or cancelled). The
    pre-task greedy fill runs up to the budget — that *is* the lead-time capture
    — and past it the warm frontier advances only as consumption returns credit.
    Because the manifest is ordered by launch order (ascending partition index),
    the bounded window always holds the *soonest-needed* files: a gigantic query
    warms a sliding window over its manifest instead of racing through it, so
    the agent never fetches data it will evict before use. This bound is
    load-bearing precisely because the SSD tier cannot provide it: region
    eviction ranks by decayed read score and every unread warm region ties at
    ~zero, so *which* warm data survives an overrun is unspecified — the ledger
    prevents the overrun rather than hoping eviction order favors the window.
- **Warm blocks are SSD-admissible — a deliberate inversion of the prefetch rule.**
  §2.5 forbids intra-task prefetch from setting `ever_visited` because its blocks
  are consumed within seconds and SSD admission would turn single-pass scans into
  device wear. Warm blocks are the opposite case: consumption may be minutes away,
  and a small memory tier *cannot* hold a host's manifest until then — so a warm
  block evicted before consumption is written to the SSD tier (when present)
  rather than counted as waste; eviction-to-SSD is its *intended* path, and
  "wasted" means evicted from SSD too or never consumed before cancel — a wasted
  release that also returns warm-ledger credit (above), so tier overrun both
  surfaces in the counters and self-limits. This needs a two-bit intent tag
  (`none | prefetch | warm`) where §2.5 needed one.
- **Cancellation.** The driver plugin registers a `SparkListener` and pushes a
  warm-cancel on stage completion; the agent also expires each kick on a TTL, and
  demand-yield makes a stale warm nearly free in the meantime. Re-kicks are
  idempotent via the seen-set.

**The retry path.** Each task re-presents its host's warm list — files of
higher-indexed partitions assigned to the host the task is *actually running on*
(self-lookup against the broadcast manifest, capped at 64 files) — appended
**behind** its own files in its `ScanPrefetcher` queue and paced by its own
consumption ledger (§2.4). This is not a second delivery system: it is the same
manifest, the same warm agent, the same seen-set, re-entered from the task side —
on hosts where the kick landed it degenerates to probes. It exists because the
kick is best-effort and blind to scheduling reality: executors that register
after the kick (dynamic allocation scale-up), lost or disabled RPC, and
delay-scheduling fallbacks (a task landing on a non-preferred host warms *that*
host with *that host's* slice — the kick warmed the intended one). Because the
retry path needs no channel, the warm-set never has a hard dependency on the RPC
surface.

### 2.11 What this design deliberately does not do

- **No page-index- or bloom-filter-aware range computation** — prefetch granularity
  is the projected column chunk of a stats-surviving row group; the demand path's
  finer pruning just means some prefetched bytes go unread, bounded by the budget.
- **The push channel is warm-hints-only, best-effort, and never load-bearing.**
  Stage-kick (§2.10) does add a driver→executor RPC surface, but with hard
  boundaries: it carries only warm assignments and cancels, delivery is
  fire-and-forget with the task-side retry path always covering a missed kick,
  and no correctness or scheduling decision ever flows over it. It also ships
  behind its own kill switch.
- **No cross-host warming**: a host only ever warms itself, from assignments
  addressed to it. Nothing prefetches on host A because host B might need it.
- **No Iceberg/Delta coverage in phase 3a** — but no longer a hand-wave: §4
  specifies both integrations (cache coverage first, then the prefetch front-ends)
  against the code as it exists on `main` (Iceberg) and on the stacked fork PRs
  (Delta). The paced fetcher + ledger are format- and storage-neutral by
  construction precisely so those phases add only front-ends and store adapters.
- **No encrypted-footer parsing** (footer-bytes-only prefetch for encrypted files).
- **No upstreaming initially.** arrow-rs's `next_row_group` hook and any future
  DataFusion-native readahead are the right eventual homes for *intra-reader*
  overlap; this design intentionally sits below the reader so Comet ships and tunes
  the capability without forking the opener, and can shrink later if upstream grows
  equivalent machinery (§3.1).
- **No new threads or runtimes; no scheduler or RDD changes.** Driver-side state
  is confined to what §2.10 adds — the warm-agent endpoint registry and the
  stage-completion listener — both owned by the existing `CometDriverPlugin` and
  both inert unless the warm-set is enabled.

## 3. Alternatives considered

1. **Overlap inside the reader: drive `ParquetRecordBatchStream::next_row_group` (or
   a push-decoder pipeline) from a custom opener** — decode row group N while
   fetching N+1. Rejected for v1: Comet uses DataFusion's stock
   `DataSourceExec`/opener stack (`parquet_exec.rs`), and the hook sits under the
   opener's control, so this path means forking the opener + morsel machinery and
   re-forking on every DataFusion bump. It also only overlaps *one row group ahead
   within one file* and does nothing for CSV or footers. The cache-side prefetcher is
   engine-agnostic, byte-budgeted rather than row-group-quantized, and needs no fork.
   Revisit if/when DataFusion grows native readahead — the two compose (the reader's
   fetch simply hits warm blocks).
2. **Contribute readahead to DataFusion upstream first.** Right long-term home, wrong
   first step, same reasoning as the parent design's §3.2 for the cache: iteration
   speed while the pacing/waste heuristics are tuned against real workloads. The
   in-tree prefetcher produces exactly the evidence an upstream proposal needs.
3. **A dedicated prefetch buffer pool with explicit handoff** (Velox
   `CoalescedLoad`-style pins). Rejected: a second memory accounting domain, a
   pin/unpin protocol on the read path, and an ownership transfer channel — all to
   replicate what single-flight + tagged cache blocks already provide. The cache-as-
   buffer design costs a two-bit intent tag and a ledger.
4. **Hard cancellation via `JoinHandle::abort`.** Rejected: owner-drop poisons
   concurrent single-flight waiters (`cache.rs:314-322`), and demand-racing-prefetch
   is the common case. Cooperative tokens bound teardown to ≤ in-flight requests;
   waiter retry-once (§2.6) covers the residue.
5. **Heuristic sequential read-ahead only** (next-block-on-hit, Velox-style density
   gating) instead of plan-driven ranges. Simpler, storage-layer-only — but blind:
   it over-reads selective projections (fetching gaps between projected chunks) and
   under-reads at row-group boundaries, precisely because it lacks the plan. Comet
   *has* the plan natively for free; heuristics survive only as the CSV/whole-split
   mode where they are exact (§2.3).
6. **Task-attached-only warming** (no push channel; earlier drafts of this design
   stopped there). Rejected as the *sole* mechanism because it cannot create lead
   time: warming gated on the first task's arrival starts at exactly the moment
   demand I/O would have started on that host, so the pre-first-task window —
   task-launch latency at best, whole stage lifetimes on busy slots — is wasted.
   Stage-kick (§2.10) claims that window. The inverse extreme — push-*only*
   warming — is also rejected: pushed hints race delay scheduling
   (`spark.locality.wait`, 3s default, `config/package.scala:680-683`) and lose
   executors that register late, while the task that self-identifies its host at
   start cannot be raced, because it *is* the scheduling outcome. Hence one
   kick-driven mechanism whose retry path is the task itself: lead time from the
   kick, ground truth from the retry.
7. **Prefetch whole files on first touch.** Same rejection as the parent design's
   alternative 5: selective column reads touch a fraction of each file; the plan
   knows which fraction.

## 4. Iceberg and Delta integration

The parent design scoped both formats out of the cache's phase 1 because their reads
bypass Comet's `object_store` wrapper. This section specifies how each path gains
**cache coverage** (the prerequisite) and then **prefetch**, against the code as it
actually stands: Iceberg native scan on `main`, Delta native scan on the stacked
fork PRs (`contrib-delta-direct` / `contrib-delta-pr2`). The unifying principle: the
`native/block-cache` core, the global request semaphore, the warm agent, and the
metrics are **shared process-wide singletons**, and the per-plan ledgers are
instances of one shared mechanism — each format contributes only a thin storage
adapter plus a range-computation front-end, and all three formats then draw from
one memory/SSD budget and one pacing regime.

### 4.1 Iceberg (on `main`)

**How it reads today.** `IcebergScanExec` (`execution/operators/iceberg_scan.rs`)
builds an iceberg-rust `FileIO` via
`FileIOBuilder::new(Arc<dyn StorageFactory>)` with `OpenDalStorageFactory`
(`iceberg_scan.rs:345-363`), and drives `iceberg::arrow::ArrowReaderBuilder` over
`FileScanTask`s on Comet's tokio runtime (`iceberg_scan.rs:202-207`). All native
I/O on this path — data files, positional/equality delete files, and the
`fill_delete_file_sizes` stat calls — flows through that `FileIO`; catalog and
manifest reads happen on the JVM side, so the native surface touches only
immutable, snapshot-named files.

**Cache coverage: wrap the `Storage` trait, not opendal.** The pinned iceberg-rust
rev exposes `Storage` and `StorageFactory` as public, third-party-implementable
traits (`iceberg/src/io/storage/mod.rs:72,130`) — a narrower and more stable seam
than an opendal `Layer` (the parent design's §4 assumption, now superseded):

- `CachingStorageFactory { inner: OpenDalStorageFactory, .. }` implements
  `StorageFactory::build` by wrapping the inner `Arc<dyn Storage>` in a
  `CachingStorage`.
- `CachingStorage` routes `reader()` (returning a `FileRead` whose
  `read(range)` — the trait is single-range today, `file_io.rs:251-256` — calls
  `BlockCache::get_ranges`) and `read()` through the cache; `metadata`, `exists`,
  and all write/delete methods pass through (delete invalidates). The cache's block
  coalescing compensates for the single-range `FileRead` API on adjacent reads.
- Injection point: `IcebergScanExec::load_file_io` swaps in the caching factory
  when `data_cache::global()` is `Some`. One line of policy, zero changes to
  iceberg-rust.
- Because the adapter implements iceberg's traits, it lives beside them:
  `native/core` (which already depends on `iceberg`) or a sibling
  `native/iceberg-storage-cache` crate. It must **not** go into
  `native/block-cache` (storage-neutrality guard) or `native/object-store-cache`
  (that crate's contract is "only crate touching the `object_store` trait").
- **`typetag::serde` wrinkle**: `Storage`/`StorageFactory` are serde-serializable
  by contract. The adapter serializes only its inner factory's config; on
  deserialize it re-attaches the process-global cache via `data_cache::global()`
  (a `OnceLock` singleton, so this is well-defined at any point after first
  `createPlan`).
- **Versioning**: opendal reads don't surface ETags per-request the way
  `GetResult::meta` does, so the Iceberg fetcher captures `FileVersion` from size
  (known up front via `FileScanTask::file_size_in_bytes` or `Storage::metadata`).
  This is sound under Iceberg's format guarantee — data/delete files are never
  overwritten in place and paths are unique per snapshot — a strictly *stronger*
  immutability contract than the Hive-style one the parent design already accepts
  (§2.4 there).
- **Namespace**: hash of the scheme plus the storage-relevant property bag
  (the `STORAGE_PROPERTY_PREFIXES`-filtered set already computed at
  `iceberg_scan.rs:354-360`) **excluding credential values** — see §4.3.

**Locality follows coverage.** The parent design excluded
`CometIcebergNativeScanExec` from cache-affinity scheduling only because there was
no cache to hit (parent §2.10). Once the storage adapter lands, the exec feeds its
tasks' `data_file_path`s into `CometFileLocalityManager.assignFilesForQuery` and
returns preferred hosts exactly as `CometNativeScanExec` does — the manager is
format-agnostic by construction.

**Prefetch front-end.** The planner's `IcebergScan` arm (`planner.rs:1566-1600`)
deserializes the partition's `FileScanTask`s at plan creation — the same moment the
Parquet arm builds its spec — and each task carries everything the range computation
needs: `data_file_path`, `file_size_in_bytes`, `start`/`length`,
`project_field_ids`, `predicate: Option<BoundPredicate>`, and the delete-file list
(`iceberg-rust scan/task.rs:55-90`). Per task:

1. Footer prefetch + parse through `CachingStorage` (same tail-range trick as
   §2.3.1; `file_size_in_bytes` is already known, saving even the stat).
2. Row groups overlapping `[start, start + length)`.
3. Projected columns mapped by **Parquet field id** (`project_field_ids` — Iceberg's
   projection contract) to leaf column chunks.
4. Statistics pruning is **v2** for Iceberg: iceberg-rust's `ArrowReader` prunes row
   groups internally from the `BoundPredicate`, and replicating that requires either
   its internals becoming public or a small converter from `BoundPredicate` to a
   DataFusion `PruningPredicate`. Range + projection pruning alone already bounds
   the fetch set tightly; the `filterAware` knob simply has no effect on Iceberg
   until v2.
5. Delete files are prefetched whole (they are small and read eagerly by the
   MOR machinery) ahead of the data files they mask.

Pacing, ledger, cancellation, and metrics are the shared machinery of §2.4–2.9,
unchanged — the prefetch task is spawned from the same planner-accumulated spec list
and cancelled by the same `releasePlan` path.

### 4.2 Delta (stacked fork PRs)

**How it reads today.** The Delta contrib is an **rlib linked into `libcomet`**
(`contrib/delta/native/Cargo.toml` — "never a cdylib on its own"), which is the
load-bearing fact: it shares core's process globals, so `data_cache::global()`, the
ledger, and the metrics are directly reachable — no cross-library bridging. It pins
the same `object_store` 0.13 / arrow 58 versions as core via delta-kernel 0.24's
`arrow-58` feature. All kernel I/O — log replay, checkpoints, data files, deletion
vectors — goes through the `Arc<dyn ObjectStore>` that
`create_object_store` (`contrib/delta/native/src/engine.rs`) builds and hands to
delta-kernel's `DefaultEngine`, which is cached per
`(scheme, authority, DeltaStorageConfig)` in an LRU-bounded engine cache.

**Cache coverage: wrap at `create_object_store`.** Exactly the parent design's §2.7
pattern, at the Delta contrib's equivalent choke point: when the global cache is
enabled and the scheme is remote, wrap the built store in `CachingObjectStore`
(the existing `native/object-store-cache` crate — same trait, same versions, zero
new adapter code) before it enters the engine cache. Two Delta-specific rules:

- **Bypass `_delta_log/_last_checkpoint`.** It is the one object on the Delta read
  path that is *overwritten in place* by design. The cache's ETag reconciliation
  would eventually catch an overwrite, but only on a miss — a hit never
  revalidates — so a cached `_last_checkpoint` could pin log replay to a stale
  checkpoint indefinitely. It is a single small JSON read per snapshot resolution;
  the wrapper passes any path ending in `_delta_log/_last_checkpoint` straight
  through. Version-named commit JSONs and checkpoint Parquet files are immutable
  once visible and cache normally — warm log replay is a real win for repeated
  queries against the same table version.
- **Namespace from the engine key, minus credentials** (§4.3): `(scheme,
  authority)` plus non-credential settings (region, endpoint, path-style). The
  fork's own engine-cache commentary documents hourly STS/IRSA token rotation;
  a namespace that included `aws_session_token` would silently discard the entire
  cache every rotation.

**Locality follows coverage**, as with Iceberg: `CometDeltaNativeScanExec` feeds its
per-partition data-file paths into `CometFileLocalityManager` once its reads
actually hit a local cache.

**Prefetch front-end.** `DeltaKernelScanExec` executes from serializable per-file
inputs — path, size, DV descriptor, partition values
(`contrib/delta/native/src/kernel_scan.rs`) — and the physical read schema is known
at plan time (kernel matches file columns by Parquet field id,
`kernel_scan.rs:89`). The front-end is therefore the Parquet computation of §2.3
with Delta's inputs:

1. Footer prefetch + parse via the wrapped store (size known — no stat).
2. Delta scans are whole-file (no split ranges on this path), so range pruning is
   the identity; row-group *statistics* pruning against the scan's physical
   predicate reuses the same public DataFusion machinery as §2.3.3 — the contrib
   already depends on `parquet` 58 directly, and the file schema is plain Parquet.
3. Projected columns by field id → chunk ranges.
4. **Deletion vectors first**: the per-file DV (inline bitmaps aside) is a small
   object read before the file's rows can be emitted — prefetch it ahead of the
   data chunks, same ordering logic as Iceberg's delete files.

Spawn/cancel lifecycle is identical: the dispatcher arm that builds
`DeltaKernelScanExec` accumulates a spec; `jni_api` spawns and `releasePlan`
cancels. Because prefetch rides the *store instance's* inherent method, the
namespace and inner store are automatically the ones kernel itself reads through —
prefetched blocks are exactly the blocks kernel's parquet handler will request.

### 4.3 Shared consideration: cache namespaces must not include credentials

The parent design namespaces cache entries by `hash(url_key, config_hash)` — the
key that distinguishes store *instances*. All three formats need the sharper rule
made explicit: the **cache namespace must be derived from storage identity**
(scheme, authority/bucket, endpoint, region, path-style) **and never from
credential material** (access keys, session tokens). Rotating credentials produce a
new store instance — correct, connections must be rebuilt — but the same bytes; a
credential-sensitive namespace silently zeroes cache effectiveness on every
STS/IRSA rotation (hourly in the deployments the Delta fork's engine cache was
hardened for). This applies retroactively to the core Parquet path's namespace
choice and should be verified there as part of phase 3a; two stores that differ
*only* in credentials and would read different bytes (different authorization ⇒
different visibility) do not exist for immutable-object workloads — and a read the
credential is not authorized for fails at fetch time before anything is cached.

## 5. Phasing

- **Phase 3a — the MVP.** `PrefetchLedger` (fixed budget — no adaptivity) + tagged blocks +
  `prefetch_ranges` + waiter retry-once in `native/block-cache`; the inherent
  wrapper method in `native/object-store-cache`; `ScanPrefetcher` (footer → prune →
  chunk ranges → paced fetch) + planner/`ExecutionContext` lifecycle wiring in
  `native/core`; Parquet + CSV coverage; configs, metrics, benchmark columns;
  credential-free namespace audit (§4.3). Default off.
- **Phase 3b — the host warm-set** (§2.10) as one unit behind one flag: manifest
  broadcast, executor warm agent, stage-kick (the `CometExecutorPlugin`, RPC
  endpoint handshake, driver listener), and the task-side retry path — one
  mechanism, so shipping it split would mean shipping only its retry path. Value
  measured by `warm_kick_lead_ms`.
- **Phase 3c — post-MVP follow-ups**, each gated on 3a/3b metrics: the
  **adaptive waste throttle** (shrink the effective ahead budget on high wasted
  fraction, decay back on clean consumption — tuned against the waste
  distributions 3a's counters reveal, and the reactive half of the
  unified-memory-shrink story in §2.4); parsed-metadata cache seeding (§2.3);
  encrypted-footer support (thread the decryption factory into the prefetcher);
  page-index-aware range refinement.
- **Phase 4a — Iceberg cache coverage** (§4.1): `CachingStorageFactory` /
  `CachingStorage` adapter, `load_file_io` wiring, locality manager hookup for
  `CometIcebergNativeScanExec`. Supersedes the parent design's "opendal layer
  adapter" plan with the narrower `Storage`-trait seam.
- **Phase 4b — Iceberg prefetch** (§4.1): `FileScanTask`-driven front-end
  (range + field-id projection; stats pruning when the `BoundPredicate` converter
  lands), delete-file prefetch.
- **Phase 5a — Delta cache coverage** (§4.2, lands on the fork's stacked PRs and
  rides them upstream): `CachingObjectStore` wrap in `create_object_store`,
  `_last_checkpoint` bypass, locality hookup.
- **Phase 5b — Delta prefetch** (§4.2): per-file front-end with stats pruning and
  DV-first ordering.
- **Relation to the parent roadmap**: 3a–3c are the parent design's "Phase 3 — item
  #23 polish, readahead half"; 4a/5a replace and sharpen its phase-4 sketch. The
  ledger and paced fetcher are shared by every phase; only front-ends and store
  adapters accumulate.

## 6. Test plan

- **Core (`native/block-cache`)**: ledger charge/await/release under concurrent
  consume+evict; tag lifecycle — prefetch insert leaves `visited=false`, first hit
  clears tag + releases credit + does **not** set `ever_visited`, second hit sets it
  (SSD admission unchanged — regression test that a prefetch+single-scan workload
  admits zero SSD writes); waste counters account exactly under concurrent
  consume/evict (the post-MVP throttle's input data); cancellation between
  fetch units leaves no in-flight entries; waiter retry-once on owner error and on
  owner drop (both demand/demand and demand/prefetch interleavings); budget stall on
  stopped consumption and wake on release.
- **Prefetcher unit (`native/core`)**: computed block set equals the blocks a real
  scan of the same file/projection/filters touches when only range+stats pruning
  applies, and is a superset when page-index pruning applies (write files with page
  indexes to force it); split-range pruning matches the opener's midpoint rule;
  nested-type projection maps to the right leaves; encrypted spec degrades to
  footer-only; CSV spec covers exactly the split; footer-parse failure skips the
  file and counts it.
- **Integration (`parquet_exec` tests, request-counting store)**: cold scan with
  prefetch returns byte-identical batches; observed upstream request concurrency > 1
  (the #3817 fix, asserted via a max-in-flight counter on the shim); second scan
  issues zero upstream requests; `LIMIT`-style early stream drop bounds fetched
  bytes to footer + aheadBudget + in-flight slop; injected upstream failures fail
  prefetch (counted) while the query succeeds; prefetch disabled ⇒ zero prefetch
  tasks, zero new counters.
- **Scala suite**: result parity with prefetch enabled across the standard scan
  suites (local FS + MinIO/S3); counters assert nonzero consumed and low wasted on a
  repeated-scan workload; kill/early-cancel test (task interruption) leaves the
  executor healthy and later queries unaffected.
- **Warm-set (§2.10)**: queue-order test — a task's prefetch queue lists its own
  files first, then same-host successors in ascending partition index; manifest
  self-lookup returns the *actual* host's slice when a task runs on a
  non-preferred host; per-stage dedup — N concurrent tasks on one host enqueue
  each warm file once; plan release cancels the warm remainder and the successor
  task re-covers it; a multi-executor run asserts no host ever fetches a file
  assigned to (and consumed on) another host; warm waste counters populate when
  run order is deliberately inverted (scheduler stubbed).
- **Stage-kick (§2.10)**: endpoint registration handshake (executor announces,
  driver registry populates, executor loss prunes); a kicked host begins warm
  fetches before its first task launches (assert `warm_kick_lead_ms > 0` with a
  slot-starved scheduler); demand-yield — injecting demand reads mid-drain drops
  warm request concurrency to zero, then it recovers; warm blocks evicted
  unconsumed land on SSD (`warm_blocks_to_ssd`) and a later task reads them from
  the SSD tier with zero upstream fetches; **gigantic-manifest test** — a manifest
  several times the warm budget never holds more than the budget unconsumed, the
  lowest-indexed (soonest-needed) files are never evicted before consumption, and
  the frontier advances as consumption returns credit (both SSD and memory-only
  budgets); stage completion pushes warm-cancel and the TTL expires an orphaned
  kick; kick
  disabled / RPC lost ⇒ the task-side retry path converges to the same cached set;
  a kicked-but-rescheduled stage (task lands on a different host) wastes at most
  the kicked bytes and never blocks the query.
- **Benchmark (`CometDataCacheBenchmark`)**: add a *cold + prefetch* arm beside the
  existing cold/warm arms — the headline number is cold-scan wall time cache-only
  vs. cache+prefetch at equal `ColdFetch` bytes, plus the coverage ratio; scenarios
  to include single-large-file (row-group pipelining), many-small-files (footer +
  next-file overlap), selective projection (filter-aware fetch volume ≪ full scan),
  and a `LIMIT` query (bounded waste). Single- and multi-task runs to exercise the
  global semaphore.
- **Iceberg (phase 4)**: `CachingStorage` adapter tests over an in-memory opendal
  backend (zero-refetch on second read, byte-exactness vs. unwrapped, delete
  invalidation, serde round-trip of the caching factory re-attaching the global
  cache); MOR correctness with cache + prefetch enabled (positional and equality
  deletes, DVs prefetched before data); field-id projection mapping incl. renamed
  columns; the existing Iceberg suites re-run with cache+prefetch on.
- **Delta (phase 5, on the fork's stacked PRs)**: `_last_checkpoint` staleness
  test — overwrite the checkpoint pointer between two queries and assert the second
  query replays from the new version with the cache enabled; log-replay warm test
  (second snapshot resolution issues zero GETs for immutable commits/checkpoints);
  DV-masked scan parity with prefetch on; namespace stability across a simulated
  credential rotation (same table, new session token, nonzero cache hits — §4.3).

## 7. Risks

| Risk | Mitigation |
|---|---|
| Prefetch evicts hot cached data (pollution) | inserts are SIEVE-coldest (`visited=false`, §2.5); per-plan ahead budget; waste counters make residual pollution visible (§2.9); adaptive throttle post-MVP (§5) |
| Prefetcher outruns decoder, thrashing a small memory tier | credit ledger counts unconsumed bytes, not just in-flight; wasted blocks were the coldest anyway; damage capped at re-fetching the prefetcher's own output (§2.4) |
| Extra GET volume / S3 request cost on selective scans | filter-aware pruning reuses the opener's own row-group filters (§2.3); over-read bounded by ahead budget; `filterAware` + `enabled` kill switches; coverage ratio metric makes waste visible |
| Prefetch requests starve demand reads at the store client | per-plan request cap (3) + global semaphore (16) below typical client pool sizes; demand joins in-flight prefetch fetches instead of queueing behind them |
| Query fails due to a prefetch-side error | errors never cached (existing core invariant); waiter retry-once (§2.6); error caps degrade prefetch to off for the file/plan, never the query |
| Teardown races: task ends while prefetch mid-fetch | cooperative cancellation bounded to in-flight requests; completed publishes still benefit later queries; no aborts ⇒ no poisoned waiters |
| SSD wear from prefetched single-pass traffic | consumption does not set `ever_visited`; admission gate semantics preserved (§2.5) |
| Task-thread overhead violates the "no task time" requirement | only spec-build + spawn on the task thread (µs, §2.7); everything else on the existing tokio runtime |
| LIMIT / early-terminated queries prefetch data nobody reads | consumption-based credit freezes the window when polling stops; plan release cancels; waste ≤ ahead budget |
| DataFusion upgrade churn (pruning APIs) | uses only exported DF types (`RowGroupAccessPlanFilter`, `ParquetAccessPlan`, `build_pruning_predicate`) compiled in-tree on every bump, same posture as the adapter crate (parent §2.6) |
| Config set without the data cache | warn-and-ignore; documented requirement (§2.8) |
| Warm-set predicts the wrong task order (speculation, failures, fair scheduling) | order is a hint over an optimistic cache fill — worst case is later-consumed or wasted bytes, bounded by the shared ledger; dedicated warm waste counters make a misfiring host visible (§2.9); default off; adaptive throttle post-MVP (§5) |
| Warm-set broadcast grows with very large scans | manifest is `O(total files)` of paths+indices, same order as the closure data Spark already ships; per-task warm list capped at a constant (64 files) |
| Stage-kick RPC surface (private[spark] `rpcEnv`) breaks across Spark versions or deployment modes | registration handshake stays on public plugin API; endpoint use follows the established `CometTaskMemoryManager` in-`org.apache.spark` posture; dedicated kill switch (`stageKick.enabled`); the task-side retry path makes a dead channel a latency regression, never a functional one |
| Kicked warming competes with live queries for network / store connections | demand-yielding pacing: warm concurrency drops to zero while any demand fetch is in flight (§2.10); global request semaphore unchanged |
| Gigantic query: warm drain outruns tier capacity, evicting warm data before its task runs | process-wide warm ledger caps unconsumed warm bytes at 50% of SSD (25% of memory when SSD off); fill-to-budget then slide with consumption; launch-ordered manifest keeps the window on the soonest-needed files; SSD eviction of unconsumed warm returns credit as visible waste (§2.10) |
| Stage-kick warms a host the scheduler then bypasses | manifest *is* the locality preference the scheduler will follow, so this requires a `locality.wait` fallback; bounded by kicked bytes, visible in `warm_blocks_wasted`, self-corrected by the task-side retry path on the actual host |
| Stale `_last_checkpoint` served from cache pins Delta log replay to an old version | explicit path bypass in the Delta wrapper (§4.2); staleness regression test (§6) |
| Credential rotation (STS/IRSA) silently discards the cache | namespaces derived from storage identity, never credential material (§4.3); rotation test asserts warm hits survive a token change |
| iceberg-rust `Storage`/`StorageFactory` trait churn (git-pinned dep) | adapter is a thin in-tree crate compiled against the pinned rev; every rev bump compiles it in the same PR — same posture as the `object_store` adapter (parent §2.6) |
| Iceberg prefetch over-fetches without stats pruning (v1 is range+projection only) | projection by field id already excludes unread columns; ahead budget bounds the rest; `BoundPredicate`→`PruningPredicate` converter tracked as phase 4b |
| Delta contrib globals diverge from core (separate library) | non-risk by construction: the contrib is an rlib linked into `libcomet`, sharing `data_cache::global()`, the ledger, and metrics (§4.2) |
