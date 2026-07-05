# Design: Object Store Data Caching for DataFusion Comet

*2026-07-03. Targets datafusion-comet `main` with DataFusion 54.0.0. Companion to the
parent proposal [apache/datafusion-comet#4695](https://github.com/apache/datafusion-comet/issues/4695)
(local data caching gap vs. Gluten/Velox; folds in footer caching / readahead polish
as follow-up scope, not core scope).*

## 1. What it is

Every Comet scan today re-fetches data bytes from S3/HDFS/ABFS. The only thing cached
is *parsed Parquet metadata*, and even that only per plan: `parquet_exec.rs:152-157`
resolves the store from the per-plan `RuntimeEnv` and wraps it in DataFusion's
`CachedParquetFileReaderFactory` (footer + page index, bounded LRU) — and Comet builds
a **fresh `RuntimeEnv` per plan** (`jni_api.rs:572`,
`SessionContext::new_with_config_rt`), so nothing survives across tasks, let alone
across queries. A hot table scanned by ten queries pays ten full rounds of GETs.

Velox's answer — the reference architecture, not the port target — is a two-tier cache
fed by the read path (`velox/velox/common/caching/`, `velox/dwio/common/`):

- **`AsyncDataCache`**: in-memory, 4 shards by default (`kDefaultNumShards`,
  `AsyncDataCache.h:800`), entries keyed by `(fileNum, offset)`, pin-counted
  (Shared/Exclusive), evicted by a clock hand that keeps entries whose
  `score = (now − lastUse)/(1 + numUses)` beats a threshold recalibrated from the 80th
  percentile of 10 sampled entries (`kEvictionPercentile`/`kMaxEvictionSamples`,
  `AsyncDataCache.h:710-711`, `AsyncDataCache.cpp:517-616`).
- **`SsdCache`/`SsdFile`**: one file per shard, laid out as 64 MiB regions
  (`kRegionSize = 1<<26`, `SsdFile.h:320`); entries never span regions and max out at
  8 MiB (`kSizeBits = 23`); writes are batched from memory-tier evictions; eviction is
  **wholesale per region** by decayed read score; optional crc32 checksums and a
  checkpoint file (`"CPT1"`/`"CPT2"` versions, `SsdFile.h:446-448`) for warm restart.
- **`CachedBufferedInput`**: quantizes reads to an 8 MiB `loadQuantum`
  (`common/io/Options.h:65`), coalesces neighbors (512 KiB gap / 128 MiB max,
  `Options.h:66-67`; 20 KB gap for SSD reads, `CachedBufferedInput.cpp:297`), and
  schedules prefetch on an executor for columns whose tracked read density ≥ 80%
  (`cache_prefetch_min_pct`, `flag_definitions/flags.cpp:118-121`).

Databricks' disk cache is the same idea. The gap-assessment verdict: Comet needs a
**block-aligned local cache (memory + SSD) behind the `object_store` API**, with the
cache core kept storage-API-neutral.

Packaging: **everything lives in the Comet tree** — no third-party cache dependency.
Two new workspace crates with a strict layering split: `native/block-cache`, the
storage-API-neutral cache core (no `object_store`, `opendal`, or Comet deps; the
caller supplies a fetcher), and `native/object-store-cache`, the thin
`object_store::ObjectStore` wrapper over it. The split is a design discipline, not a
distribution plan: the core stays swappable/upstreamable and the wrapper is the only
code touching the `object_store` trait, so DataFusion bumps compile both in the same
PR. The workloads justify the core's central assumption: Parquet data files in
Iceberg/Delta/Hive-style lakes are never modified in place, so file-version
validation is cheap (§2.4).

A cache alone is not enough in a real cluster, and this design treats scheduling as
part of the feature, not a follow-up. With K executors and Spark's default
locality-blind scheduling of S3-backed scans, consecutive scans of the same file land
on arbitrary hosts, so each host's private cache sees roughly `1/K` of the re-reads —
at 50 executors the cache is ~2% effective no matter how good the eviction policy is.
Comet owns the scan RDD (`CometExecRDD`), so it can declare **preferred locations**
per partition and route repeat reads of a parquet file back to the host that cached
it (§2.10), the same way `indextables_spark` routes split reads today.

## 2. How it works (this design)

### 2.1 Architecture

All in the Comet tree, as two new native workspace crates plus wiring:

```
   Comet native workspace (native/Cargo.toml:20)
   ┌─────────────────────────────────────────────────────────────────┐
   │ native/block-cache  (storage-API-neutral: no object_store/      │
   │ opendal deps — tokio/bytes-class primitives only)               │
   │  block store: memory tier (sharded, SIEVE) + SSD tier           │
   │  (region files, crc32c) · file-version (ETag) validation ·      │
   │  single-flight miss dedup · coalesced miss fetch · metrics      │
   └────────────▲────────────────────────────────────────────────────┘
                │ workspace dep
   ┌────────────┴───────────────────────────────┐
   │ native/object-store-cache                  │
   │  CachingObjectStore wraps dyn ObjectStore; │
   │  [later: opendal layer for Iceberg]        │
   └────────────▲───────────────────────────────┘
                │ workspace dep
   ┌────────────┴────────────────────────────────────────────────────┐
   │ native/core: prepare_object_store_with_configs wraps each new   │
   │ store (parquet_support.rs:621-625); process-global BlockCache   │
   │ in a OnceLock, init from JNI configs + Spark local dirs         │
   ├──────────────────────────────────────────────────────────────────┤
   │ Comet JVM (driver): CometFileLocalityManager assigns file →    │
   │ host; CometExecRDD.getPreferredLocations routes tasks (§2.10)   │
   └─────────────────────────────────────────────────────────────────┘
```

- **`native/block-cache`**: storage-API-neutral by construction — depends on
  `tokio`, `bytes`, `dashmap`-class primitives only; the caller supplies a fetcher,
  the core never talks to storage itself. Neutrality is enforced by the crate
  boundary (CI-checkable: no `object_store` in its dependency tree), which keeps the
  tier internals swappable (§3.3) and the crate extractable later if it ever needs
  an out-of-tree life (§3.1).
- **`native/object-store-cache`** (workspace member alongside `core`, `common`,
  `proto` etc.): implements `object_store::ObjectStore` around an inner store + a
  shared `Arc<BlockCache>`. The only crate touching the `object_store` trait; it
  compiles against the workspace `object_store` version on every DataFusion bump.
  Reusable by the delta-kernel-rs path (§4) and upstreamable later (§3.2).
- **Comet integration**: store wrapping + `CometConf` plumbing (§2.7–2.8) on the
  native side; driver-side cache-affinity scheduling (§2.10) on the JVM side.

Core public surface (sketch):

```rust
impl BlockCache {
    /// Serve `ranges` of `file`, block-aligned internally; misses go through
    /// `fetcher` exactly once per block regardless of concurrent callers.
    pub async fn get_ranges(&self, file: &FileKey, ranges: &[Range<u64>],
                            fetcher: &dyn RangeFetcher) -> Result<Vec<Bytes>>;
    pub fn invalidate_file(&self, file: &FileKey);
    /// Change the memory-tier budget at runtime. A reduction evicts (SIEVE order)
    /// until under the new budget before returning; pinned/in-flight blocks are
    /// excluded, so the target is met as they complete. Day-one API: phase 1 calls
    /// it never, phase 2's unified-memory integration calls it on grant/shrink
    /// (§2.9) — the tiers must not assume a constant budget.
    pub fn set_memory_budget(&self, bytes: u64);
}
pub trait RangeFetcher: Send + Sync {
    /// Fetch absolute byte ranges; returns bytes plus the file version
    /// (ETag / last-modified / size) observed by this fetch.
    async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)>;
}
```

### 2.2 Cache core: block store, keying, miss handling

- **Block alignment.** Reads are quantized to a configurable power-of-two block size,
  **default 4 MiB** (Velox's quantum is 8 MiB; `object_store`'s own range coalescing
  merges ≤1 MiB gaps, `OBJECT_STORE_COALESCE_DEFAULT`, `object_store-0.13.2/src/util.rs:92`.
  4 MiB keeps over-read amplification tolerable for selective column reads while
  staying in S3's GET-throughput sweet spot; the config allows 1–16 MiB). The final
  block of a file is short (file size learned from the first fetch's `FileVersion`).
- **Keying.** `FileKey = (namespace: u64, path)` where the namespace identifies the
  logical store (Comet passes a hash of its `(url_key, config_hash)` store key, §2.7);
  paths are interned to a `u64` file id (Velox's `StringIdMap` pattern), and block keys
  are `(file_id, block_index: u32)` — 12 bytes, cheap to hash and shard.
- **Memory tier.** N shards (default 16), each a hash map from block key to
  `Arc<Block { data: Bytes, visited: AtomicBool }>` plus a SIEVE queue (§2.3). Hits
  clone the `Arc` and set `visited` — no lock ordering with other shards, no list
  splice on the hot path.
- **Single-flight dedup.** A per-shard in-flight map from block key to a shared future.
  The first task to miss a block inserts the future and drives the fetch; concurrent
  tasks (Spark runs many tasks per executor against the same hot files) await the same
  future. Fetch errors propagate to all waiters and the entry is removed so the next
  caller retries — errors are never cached.
- **Coalesced miss fetch.** Within one `get_ranges` call, runs of *adjacent missing
  blocks* are merged into a single upstream fetch (capped at 16 MiB per request, i.e.
  4 default blocks); cached blocks are never re-fetched to bridge gaps. This mirrors
  Velox's coalescing at reference level without its density tracking — the block grid
  already makes requests contiguous where it matters.

### 2.3 Eviction: SIEVE (memory), region scores (SSD)

**Memory tier: SIEVE**, per shard. A FIFO queue with one `visited` bit per entry and a
hand that walks tail→head on eviction: visited entries survive (bit cleared, position
kept), unvisited entries are evicted; new blocks insert at head. Chosen over
alternatives because:

- **Hit path is one atomic store.** LRU needs a list splice under a lock per hit;
  CLOCK and SIEVE need only the bit. For a cache fronting multi-GB/s scans, hit-path
  cost dominates.
- **One-hit-wonder resistance.** Large scans push blocks through the cache exactly
  once. Under SIEVE, a never-revisited block is evicted on the hand's first pass while
  re-used blocks stay put (lazy promotion, quick demotion) — measurably better than
  LRU/CLOCK on skewed traces (SIEVE, NSDI '24) and precisely the scan-heavy pattern
  here. CLOCK recycles survivors circularly and filters one-hit blocks more weakly.
- **Less machinery than Velox's policy.** Velox's clock needs per-entry access
  timestamps, use counts, and periodic percentile recalibration
  (`AsyncDataCache.cpp:626+`); SIEVE gets comparable behavior from one bit. If real
  workloads prove otherwise, the policy is a per-shard implementation detail behind
  the core API — swappable without touching adapters.

**SSD tier: wholesale region eviction** (Velox's design, adopted as-is in shape):
regions are ranked by a decayed bytes-read score (`SsdFileTracker` analog); the
lowest-scored unpinned region is erased and reused in full. Evicting whole regions
avoids free-list fragmentation and keeps writes sequential (SSD-friendly). Per-block
LRU on disk is deliberately rejected — it turns the write path into random I/O.

### 2.4 Validation: ETag capture, immutability

- The first fetch of any block of a file captures the store's object version —
  `e_tag`, `last_modified`, `size` from `ObjectMeta`
  (`object_store-0.13.2/src/lib.rs:1420-1434`) — into the file entry.
- Every subsequent **miss** fetch returns meta too (piggybacked, zero extra requests):
  if it disagrees with the stored version, all cached blocks of the file are
  invalidated, the entry is re-created at the new version, and the fresh bytes are
  served. Hits do not revalidate.
- This is sound because the target workloads read **immutable objects**: Iceberg and
  Delta never modify data files in place (new files + metadata swap), and classic
  Hive tables overwrite at partition granularity with new file names. Validation
  therefore costs nothing in steady state and still
  self-heals if a file *is* overwritten (first miss after the overwrite flips the
  version). A `head`-based revalidation TTL for hostile environments is a deliberate
  non-goal in this phase (§2.11).

### 2.5 SSD tier

Phase 2 (§4); designed now so the core API doesn't churn:

- **Layout.** One append-oriented file per SSD shard under the configured directory,
  sized in 64 MiB regions up to `ssd.limit`; blocks never span regions (block ≤ 16 MiB
  ≪ region). An in-memory index maps block key → (region, offset, len, crc32c).
- **Admission & writes.** On memory-tier eviction, a block is written to SSD only if
  its `visited` bit was ever set (accessed ≥ 2 times) — the cheap analog of Velox's
  `shouldSaveToSsd` admission, filtering single-pass scan traffic that would only wear
  the device. Writes are batched per region, off the read path (background task);
  read-side misses never block on SSD writes.
- **Checksums.** crc32c computed at write, stored in the index, verified on every SSD
  read (~GB/s, negligible vs. NVMe). A mismatch drops the entry and falls through to
  the network — corruption is a miss, never an error.
- **Crash behavior: cold start.** The index lives in memory only; on executor start
  the cache unlinks any stale files in its directory and starts empty. Velox's
  checkpoint/recovery (`SsdFile.h:412-448`) is explicitly deferred — executors are
  long-lived relative to cache warm-up, and checkpoint correctness (stale-checkpoint
  detection, versioned formats) is a large fraction of Velox's SSD code for a
  second-order win.

### 2.6 The `object_store` wrapper (`native/object-store-cache`)

`CachingObjectStore` wraps an `Arc<dyn ObjectStore>` + `Arc<BlockCache>` + namespace.
In `object_store 0.13.2`, `get_opts` is the one required method and everything
defaults through it (`lib.rs:891`; `get_range` default = `get_opts` with a bounded
range, `lib.rs:1358`; `get_ranges` default = `coalesce_ranges` over `get_range`,
`lib.rs:895`). The wrapper intercepts:

- **`get_range` / `get_ranges`** → `BlockCache::get_ranges` with a fetcher that calls
  the inner store's `get_opts` and harvests `GetResult::meta` for version capture.
  These two methods carry **all Parquet I/O in Comet's path**: DataFusion 54's footer
  read drives `ParquetMetaDataPushDecoder` via `store.get_ranges` with bounded ranges
  computed from the known file size
  (`datafusion-datasource-parquet-54.0.0/src/metadata.rs:167-192` — no suffix reads),
  and column-chunk reads go through `ParquetObjectReader::get_bytes/get_byte_ranges`
  (`reader.rs:104-123`). Nothing in the DF 54 parquet path sends conditional
  `GetOptions` (verified: no `if_match`/`version` usage in `datafusion-datasource*`).
- **`get_opts`** → routed through the cache only when it is a plain
  `GetRange::Bounded` with no conditions/version/head; anything else passes through.
- **`get`, `head`, `list*`, `copy*`** → passthrough (full-file streaming reads are not
  cached in this phase). **`put*` / `delete`** → passthrough + `invalidate_file`.

A useful consequence: raw **footer bytes are block-cached process-wide** even though
the *parsed* metadata LRU stays per-plan — the second plan touching a file re-parses
from local memory instead of re-fetching 512 KiB from S3 (Comet sets
`with_metadata_size_hint(512 * 1024)`, `parquet_exec.rs:135`). That is most of gap
item #23's footer half, for free; readahead is the remaining half (§4).

### 2.7 Integration point in Comet

Comet already has exactly one choke point where every native `ObjectStore` is born:
`prepare_object_store_with_configs` (`parquet_support.rs:569-632`), called from the
native scan (`planner.rs:1452`), CSV scan (`planner.rs:1504`), the JNI parquet reader
(`parquet/mod.rs:162`), and the parquet writer (`parquet_writer.rs:294`). It keeps a
**process-global instance cache** — `static CACHE: OnceLock<ObjectStoreCache>` keyed
by `(url_key, config_hash)` (`parquet_support.rs:550-553`) — because every plan gets a
fresh `RuntimeEnv`; stores are created once per process and re-registered into each
plan's registry (`parquet_support.rs:630`).

The wrapper slots into that existing pattern with two changes:

1. **Global cache instance.** `static DATA_CACHE: OnceLock<Option<Arc<BlockCache>>>`
   alongside the existing process globals (`TOKIO_RUNTIME`, `jni_api.rs:121`).
   Initialized on the first `createPlan` from the JNI-passed config map and
   `local_dirs` (`jni_api.rs:342, 414-420` — Spark's block-manager local dirs, the
   same ones that feed DataFusion's `DiskManagerBuilder` today). First plan wins;
   changing cache configs requires executor restart (documented; acceptable for a
   sizing knob). `None` when disabled — zero overhead, wrapper never constructed.
2. **Wrap at store creation.** At the insert into the global instance cache
   (`parquet_support.rs:621-625`), when the cache is enabled and the scheme is remote
   (not `file`), wrap: `Arc::new(CachingObjectStore::new(store, cache, namespace))`
   with `namespace = hash(url_key, config_hash)` — the same key that already
   distinguishes stores with different credentials/configs. Everything downstream
   (per-plan registry, `CachedParquetFileReaderFactory`, CSV, writer) picks up the
   wrapped store with no further changes.

### 2.8 Configuration

`CometConf` (`spark/src/main/scala/org/apache/comet/CometConf.scala`; builder pattern
`conf(...)`.category`.doc`.typeConf`.createWithDefault`), all `CATEGORY_SCAN`, dotted
lowerCamel per existing convention (`spark.comet.scan.icebergNative.enabled` at :115):

| Config | Type / default | Meaning |
|---|---|---|
| `spark.comet.scan.dataCache.enabled` | boolean, **`false`** | master switch (experimental rollout, same convention as other new features) |
| `spark.comet.scan.dataCache.memoryLimit` | `bytesConf(ByteUnit.MiB)`, 512 | memory-tier budget, process-wide per executor |
| `spark.comet.scan.dataCache.blockSize` | `bytesConf(ByteUnit.MiB)`, 4 | block quantum, power of two in [1, 16] |
| `spark.comet.scan.dataCache.ssd.limit` | `bytesConf(ByteUnit.MiB)`, 0 | SSD-tier budget; 0 disables the tier (phase 2) |
| `spark.comet.scan.dataCache.ssd.path` | string, optional | SSD directory; defaults to first Spark block-manager local dir |
| `spark.comet.scan.dataCache.locality.enabled` | boolean, `true` | declare preferred locations for scan partitions when the data cache is enabled (§2.10); no effect when `dataCache.enabled` is false |
| `spark.comet.scan.dataCache.unifiedMemory.enabled` | boolean, `false` *(phase 2)* | account the memory tier as off-heap storage memory in Spark's unified memory manager instead of `memoryOverhead` headroom; requires `spark.memory.offHeap.enabled` (§2.9) |

Plumbing (all verified mechanisms): values cross JNI in the protobuf `ConfigMap`
(`CometExecIterator.serializeCometSQLConfs`), which **only carries explicitly-set
confs** — the whole `dataCache.*` family must be force-added there like
`COMET_PARQUET_ROW_FILTER_PUSHDOWN_ENABLED` is today (`CometExecIterator.scala:274-279`).
Native side adds `pub(crate) const` keys and reads via the `SparkConfig` accessors
(`native/core/src/execution/spark_config.rs:23-53`). Docs regenerate via GenerateDocs.
Exception: `locality.enabled` is consumed **only on the JVM driver** (task scheduling
never crosses JNI), so it needs no ConfigMap entry.

### 2.9 Memory & disk accounting

**Phase-1 contract: fixed budget, outside Spark's pools.**

- **Cache memory is bounded but sits OUTSIDE Comet's task memory pools** — a
  deliberate choice, documented rather than hidden. Comet's only existing bridge to
  Spark's unified memory manager is the wrong shape for a cache: in off-heap mode
  (pool type `fair_unified`/`greedy_unified`, `CometConf.scala:679`), native
  reservations delegate via JNI to `CometTaskMemoryManager`
  (`memory_pools/unified_pool.rs:64-78`), which is built on
  `TaskContext.get().taskMemoryManager()` (`CometTaskMemoryManager.java:52`) —
  **task-scoped execution memory**. A process-lifetime cache charged there would be
  clawed back (and flagged as a leak) by Spark's task-end
  `cleanUpAllAllocatedMemory`, misattribute bytes to whichever task triggered a
  fill, and — because Comet's `NativeMemoryConsumer.spill()` returns 0
  (`CometTaskMemoryManager.java:110-113`) — be unreclaimable under pressure. Velox
  makes the same split (its cache lives in the memory allocator, not query pools).
- Operators size for it the same way they size for Comet itself: the memory-tier
  budget must come out of `spark.executor.memoryOverhead` headroom. The config doc
  says so explicitly. Accounting counts `Bytes` capacity plus fixed per-block index
  overhead; eviction triggers at budget, not above it.
- **Disk**: hard cap at `ssd.limit`, allocated in region units, never exceeded (a full
  tier evicts a region before writing). Default path under the block-manager local
  dirs means Yarn/K8s clean the files up with the executor — consistent with the
  cold-start crash model (§2.5). Disk-full or I/O errors permanently degrade the SSD
  tier to passthrough with a warning; they never fail a query.

**Designed-for now, built in phase 2: unified storage-memory integration.** The
correct Spark-side home for an executor-lifetime cache is **off-heap *storage*
memory** on the executor-wide `UnifiedMemoryManager` — the same accounting class as
cached RDD blocks — not task execution memory. The phase-1 pieces are shaped so this
drops in without API churn:

1. **JVM side — `CometCacheMemoryManager`**: an executor singleton in the
   `org.apache.spark` package (the same `private[spark]`-access pattern
   `CometTaskMemoryManager` already uses), holding `SparkEnv.get.memoryManager`. It
   reserves and releases in fixed **quanta** (64 MiB — the SSD region unit) via
   `acquireStorageMemory(cometCacheBlockId, quantum, MemoryMode.OFF_HEAP)`, where
   `cometCacheBlockId` is a synthetic `BlockId` (Spark uses it only to attribute the
   reservation and steer its own eviction bookkeeping). The manager tracks
   `granted = Σ quanta` and never lets the native budget exceed it.
2. **Native↔JVM bridge**: the same global-ref up-call pattern as
   `comet_task_memory_manager`, but process-lifetime — a handle passed once at cache
   init (first `createPlan`, alongside the existing `DATA_CACHE` init, §2.7).
   **Grant-gated growth**: a fill that would cross the current grant first up-calls
   `acquireQuantum()`; on refusal the cache evicts internally instead of growing.
   The cache therefore *cannot* exceed what Spark granted, and each growth step
   competes with executors' other storage users under Spark's normal rules.
3. **Shrink hook**: Spark has no storage-pressure callback, so shrink is driven from
   the two places Comet already observes pressure:
   - **Grant refusal** (above): stop growing, evict internally.
   - **Execution shortfall**: `CometTaskMemoryManager.acquireMemory` already
     detects `acquired < requested` (`CometTaskMemoryManager.java:64-75`). On
     shortfall it additionally calls
     `CometCacheMemoryManager.releaseQuanta(shortfall)`, which invokes native
     `BlockCache::set_memory_budget(granted − released)` (§2.1 — evicts to the new
     budget before returning), then releases the freed quanta back via
     `releaseStorageMemory`; the caller retries its acquire once before surfacing
     the reservation error to DataFusion (which then spills as today). Net effect:
     under memory pressure the cache gives way to query execution, which is
     exactly Velox's cache-vs-query arbitration.
4. **Honesty caveat (why the hook is mandatory, not optional)**: the reservation is
   not backed by real `MemoryStore` blocks, so Spark's own
   `evictBlocksToFreeSpace` path cannot reclaim it when execution borrows storage
   memory. Below the `spark.memory.storageFraction` floor that is irrelevant
   (storage is protected there anyway, same as RDD cache blocks); *above* the
   floor, the shortfall hook in (3) is what keeps the borrowed region reclaimable.
   Implementation rule: unified mode refuses to grant beyond the storage floor
   unless the shortfall hook is active.
5. **Config**: phase 2 adds `spark.comet.scan.dataCache.unifiedMemory.enabled`
   (default false). Off = phase-1 behavior (`memoryLimit` from overhead headroom);
   on (requires `spark.memory.offHeap.enabled`) = `memoryLimit` becomes a *cap* on
   quanta acquired from off-heap storage memory, and executor sizing needs no
   overhead adjustment at all.

Phase 1 ships with `set_memory_budget` in the core API and the quantum-friendly
(64 MiB-aligned) accounting, so phase 2 adds only the JVM manager, the JNI handle,
and the two hook call sites.

### 2.10 Cache-affinity scheduling: preferred locations for parquet files

Without scheduler affinity the cache is ineffective at cluster scale: each executor
caches privately, and Spark schedules S3-backed scan tasks with no locality (S3A
reports no block locations), so a re-read of a hot file lands on a random host and
hits its cache with probability ~`1/K`. The fix is driver-side: assign each parquet
file a sticky owner host and expose it through `RDD.getPreferredLocations`, so
Spark's scheduler routes repeat reads of a file to the host that already cached it.
Comet owns the scan RDD, and the hooks already exist:

- `CometExecRDD.getPreferredLocations` (`CometExecRDD.scala:146-154`) currently
  returns `Nil` for native scans — `inputRDDs` is empty on the
  `CometNativeScanExec` path, so scans get **no locality today**.
- `CometExecPartition` already carries `filePaths: Seq[String]`
  (`CometExecRDD.scala:41`), threaded from
  `CometNativeScanExec.serializedPartitionData` (`CometNativeScanExec.scala:232`,
  currently used only for error reporting). The same plumbing serves locality.

**`CometFileLocalityManager`** — a driver-side singleton in the Spark module,
modeled directly on `indextables_spark`'s `DriverSplitLocalityManager`
(`storage/DriverSplitLocalityManager.scala`), which solved the identical problem for
tantivy splits. Semantics carried over verbatim:

1. **Sticky assignment.** A driver-lifetime `ConcurrentHashMap[filePath → host]`.
   Once a file is assigned, every later query prefers the same host — that is what
   turns per-host private caches into a coherent cluster-wide cache. No broadcast,
   no executor→driver reporting: the driver *decides* placement rather than
   observing it, and the executor caches simply fill where tasks run
   (assignment-follows-scheduling, the same trick indextables uses).
2. **Per-query load balancing for new files.** At scan planning, one batch call
   (`assignFilesForQuery(paths, availableHosts)`, analog of
   `assignSplitsForQuery`, `DriverSplitLocalityManager.scala:76-136`): files whose
   assigned host is still available keep it; unassigned files (and files orphaned
   by a lost host) go to the host with the fewest files *in this query* — balancing
   the current query's work, not the historical total.
3. **Autoscaling up — fair-share rebalance.** Per `rebalanceForNewHosts`
   (`DriverSplitLocalityManager.scala:246-324`): each query compares
   `availableHosts` against the remembered host set; when new executors appear,
   compute `fairShare = ceil(filesInQuery / hosts)` and move just enough files from
   the most-overloaded hosts to bring newcomers up to fair share. New capacity gets
   work immediately; the moved files re-warm on their new owner (first read is a
   miss); untouched assignments keep their locality. Without this, a scaled-up
   cluster would never route anything to the new executors.
4. **Scale-down / executor loss.** Assignments are validated against
   `availableHosts` on every query (step 2), so files owned by a dead host are
   simply reassigned least-loaded — no failure detection beyond Spark's own.
   `availableHosts` comes from `sc.getExecutorMemoryStatus` minus the driver, with
   a localhost fallback for local mode (`getAvailableHosts`,
   `DriverSplitLocalityManager.scala:160-185`).
5. **Bounded memory.** `pruneStaleAssignments` analog: periodically drop entries
   for files no longer referenced (the map is `O(distinct files seen)`, a few
   hundred bytes each; pruning is hygiene, not correctness).

**Wiring.** In `CometNativeScanExec.doExecuteColumnar`, after
`serializedPartitionData` resolves the post-DPP file partitions: call
`assignFilesForQuery` with all file paths, then have
`CometExecRDD.getPreferredLocations` return the partition's hosts from
`partition.filePaths` when `inputRDDs` is empty and locality is enabled (lookup, not
assignment — `getPreferredLocations` is called per partition by the DAGScheduler and
must be cheap). A partition holding files assigned to different hosts returns the
hosts by descending byte count (Spark treats the result as an ordered preference
list). Same wiring applies to `CometCsvNativeScanExec` for free, since it feeds the
same RDD.

**Partition packing.** Spark packs files into `FilePartition`s by size
(`CometScanExec.getFilePartitions`, `CometScanExec.scala:252-263`), not by host, so
a multi-file partition can straddle owners. Phase 1 accepts this: large files split
by `maxSplitBytes` produce single-file partitions (every range of a file routes to
the same owner — footer and dictionary blocks cached once), and small-file
partitions still get majority-host preference. The indextables approach of grouping
by assigned host *before* batching and interleaving the result
(`IndexTables4SparkScan.scala:363-395`) is the follow-up if mixed partitions show up
hot in practice: sort `splitFiles` by assigned host before the packing call so
partitions come out host-homogeneous — Comet controls that code path too.

**Semantics and caveats.**

- Preferred locations are **hints**: if the owner host is busy past
  `spark.locality.wait`, the task runs elsewhere, reads through that host's cache,
  and is still correct — worst case is a cache miss plus duplicate cached bytes,
  which is exactly today's behavior. Locality can never fail or stall a query.
- Hints are **host-level**; the cache is **per executor process**. With multiple
  executors per host, Spark may pick any executor on the preferred host, splitting
  the working set across process caches. Acceptable for phase 1 (one-executor-per-
  host is the common large-node deployment); the SSD tier (§2.5) is the structural
  fix, since a per-host cache directory could be shared read-mostly across
  executors on a host — noted as a phase-2 consideration, not designed here.
- Assignment is by file path only (no store namespace): two stores reading the same
  path would collide in the affinity map, but they would also both benefit from
  co-locating, so this is harmless.
- Iceberg (`CometIcebergNativeScanExec`) is excluded in phase 1 for the same reason
  as §4: its reads bypass the `object_store` wrapper entirely, so routing them
  would pin tasks without any cache to hit.

### 2.11 What this phase deliberately does not do

- **No prefetch / scheduled readahead.** Velox's `CoalescedLoad` + executor scheduling
  and density tracking is item #23's readahead half — a follow-up on top of the same
  wrapper, not core scope (§4).
- **No SSD checkpointing** — cold start on restart (§2.5); Velox proves warm restart
  is possible, deferred until hit-ratio data justifies it.
- **No opendal adapter / no Iceberg coverage** in phase 1 (§4).
- **No caching of streaming `get()`**, list results, or writes.
- **No revalidation TTL** for mutable objects; immutability is the stated contract.
- **No unified-memory integration in phase 1** — fixed budget only; but the
  storage-memory reservation + shrink hook is fully specced and the core API ships
  `set_memory_budget` day one so phase 2 is additive (§2.9).
- **No changes to parsed-metadata caching** — DataFusion's per-plan
  `CachedParquetFileReaderFactory` stays as is; this design caches bytes below it.
- **No host-aware partition packing** — locality is per-partition majority
  preference (§2.10); re-sorting files by owner before `FilePartition` packing is a
  measured follow-up.
- **No executor-level affinity or cross-executor cache sharing** — hints are
  host-level; multi-executor-per-host working-set splitting is accepted (§2.10).

## 3. Alternatives considered

1. **Stand-alone cache-core crate outside the Comet tree** (a separate repo/org,
   released to crates.io, consumed as a pinned dependency; other embedders — e.g.
   tantivy4java-style search-over-immutable-splits systems — could share it).
   Rejected: an Apache project taking its cache core from a third-party repo adds a
   release-coordination step to every fix while the eviction/SSD design is still
   being tuned, and adds a supply-chain surface for no phase-1 benefit. The reuse
   story is preserved anyway — the core crate is storage-API-neutral with no Comet
   deps (§2.1), so extracting or donating it later is mechanical, not a rewrite.
2. **Contribute the cache directly into `object_store` upstream.** Right long-term
   home for the *adapter*, wrong first step: upstream review cycles would gate every
   iteration while the eviction/SSD design is still being tuned against real
   workloads, and the crate's API is still moving (0.12→0.13 changed range types to
   `u64`). Deferred, not rejected — the adapter is small, in Comet's own tree, and
   upstreamable later.
3. **Build the core on `foyer`** (the existing Rust hybrid memory+disk cache used by
   RisingWave) instead of bespoke tiers. Genuinely attractive; the concerns are
   dependency weight and control over the region/admission layout for scan
   workloads. Worth a **time-boxed spike before phase 2**: the core API (§2.1) is
   the contract, so foyer could replace the tier internals inside
   `native/block-cache` without touching the wrapper.
4. **Cache inside DataFusion's `RuntimeEnv`/`cache_manager`.** Rejected on verified
   lifecycle grounds: Comet creates a fresh `RuntimeEnv` per plan (`jni_api.rs:572`),
   so nothing registry- or manager-scoped survives a single plan.
5. **Whole-file caching** (download-on-first-touch). Rejected: selective column reads
   touch a fraction of each file; block granularity is what both Velox and
   Databricks converged on.
6. **External caching layers** (mountpoint-S3 cache, Alluxio, OS page cache over an
   NFS gateway). Operationally heavyweight, no ETag coupling to the engine, and
   invisible to Comet metrics. Out of scope.
7. **Locality via consistent/rendezvous hashing** instead of the sticky map (§2.10).
   Stateless and scale-friendly on paper (host add moves ~`1/K` of keys), but it
   cannot load-balance the *current query* — a query's file subset can hash
   lopsidedly onto few hosts with no recourse — and any membership flap silently
   reassigns files. indextables tried the adjacent design (broadcast-based executor
   reporting, `BroadcastSplitLocalityManager`) and replaced it with the driver-side
   sticky map precisely for zero per-query overhead and explicit rebalance control;
   this design adopts the surviving approach.
8. **Executor-reported cache contents** (executors tell the driver what they hold;
   driver routes to observed copies). Truthful but heavy: a reporting channel,
   staleness windows, and races with eviction. Declaring placement driver-side and
   letting caches fill where tasks run achieves the same steady state with no new
   channel.

## 4. Phasing & Iceberg/Delta applicability

Per the gap assessment's Delta-first plan:

- **Phase 1 — core + adapter + wiring + locality, memory tier only.** The
  `native/block-cache` and `native/object-store-cache` workspace crates; Comet
  wiring per §2.7–2.8, default off; `CometFileLocalityManager` + the
  `CometExecRDD.getPreferredLocations` hookup (§2.10). Locality ships in phase 1
  because without it the cache doesn't pay off beyond a handful of executors — it is
  the difference between a per-host cache and a cluster cache. Covers the plain
  Parquet path end to end.
- **Phase 2 — SSD tier** (§2.5), preceded by the foyer spike (§3.3), plus
  **unified storage-memory integration** (`CometCacheMemoryManager`, grant-gated
  growth, execution-shortfall shrink hook — specced in §2.9). SSD is where the
  Databricks-parity win lives for working sets bigger than executor memory.
- **Phase 3 — item #23 polish**: readahead (sequential-pattern next-block prefetch on
  the wrapper's own hits, Velox-style density gating) and footer warmup. Footer
  *bytes* are already covered by phase 1 (§2.6). Locality follow-ups land here too if
  the data warrants: host-aware partition packing and per-host SSD sharing across
  executors (§2.10).
- **Phase 4**: an opendal layer adapter over the same core (in the Comet wrapper
  crate or beside it) to cover Comet's Iceberg path.

**Delta: inherited.** delta-kernel-rs's default engine does its IO through
`object_store`, so a `CachingObjectStore` registered where the contrib builds its
engine covers data-file reads (verify point: `contrib/delta/native/src/engine.rs` —
not in the local checkout, cited as described by the gap assessment).

**Iceberg: not inherited.** Verified locally: Comet pins `iceberg` +
`iceberg-storage-opendal` (git rev `80a30d3`, `native/Cargo.toml:61-62`) — iceberg-rust's
FileIO is opendal-based, so the `object_store` wrapper never sees those reads. Options,
in preference order: (a) the phase-4 opendal layer adapter (opendal's layering
mechanism → same core behind a second thin adapter); (b) track iceberg-rust's
object_store-backed FileIO work upstream and re-test. The core being storage-API
neutral (§2.1) is exactly what keeps this a bounded follow-up instead of a rewrite.

Follow-up items (tracked separately): SSD checkpointing for warm restart;
Spark-metrics-visible hit/miss counters (phase 1 logs periodic stats natively);
upstreaming the adapter to `object_store` or a DataFusion contrib home; revalidation
TTL if a mutable-object use case materializes.

## 5. Test plan

- **Core crate unit tests** (`native/block-cache`, run by Comet CI; include a
  dependency-tree check that the crate stays free of `object_store`/`opendal`):
  block math (unaligned starts/ends, short final block,
  cross-block ranges); SIEVE retention/eviction sequences; single-flight — N
  concurrent readers, exactly one fetch, error propagation without error caching;
  version-mismatch invalidation; budget enforcement under concurrent fill;
  `set_memory_budget` reduction under concurrent readers — evicts to the new target,
  never drops pinned/in-flight blocks, growth re-admits (phase-1 test even though
  phase 1 never calls it: the API is the phase-2 contract). SSD
  (phase 2): write/read round-trip, crc32c corruption → treated as miss, region
  eviction order, disk-full degradation, cold-start unlink. Unified memory
  (phase 2): grant refusal gates growth (upstream fetch still served, block not
  admitted); execution-shortfall hook releases quanta and the retried acquire
  succeeds; grants capped at the storage floor when the hook is disabled.
- **Adapter tests** (in-tree, `native/object-store-cache`, run by Comet CI on every
  dependency bump): `CachingObjectStore` over `object_store::memory::InMemory`
  behind a request-counting shim — second identical `get_ranges` issues zero upstream
  requests; byte-exact equality against the unwrapped store for randomized range sets;
  `get_opts` condition/suffix passthrough; `put` invalidation.
- **Locality unit tests** (Scala, `CometFileLocalityManager`, mirroring indextables'
  semantics): sticky reassignment across calls; least-loaded assignment counted
  per-query; lost host → files reassigned, survivors untouched; new host →
  fair-share rebalance moves only the excess; prune drops only unreferenced paths.
  RDD level: `CometExecRDD.getPreferredLocations` returns assigned hosts for a
  file-backed partition, `Nil` when locality is disabled, and ordered-by-bytes hosts
  for a mixed partition.
- **Comet Rust integration** (`parquet_exec` tests): scan the same file set twice with
  the cache enabled — identical batches, second scan's upstream fetch count is zero;
  encrypted-Parquet and CSV paths unaffected; disabled-config path constructs no
  wrapper.
- **Scala suite** (config enabled): result parity vs. Spark across the standard scan
  suites on local FS and (existing MinIO/S3 test infra) `s3a://`; re-run same query
  twice and assert native hit-ratio log shows nonzero hits; overwrite-file case
  (rewrite a Parquet file in place) still returns fresh data via ETag invalidation.
- **Benchmark**: TPC-H subset against MinIO, cold vs. warm cache, published in the PR;
  hit ratio and bytes-saved become the phase-2/phase-3 go/no-go inputs. Run once
  single-executor and once multi-executor (standalone cluster) with locality on/off —
  the multi-executor delta is the number that justifies §2.10.

## 6. Risks

| Risk | Mitigation |
|---|---|
| Cache memory invisible to Spark accounting → executor OOM | phase 1: fixed budget outside task pools by design, documented `memoryOverhead` sizing, small default (512 MiB), default-off config; phase 2 removes the class entirely via off-heap storage-memory reservation + shrink hook (§2.9) |
| Unified mode (phase 2): unreclaimable "phantom" storage reservation starves execution | grant-gated growth in quanta; execution-shortfall hook shrinks the cache and retries; grants above the `storageFraction` floor refused unless the hook is active (§2.9.4) |
| Stale reads if objects are overwritten in place | ETag capture + miss-time invalidation (§2.4); immutability contract documented; per-store disable via existing config-hash namespacing |
| Single-flight / concurrency bugs under task parallelism | dedicated stress tests in core-crate CI; errors never cached; wrapper bypass (passthrough) preserved for all non-bounded-range ops |
| `object_store` trait evolution across upgrades | adapter is the only crate touching the trait and lives in the Comet workspace — every DF/object_store bump compiles it in the same PR; no cross-repo coordination |
| Locality hints concentrate load (hot file → one host) | hints not constraints — `spark.locality.wait` bounds the wait, then the task runs elsewhere (= today's behavior); per-query least-loaded assignment spreads distinct files; fair-share rebalance on scale-up (§2.10) |
| Preferred host busy/gone → perceived slowdown or cold reads | scheduler fallback is automatic and correctness-neutral (cache miss only); lost hosts reassigned on next query; `locality.enabled` kill switch |
| Multiple executors per host split the per-process cache | accepted phase 1 (hints are host-level, matches one-executor-per-host deployments); per-host SSD sharing is the phase-2/3 structural fix (§2.10) |
| SSD wear / write amplification | admission filter (≥2 accesses), batched sequential region writes, wholesale region eviction |
| Disk full or bad `ssd.path` on executors | hard cap in region units; I/O errors degrade tier to passthrough, never fail queries; default path = Spark-managed local dirs |
| First-plan-wins global init surprises on config change | documented (executor restart required); mirrors existing process-global store-instance cache behavior |
