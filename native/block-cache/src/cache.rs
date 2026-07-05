// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeSet, HashMap};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::future::FutureExt;
use futures::stream::StreamExt;
use tokio::sync::Semaphore;

use crate::error::{CacheError, Result};
use crate::ledger::{PrefetchIntent, PrefetchLedger, PrefetchStats, ReleaseOutcome};
use crate::metrics::{Metrics, MetricsSnapshot};
use crate::sieve::{Block, BlockKey, Evicted, FetchResult, InFlightFut, Shard};
use crate::ssd::{SsdCache, SsdConfig, DEFAULT_REGION_SIZE};
use crate::version::{FileKey, FileVersion};

/// Process-wide cap on concurrent prefetch fetches (SCAN_PREFETCH_DESIGN.md §2.4). Sits below
/// typical object-store client connection-pool sizes so that many concurrent tasks' prefetchers
/// cannot pile requests onto the store ahead of demand reads. A deliberate constant, not a config.
const PREFETCH_GLOBAL_CONCURRENCY: usize = 16;

/// The process-wide prefetch fetch semaphore.
fn prefetch_semaphore() -> &'static Semaphore {
    static SEM: Semaphore = Semaphore::const_new(PREFETCH_GLOBAL_CONCURRENCY);
    &SEM
}

/// Clears any still-owned prefetch in-flight entries if `prefetch_run`'s future is dropped
/// mid-fetch (cooperative cancellation, SCAN_PREFETCH_DESIGN.md §2.2). Without this, a
/// cancelled prefetcher — common for `LIMIT` queries that never read past the cutoff — would
/// leak in-flight entries into the shard maps forever, since eviction only touches the block
/// map and no demand read ever arrives to clear them. On normal completion the guard is
/// disarmed (each block's entry is already removed by publish/error). Any demand read that
/// joined a cleared entry as a waiter recovers via waiter-retry-once.
struct PrefetchClaimGuard<'a> {
    cache: &'a BlockCache,
    file_id: u64,
    owned: Vec<u32>,
    armed: bool,
}

impl Drop for PrefetchClaimGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        for &b in &self.owned {
            let key = (self.file_id, b);
            self.cache.shard(key).lock().unwrap().in_flight_remove(&key);
        }
    }
}

/// Minimum / maximum / default block size (the read quantum). Powers of two only.
pub const MIN_BLOCK_SIZE: u64 = 1 << 20; // 1 MiB
pub const MAX_BLOCK_SIZE: u64 = 16 << 20; // 16 MiB
pub const DEFAULT_BLOCK_SIZE: u64 = 4 << 20; // 4 MiB
/// Default number of memory-tier shards.
pub const DEFAULT_NUM_SHARDS: usize = 16;
/// Default cap on a single coalesced upstream fetch (4 default blocks).
pub const DEFAULT_MAX_COALESCE_BYTES: u64 = 16 << 20; // 16 MiB

/// Configuration for a [`BlockCache`].
#[derive(Clone, Debug)]
pub struct BlockCacheConfig {
    /// Block quantum in bytes. Clamped to a power of two in `[MIN_BLOCK_SIZE, MAX_BLOCK_SIZE]`.
    pub block_size: u64,
    /// Memory-tier budget in bytes, process-wide.
    pub memory_budget: u64,
    /// Number of memory-tier shards.
    pub num_shards: usize,
    /// Cap on bytes fetched in a single coalesced upstream request.
    pub max_coalesce_bytes: u64,
    /// Directory for the SSD tier's region files. `None` (or `ssd_limit == 0`) disables the
    /// tier — the cache runs memory-only.
    pub ssd_dir: Option<PathBuf>,
    /// SSD-tier budget in bytes; `0` disables the tier.
    pub ssd_limit: u64,
    /// SSD region size in bytes (blocks never span a region).
    pub ssd_region_size: u64,
}

impl Default for BlockCacheConfig {
    fn default() -> Self {
        BlockCacheConfig {
            block_size: DEFAULT_BLOCK_SIZE,
            memory_budget: 512 << 20,
            num_shards: DEFAULT_NUM_SHARDS,
            max_coalesce_bytes: DEFAULT_MAX_COALESCE_BYTES,
            ssd_dir: None,
            ssd_limit: 0,
            ssd_region_size: DEFAULT_REGION_SIZE,
        }
    }
}

/// Round `v` down to the largest power of two `<= v`.
fn floor_pow2(v: u64) -> u64 {
    if v == 0 {
        return 0;
    }
    1u64 << (63 - v.leading_zeros() as u64)
}

impl BlockCacheConfig {
    /// Normalize into a valid config: block size becomes a power of two within bounds,
    /// shard count is at least 1, and the coalesce cap is at least one block.
    fn normalized(mut self) -> Self {
        let clamped = self.block_size.clamp(MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        self.block_size = floor_pow2(clamped).max(MIN_BLOCK_SIZE);
        self.num_shards = self.num_shards.max(1);
        self.max_coalesce_bytes = self.max_coalesce_bytes.max(self.block_size);
        self
    }
}

/// Fetches absolute byte ranges from the underlying storage on a cache miss.
///
/// The cache calls this exactly once per block per version regardless of how many tasks
/// concurrently miss it (single-flight). Implementations return the bytes for the
/// requested ranges plus the object version observed by the fetch, which the cache uses
/// to detect in-place overwrites.
#[async_trait]
pub trait RangeFetcher: Send + Sync {
    async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)>;
}

/// Interned file identities and their captured versions.
struct FileTable {
    ids: HashMap<FileKey, u64>,
    versions: HashMap<u64, FileVersion>,
    next_id: u64,
}

/// The decision made after comparing a fetched version against the stored one.
enum VersionDecision {
    Unchanged,
    FirstSeen,
    Overwritten,
}

/// A block-aligned local data cache (memory tier) sitting behind a caller-supplied
/// [`RangeFetcher`]. Storage-API-neutral: it knows nothing about `object_store`.
pub struct BlockCache {
    block_size: u64,
    num_shards: usize,
    max_coalesce_blocks: u32,
    shards: Vec<Mutex<Shard>>,
    files: Mutex<FileTable>,
    memory_budget: AtomicU64,
    metrics: Arc<Metrics>,
    /// SSD tier (phase 2), or `None` for memory-only.
    ssd: Option<SsdCache>,
}

impl BlockCache {
    /// Build a cache from `config` (normalized to valid values).
    pub fn new(config: BlockCacheConfig) -> Arc<Self> {
        let config = config.normalized();
        let per_shard_budget = config.memory_budget / config.num_shards as u64;
        let shards = (0..config.num_shards)
            .map(|_| Mutex::new(Shard::new(per_shard_budget)))
            .collect();
        let max_coalesce_blocks = (config.max_coalesce_bytes / config.block_size).max(1) as u32;
        let metrics = Arc::new(Metrics::default());
        let ssd = match (&config.ssd_dir, config.ssd_limit) {
            (Some(dir), limit) if limit > 0 => SsdCache::open(
                SsdConfig {
                    dir: dir.clone(),
                    total_limit: limit,
                    num_shards: config.num_shards,
                    region_size: config.ssd_region_size.max(config.block_size),
                },
                Arc::clone(&metrics),
            ),
            _ => None,
        };
        Arc::new(BlockCache {
            block_size: config.block_size,
            num_shards: config.num_shards,
            max_coalesce_blocks,
            shards,
            files: Mutex::new(FileTable {
                ids: HashMap::new(),
                versions: HashMap::new(),
                next_id: 0,
            }),
            memory_budget: AtomicU64::new(config.memory_budget),
            metrics,
            ssd,
        })
    }

    /// The block quantum in bytes.
    pub fn block_size(&self) -> u64 {
        self.block_size
    }

    /// A snapshot of the cache counters.
    pub fn stats(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Total in-flight single-flight entries across all shards (test-only invariant check).
    #[cfg(test)]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.lock().unwrap().in_flight_len())
            .sum()
    }

    /// Record that a file's data prefetch was skipped (the core `ScanPrefetcher` calls this on
    /// e.g. a footer parse failure — §2.6). Surfaces in [`MetricsSnapshot::prefetch_files_skipped`].
    pub fn note_prefetch_file_skipped(&self) {
        self.metrics.record_prefetch_file_skipped();
    }

    /// Flush any pending SSD-tier writes to disk synchronously. No-op when the SSD tier is
    /// disabled. Background flushes handle this automatically in production; this is a
    /// clean-shutdown / test hook.
    pub fn flush_ssd(&self) {
        if let Some(ssd) = &self.ssd {
            ssd.flush();
        }
    }

    /// Drop all cached blocks and captured versions from every tier, forcing subsequent reads
    /// to re-fetch from the underlying store. Used for maintenance and cold-cache benchmarking.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.lock().unwrap().clear();
        }
        if let Some(ssd) = &self.ssd {
            ssd.clear();
        }
        let mut files = self.files.lock().unwrap();
        files.ids.clear();
        files.versions.clear();
    }

    /// Serve `ranges` of `file`. Reads are quantized to blocks internally; misses go
    /// through `fetcher` exactly once per block regardless of concurrent callers. Returns
    /// one `Bytes` per input range, byte-for-byte identical to reading the store directly.
    pub async fn get_ranges(
        &self,
        file: &FileKey,
        ranges: &[Range<u64>],
        fetcher: &dyn RangeFetcher,
    ) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let file_id = self.intern(file);

        // Union of blocks touched by any requested range.
        let mut needed: BTreeSet<u32> = BTreeSet::new();
        for r in ranges {
            if r.start >= r.end {
                continue;
            }
            let first = (r.start / self.block_size) as u32;
            let last = ((r.end - 1) / self.block_size) as u32;
            for b in first..=last {
                needed.insert(b);
            }
        }

        // Probe the memory tier.
        let mut have: HashMap<u32, Bytes> = HashMap::with_capacity(needed.len());
        let mut missing: Vec<u32> = Vec::new();
        for &b in &needed {
            let key = (file_id, b);
            let hit = self.shard(key).lock().unwrap().get(&key);
            match hit {
                Some(block) => {
                    self.metrics.record_hit();
                    // A resident-block read is a repeat access (or the first demand
                    // consumption of a prefetched block).
                    self.note_demand_hit(&block, true);
                    have.insert(b, block.data.clone());
                }
                None => {
                    self.metrics.record_miss();
                    missing.push(b);
                }
            }
        }

        // Second tier: serve memory misses from SSD before hitting the network. An SSD hit
        // is promoted back into the memory tier.
        if !missing.is_empty() {
            if let Some(ssd) = &self.ssd {
                let mut still_missing = Vec::with_capacity(missing.len());
                for b in std::mem::take(&mut missing) {
                    match ssd.get((file_id, b)).await {
                        Some(bytes) => {
                            self.promote_from_ssd((file_id, b), bytes.clone());
                            have.insert(b, bytes);
                        }
                        None => still_missing.push(b),
                    }
                }
                missing = still_missing;
            }
        }

        if !missing.is_empty() {
            self.fill_missing(file_id, &missing, fetcher, &mut have)
                .await?;
        }

        // Assemble each requested range from the blocks now in `have`.
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            out.push(self.assemble_range(r, &have)?);
        }
        Ok(out)
    }

    /// Ensure the blocks covering `ranges` of `file` are cached, fetching misses through
    /// `fetcher` under prefetch accounting (SCAN_PREFETCH_DESIGN.md §2.1, §2.4). Unlike
    /// [`get_ranges`](Self::get_ranges) this neither assembles nor returns range bytes:
    /// already-cached blocks cost one shard probe, blocks a concurrent demand read already
    /// owns are left to that read (single-flight composition — never a duplicate GET), and
    /// only genuine misses are fetched. Newly inserted blocks carry the prefetch tag and the
    /// caller's `ledger` credit until their first demand hit or eviction.
    ///
    /// Correctness never depends on this method: every path it writes is the same block/fetch
    /// path demand reads use, and any error is counted and abandoned. Fetches are paced by
    /// `ledger` (ahead budget) and bounded to `max_concurrent` per call plus a process-wide
    /// semaphore.
    pub async fn prefetch_ranges(
        &self,
        file: &FileKey,
        ranges: &[Range<u64>],
        fetcher: &dyn RangeFetcher,
        intent: PrefetchIntent,
        ledger: &Arc<PrefetchLedger>,
        max_concurrent: usize,
    ) -> Result<PrefetchStats> {
        let mut stats = PrefetchStats::default();
        if ranges.is_empty() {
            return Ok(stats);
        }
        let file_id = self.intern(file);

        // Union of blocks touched by any requested range.
        let mut needed: BTreeSet<u32> = BTreeSet::new();
        for r in ranges {
            if r.start >= r.end {
                continue;
            }
            let first = (r.start / self.block_size) as u32;
            let last = ((r.end - 1) / self.block_size) as u32;
            for b in first..=last {
                needed.insert(b);
            }
        }

        // Memory probe. A prefetch probe is not a demand hit, so it does not touch the
        // reference bits or resolve any prefetch tag.
        let mut missing: Vec<u32> = Vec::new();
        for &b in &needed {
            let key = (file_id, b);
            if self.shard(key).lock().unwrap().contains(&key) {
                stats.blocks_already_cached += 1;
            } else {
                missing.push(b);
            }
        }

        // Second tier: promote SSD-resident blocks back into memory (warming the memory tier
        // is the point) rather than re-fetching them from the network. Probe concurrently —
        // these are independent local-disk reads and a projected file can touch many blocks.
        if !missing.is_empty() {
            if let Some(ssd) = &self.ssd {
                let probes = std::mem::take(&mut missing)
                    .into_iter()
                    .map(|b| async move { (b, ssd.get((file_id, b)).await) });
                let results: Vec<(u32, Option<Bytes>)> = futures::stream::iter(probes)
                    .buffer_unordered(max_concurrent.max(1))
                    .collect()
                    .await;
                let mut still_missing = Vec::with_capacity(results.len());
                for (b, hit) in results {
                    match hit {
                        Some(bytes) => {
                            self.promote_from_ssd((file_id, b), bytes);
                            stats.blocks_already_cached += 1;
                        }
                        None => still_missing.push(b),
                    }
                }
                missing = still_missing;
            }
        }
        if missing.is_empty() {
            return Ok(stats);
        }

        missing.sort_unstable();
        let runs = coalesce_runs(&missing, self.max_coalesce_blocks);
        let max_concurrent = max_concurrent.max(1);
        let run_stats: Vec<PrefetchStats> = futures::stream::iter(runs)
            .map(|run| self.prefetch_run(file_id, run, fetcher, intent, ledger))
            .buffer_unordered(max_concurrent)
            .collect()
            .await;
        for r in run_stats {
            stats.blocks_fetched += r.blocks_fetched;
            stats.bytes_fetched += r.bytes_fetched;
            stats.fetch_requests += r.fetch_requests;
            stats.blocks_already_cached += r.blocks_already_cached;
            stats.blocks_in_flight += r.blocks_in_flight;
            stats.errors += r.errors;
        }
        Ok(stats)
    }

    /// Prefetch one coalesced run: claim the blocks nobody else is fetching, then fetch and
    /// publish them (tagged) in sub-runs, paced by the ledger and the global semaphore.
    async fn prefetch_run(
        &self,
        file_id: u64,
        run: Vec<u32>,
        fetcher: &dyn RangeFetcher,
        intent: PrefetchIntent,
        ledger: &Arc<PrefetchLedger>,
    ) -> PrefetchStats {
        let mut stats = PrefetchStats::default();
        if run.is_empty() {
            return stats;
        }

        // Reserve the ahead-budget for the whole run BEFORE claiming any in-flight entries.
        // Charging first means a task parked here (window full) holds no un-fetched claims, so
        // a demand read for one of these blocks can never end up awaiting a prefetcher that is
        // itself blocked on the budget — the liveness invariant. Credit for blocks we don't end
        // up owning is refunded below; per-block credit for owned blocks rides along on the
        // block (`Block::charged`) and is released on consume/eviction.
        let run_charge = run.len() as u64 * self.block_size;
        ledger.charge(run_charge).await;

        // Claim ownership of blocks that are still neither cached nor already in flight.
        let mut owned: Vec<u32> = Vec::new();
        let mut senders: HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>> = HashMap::new();
        for &b in &run {
            let key = (file_id, b);
            let mut shard = self.shard(key).lock().unwrap();
            if shard.contains(&key) {
                stats.blocks_already_cached += 1;
                continue;
            }
            if shard.in_flight_get(&key).is_some() {
                // A demand read (or another prefetch) already owns this block; joining as a
                // waiter would only tie prefetch to that fetch, so leave it — a no-op probe.
                stats.blocks_in_flight += 1;
                continue;
            }
            let (tx, rx) = tokio::sync::oneshot::channel::<FetchResult>();
            let fut: InFlightFut = async move {
                match rx.await {
                    Ok(res) => res,
                    Err(_) => Err(CacheError::Internal(
                        "prefetch owner dropped before delivering block".to_string(),
                    )),
                }
            }
            .boxed()
            .shared();
            shard.in_flight_insert(key, fut);
            drop(shard);
            owned.push(b);
            senders.insert(b, tx);
        }

        // Refund the credit charged for blocks that turned out cached / in flight (a demand read
        // or another prefetch claimed them while we were charging) — neither consumed nor wasted.
        let refund = (run.len() - owned.len()) as u64 * self.block_size;
        if refund > 0 {
            ledger.refund(refund);
        }
        if owned.is_empty() {
            return stats;
        }

        // Guard the claimed in-flight entries: if this future is dropped mid-fetch (cancellation),
        // its Drop clears them so they don't leak. Disarmed on normal completion below (by then
        // each entry is already removed by publish/error).
        let mut claim_guard = PrefetchClaimGuard {
            cache: self,
            file_id,
            owned: owned.clone(),
            armed: true,
        };

        // Skipped blocks may have split the run, so re-coalesce the owned set into fetch segments.
        owned.sort_unstable();
        for seg in coalesce_runs(&owned, self.max_coalesce_blocks) {
            let start_block = seg[0];
            let end_block = *seg.last().unwrap();
            let abs_start = start_block as u64 * self.block_size;
            let abs_end = (end_block as u64 + 1) * self.block_size;

            // A global permit throttles concurrent upstream prefetch fetches (never held while
            // parked on the budget above).
            let permit = prefetch_semaphore().acquire().await;
            let fetch_range = abs_start..abs_end;
            let fetched = fetcher.fetch(std::slice::from_ref(&fetch_range)).await;
            drop(permit);

            match fetched {
                Ok((bytes_vec, version)) => {
                    let full = concat_bytes(bytes_vec);
                    self.metrics.record_prefetch_fetch(full.len() as u64);
                    stats.fetch_requests += 1;
                    stats.bytes_fetched += full.len() as u64;
                    self.reconcile_version(file_id, &version);
                    for &b in &seg {
                        let off = ((b - start_block) as u64 * self.block_size) as usize;
                        let data = if off >= full.len() {
                            Bytes::new()
                        } else {
                            let end = (off + self.block_size as usize).min(full.len());
                            full.slice(off..end)
                        };
                        let block = match intent {
                            // Each owned block carries `block_size` of the credit charged for the
                            // run up front; it is released on the block's consume/eviction.
                            PrefetchIntent::Prefetch => {
                                Block::new_prefetch(data, Arc::clone(ledger), self.block_size)
                            }
                        };
                        self.publish_prefetched(file_id, b, block, &mut senders);
                        stats.blocks_fetched += 1;
                    }
                }
                Err(_) => {
                    // Release this segment's charged credit (wasted), fail the owned blocks (so a
                    // waiter retries — §2.6), and count the error. Errors are never cached.
                    ledger.release(seg.len() as u64 * self.block_size, ReleaseOutcome::Wasted);
                    for &b in &seg {
                        let key = (file_id, b);
                        if let Some(tx) = senders.remove(&b) {
                            let _ = tx.send(Err(CacheError::Fetch("prefetch fetch failed".into())));
                        }
                        self.shard(key).lock().unwrap().in_flight_remove(&key);
                    }
                    self.metrics.record_prefetch_error();
                    stats.errors += 1;
                }
            }
        }
        // Completed normally: every owned entry is already removed (published or failed), so the
        // guard has nothing to clean up.
        claim_guard.armed = false;
        stats
    }

    /// Insert a prefetched (tagged) block into the memory tier, wake any demand waiter joined
    /// via single-flight, and clear the in-flight entry. Unlike [`publish_block`] there is no
    /// `have` map — prefetch assembles nothing.
    ///
    /// [`publish_block`]: Self::publish_block
    fn publish_prefetched(
        &self,
        file_id: u64,
        block_index: u32,
        block: Arc<Block>,
        senders: &mut HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>>,
    ) {
        let key = (file_id, block_index);
        let evicted = {
            let mut shard = self.shard(key).lock().unwrap();
            let evicted = shard.insert(key, Arc::clone(&block), &self.metrics);
            shard.in_flight_remove(&key);
            evicted
        };
        self.admit_evicted(evicted);
        if let Some(tx) = senders.remove(&block_index) {
            // A demand read that joined this block's fetch as a waiter receives it here and
            // consumes it via `note_demand_hit` on its own side; a closed receiver just means
            // no demand read was waiting.
            let _ = tx.send(Ok(block));
        }
    }

    /// Fetch and cache every missing block, deduplicating concurrent fetches (single-flight)
    /// and coalescing runs of adjacent missing blocks into one upstream request.
    ///
    /// Runs at most two passes (SCAN_PREFETCH_DESIGN.md §2.6, "waiter retry-once"): the first
    /// claims/fetches/waits normally; if any block is still missing afterward — because the
    /// task that owned its in-flight fetch errored or was dropped (a cancelled prefetcher is
    /// now a common case) — the second pass reclaims those blocks as owner and fetches them
    /// itself before surfacing an error. This is a strict robustness improvement for the
    /// pre-existing demand/demand race too.
    async fn fill_missing(
        &self,
        file_id: u64,
        missing: &[u32],
        fetcher: &dyn RangeFetcher,
        have: &mut HashMap<u32, Bytes>,
    ) -> Result<()> {
        let mut all = missing.to_vec();
        all.sort_unstable();
        all.dedup();

        let mut to_process = all.clone();
        let mut saved_error: Option<CacheError> = None;
        for attempt in 0..2 {
            let reclaim = attempt > 0;
            let (first_error, waiter_failed) = self
                .fill_pass(file_id, &to_process, fetcher, have, reclaim)
                .await;
            if saved_error.is_none() {
                saved_error = first_error;
            }
            if all.iter().all(|b| have.contains_key(b)) {
                return Ok(());
            }
            // Only re-drive blocks whose *owner* failed/was dropped (waiter failures); an
            // owner's own fetch error propagates as before and a fresh `get_ranges` retries it.
            if reclaim || waiter_failed.is_empty() {
                break;
            }
            to_process = waiter_failed;
        }

        // Final re-probe before surfacing an error. Our own fetch of a block may have failed
        // while a *concurrent* reader (which we may have collided with in the reclaim pass, or
        // which simply raced us) fetched and cached it in the meantime. Returning an error when
        // the block is actually resident would fail the query needlessly — so consult the cache
        // one last time and only error for blocks that are genuinely still missing.
        for &b in &all {
            if have.contains_key(&b) {
                continue;
            }
            let key = (file_id, b);
            let hit = self.shard(key).lock().unwrap().get(&key);
            if let Some(block) = hit {
                self.note_demand_hit(&block, true);
                have.insert(b, block.data.clone());
            }
        }
        if all.iter().all(|b| have.contains_key(b)) {
            return Ok(());
        }
        Err(saved_error
            .unwrap_or_else(|| CacheError::Internal("block missing after fill".to_string())))
    }

    /// One claim/fetch/wait pass over `blocks`. Returns the first error encountered and the
    /// blocks whose in-flight owner failed or was dropped (candidates for the retry pass).
    /// When `reclaim` is set, stale in-flight entries (left by a dead owner) are dropped and
    /// re-owned rather than awaited.
    async fn fill_pass(
        &self,
        file_id: u64,
        blocks: &[u32],
        fetcher: &dyn RangeFetcher,
        have: &mut HashMap<u32, Bytes>,
        reclaim: bool,
    ) -> (Option<CacheError>, Vec<u32>) {
        // --- claim phase: decide which blocks we own vs. wait on ---
        let mut owned: Vec<u32> = Vec::new();
        let mut senders: HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>> = HashMap::new();
        let mut waiters: Vec<(u32, InFlightFut)> = Vec::new();

        for &b in blocks {
            let key = (file_id, b);
            let mut shard = self.shard(key).lock().unwrap();
            // Re-check: a concurrent task may have filled the block since our probe.
            if let Some(block) = shard.get(&key) {
                drop(shard);
                self.note_demand_hit(&block, true);
                have.insert(b, block.data.clone());
                continue;
            }
            if reclaim {
                // We only reclaim blocks whose pass-1 owner failed or was dropped. An
                // errored owner already removed its own entry (fetch-phase `Err` branch), so a
                // *dropped* (cancelled) owner is the case that leaves a stale entry here; drop
                // it and become the new owner below. In the rare window where a fresh owner
                // claimed between the failure and here, this re-owns and both fetch — a
                // redundant GET, never a correctness problem (the duplicate insert is a no-op
                // and every waiter is still served). Left as-is deliberately: distinguishing a
                // stale entry from a live one would require a wait-then-claim protocol that can
                // fail to recover from the stale case.
                shard.in_flight_remove(&key);
            } else if let Some(fut) = shard.in_flight_get(&key) {
                waiters.push((b, fut));
                continue;
            }
            // Claim: install a shared future others can await, and own the fetch.
            let (tx, rx) = tokio::sync::oneshot::channel::<FetchResult>();
            let fut: InFlightFut = async move {
                match rx.await {
                    Ok(res) => res,
                    Err(_) => Err(CacheError::Internal(
                        "fetch owner dropped before delivering block".to_string(),
                    )),
                }
            }
            .boxed()
            .shared();
            shard.in_flight_insert(key, fut);
            drop(shard);
            owned.push(b);
            senders.insert(b, tx);
        }

        // --- fetch phase: our owned blocks, coalesced. Must happen BEFORE awaiting other
        // owners' futures so that two callers cross-owning each other's blocks cannot
        // deadlock (each fetches what it owns first, then waits). ---
        let mut first_error: Option<CacheError> = None;
        for run in coalesce_runs(&owned, self.max_coalesce_blocks) {
            let start_block = run[0];
            let end_block = *run.last().unwrap();
            let abs_start = start_block as u64 * self.block_size;
            // Over-read past EOF is fine: the store truncates a partially-out-of-bounds
            // range, yielding the short final block.
            let abs_end = (end_block as u64 + 1) * self.block_size;

            let fetch_range = abs_start..abs_end;
            match fetcher.fetch(std::slice::from_ref(&fetch_range)).await {
                Ok((bytes_vec, version)) => {
                    let full = concat_bytes(bytes_vec);
                    self.metrics.record_fetch(full.len() as u64);
                    self.reconcile_version(file_id, &version);
                    for &b in &run {
                        let off = ((b - start_block) as u64 * self.block_size) as usize;
                        let data = if off >= full.len() {
                            Bytes::new()
                        } else {
                            let end = (off + self.block_size as usize).min(full.len());
                            full.slice(off..end)
                        };
                        let block = Block::new(data);
                        self.publish_block(file_id, b, block, have, &mut senders);
                    }
                }
                Err(e) => {
                    // Fail every owned block in this run; remove the in-flight entries so
                    // the next caller retries. Errors are never cached.
                    for &b in &run {
                        let key = (file_id, b);
                        if let Some(tx) = senders.remove(&b) {
                            let _ = tx.send(Err(e.clone()));
                        }
                        self.shard(key).lock().unwrap().in_flight_remove(&key);
                    }
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        // --- wait phase: blocks other tasks own ---
        let mut waiter_failed: Vec<u32> = Vec::new();
        for (b, fut) in waiters {
            match fut.await {
                Ok(block) => {
                    // First demand delivery of a single-flight fill: like the owner, this is
                    // not a repeat access, so it does not set `ever_visited`. A prefetch-tagged
                    // block delivered here is consumed by this demand read (§2.5).
                    self.note_demand_hit(&block, false);
                    have.insert(b, block.data.clone());
                }
                Err(e) => {
                    // The owner errored or was dropped (e.g. a cancelled prefetcher). This is no
                    // longer evidence our own fetch would fail — flag it for the retry pass.
                    waiter_failed.push(b);
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        (first_error, waiter_failed)
    }

    /// Record a demand read observing `block`. The first demand hit on a prefetch-tagged block
    /// is its consumption (§2.5): whichever of consume/evict CASes the tag first accounts for
    /// it, so credit is released exactly once. Consumption sets `visited` (SIEVE keeps it) but
    /// deliberately never sets `ever_visited` — a block read once by a scan is single-pass
    /// traffic and must stay out of the SSD admission gate. An ordinary hit sets `visited`, and
    /// `ever_visited` only when `repeat_access` (a hit on an already-resident block, versus the
    /// first-touch delivery of a freshly filled block).
    fn note_demand_hit(&self, block: &Arc<Block>, repeat_access: bool) {
        // Keep the block under SIEVE regardless of path (redundant with `Shard::get` on the
        // probe/claim paths, load-bearing on the wait path where the block came from the
        // in-flight future, not the map).
        block.visited.store(true, Ordering::Relaxed);
        if block.take_prefetch_tag() {
            if let Some(ledger) = &block.ledger {
                ledger.release(block.charged, ReleaseOutcome::Consumed);
            }
            self.metrics.record_prefetch_consumed();
            // Deliberately not `ever_visited`: a block a scan reads once is single-pass
            // traffic and must stay out of the SSD admission gate (§2.5).
        } else if repeat_access {
            block.ever_visited.store(true, Ordering::Relaxed);
        }
    }

    /// Insert a fetched block into the memory tier, hand it to `have`, wake any waiters,
    /// and clear the in-flight entry.
    fn publish_block(
        &self,
        file_id: u64,
        block_index: u32,
        block: Arc<Block>,
        have: &mut HashMap<u32, Bytes>,
        senders: &mut HashMap<u32, tokio::sync::oneshot::Sender<FetchResult>>,
    ) {
        let key = (file_id, block_index);
        let evicted = {
            let mut shard = self.shard(key).lock().unwrap();
            let evicted = shard.insert(key, Arc::clone(&block), &self.metrics);
            shard.in_flight_remove(&key);
            evicted
        };
        self.admit_evicted(evicted);
        have.insert(block_index, block.data.clone());
        if let Some(tx) = senders.remove(&block_index) {
            // A closed receiver just means no other task waited; ignore.
            let _ = tx.send(Ok(block));
        }
    }

    /// Promote a block read from the SSD tier back into the memory tier, marking it accessed
    /// so SIEVE keeps it.
    fn promote_from_ssd(&self, key: BlockKey, bytes: Bytes) {
        let block = Block::new(bytes);
        block.visited.store(true, Ordering::Relaxed);
        block.ever_visited.store(true, Ordering::Relaxed);
        let evicted = self
            .shard(key)
            .lock()
            .unwrap()
            .insert(key, block, &self.metrics);
        self.admit_evicted(evicted);
    }

    /// Handle blocks evicted from the memory tier: release prefetch credit for any evicted
    /// unconsumed prefetch block (counted as waste, §2.4), and admit SSD-eligible blocks.
    ///
    /// The SSD admission gate is unchanged — only blocks hit at least once (`ever_visited`)
    /// are written, filtering single-pass scan traffic. An unconsumed prefetch block has
    /// `ever_visited == false`, so enabling prefetch never turns a one-pass scan into SSD
    /// writes (§2.5). Runs regardless of whether the SSD tier is present, because the ledger
    /// release must happen in the memory-only case too.
    fn admit_evicted(&self, evicted: Vec<Evicted>) {
        for ev in evicted {
            // Whoever CASes the tag first (this eviction or a racing demand hit) accounts for
            // the block; losing the race means a demand read already consumed it.
            if ev.block.take_prefetch_tag() {
                if let Some(ledger) = &ev.block.ledger {
                    ledger.release(ev.block.charged, ReleaseOutcome::Wasted);
                }
                self.metrics.record_prefetch_wasted();
            }
            if let Some(ssd) = &self.ssd {
                if ev.block.ever_visited.load(Ordering::Relaxed) {
                    ssd.admit(ev.key, ev.block.data.clone());
                }
            }
        }
    }

    /// Build one requested range's bytes from the (now cached) blocks in `have`.
    fn assemble_range(&self, r: &Range<u64>, have: &HashMap<u32, Bytes>) -> Result<Bytes> {
        if r.start >= r.end {
            return Ok(Bytes::new());
        }
        let bs = self.block_size;
        let first = (r.start / bs) as u32;
        let last = ((r.end - 1) / bs) as u32;

        if first == last {
            // Single block: zero-copy slice.
            let block = have
                .get(&first)
                .ok_or_else(|| CacheError::Internal("block missing during assembly".to_string()))?;
            let block_start = first as u64 * bs;
            let lo = (r.start - block_start) as usize;
            let hi = ((r.end - block_start).min(block.len() as u64)) as usize;
            if lo > block.len() {
                return Err(CacheError::Internal(
                    "range start beyond block during assembly".to_string(),
                ));
            }
            return Ok(block.slice(lo..hi));
        }

        let mut buf = BytesMut::with_capacity((r.end - r.start) as usize);
        for b in first..=last {
            let block = have
                .get(&b)
                .ok_or_else(|| CacheError::Internal("block missing during assembly".to_string()))?;
            let block_start = b as u64 * bs;
            let lo = r.start.max(block_start);
            let hi = r.end.min(block_start + block.len() as u64);
            if hi <= lo {
                continue;
            }
            buf.extend_from_slice(&block[(lo - block_start) as usize..(hi - block_start) as usize]);
        }
        Ok(buf.freeze())
    }

    /// Compare a freshly fetched version with the stored one; on the first overwrite
    /// detection, drop every cached block of the file and record the new version.
    fn reconcile_version(&self, file_id: u64, version: &FileVersion) {
        let decision = {
            let mut files = self.files.lock().unwrap();
            match files.versions.get(&file_id) {
                None => {
                    files.versions.insert(file_id, version.clone());
                    VersionDecision::FirstSeen
                }
                Some(existing) if existing.matches(version) => VersionDecision::Unchanged,
                Some(_) => {
                    files.versions.insert(file_id, version.clone());
                    VersionDecision::Overwritten
                }
            }
        };
        if let VersionDecision::Overwritten = decision {
            self.invalidate_blocks(file_id);
            self.metrics.record_invalidation();
        }
    }

    /// Drop every cached block of `file_id` from all shards.
    fn invalidate_blocks(&self, file_id: u64) {
        for shard in &self.shards {
            shard.lock().unwrap().remove_file_blocks(file_id);
        }
    }

    /// Drop all cached state for a file (used on `put`/`delete` at the wrapper).
    pub fn invalidate_file(&self, file: &FileKey) {
        let file_id = {
            let files = self.files.lock().unwrap();
            files.ids.get(file).copied()
        };
        if let Some(id) = file_id {
            self.invalidate_blocks(id);
            self.files.lock().unwrap().versions.remove(&id);
        }
    }

    /// Change the memory-tier budget at runtime. A reduction evicts (SIEVE order) until
    /// under the new per-shard budget before returning. Phase-1 callers never invoke this;
    /// it is the phase-2 unified-memory contract (grant/shrink) and is exercised by tests.
    pub fn set_memory_budget(&self, bytes: u64) {
        self.memory_budget.store(bytes, Ordering::Relaxed);
        let per_shard = bytes / self.num_shards as u64;
        for shard in &self.shards {
            let evicted = shard.lock().unwrap().set_budget(per_shard, &self.metrics);
            self.admit_evicted(evicted);
        }
    }

    /// The current memory-tier budget in bytes.
    pub fn memory_budget(&self) -> u64 {
        self.memory_budget.load(Ordering::Relaxed)
    }

    /// The version most recently captured for `file`, if the cache has ever fetched it.
    ///
    /// Used by the `object_store` wrapper to synthesize an `ObjectMeta` for a cache-served
    /// `get_opts` without issuing a `head` request. Returns `None` before the first fetch.
    pub fn file_version(&self, file: &FileKey) -> Option<FileVersion> {
        let files = self.files.lock().unwrap();
        let id = files.ids.get(file)?;
        files.versions.get(id).cloned()
    }

    /// Intern a file key into a compact id, assigning a new one on first sight.
    fn intern(&self, file: &FileKey) -> u64 {
        let mut files = self.files.lock().unwrap();
        if let Some(id) = files.ids.get(file) {
            return *id;
        }
        let id = files.next_id;
        files.next_id += 1;
        files.ids.insert(file.clone(), id);
        id
    }

    /// The shard owning a block key.
    fn shard(&self, key: BlockKey) -> &Mutex<Shard> {
        &self.shards[self.shard_index(key)]
    }

    fn shard_index(&self, (file_id, block_index): BlockKey) -> usize {
        // Cheap integer mix over the 12-byte key.
        let mut h = file_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= (block_index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        h ^= h >> 29;
        (h % self.num_shards as u64) as usize
    }
}

/// Concatenate the pieces returned by a fetch into one `Bytes` (zero-copy for the common
/// single-range fetch this cache issues).
fn concat_bytes(mut pieces: Vec<Bytes>) -> Bytes {
    match pieces.len() {
        0 => Bytes::new(),
        1 => pieces.pop().unwrap(),
        _ => {
            let total: usize = pieces.iter().map(|p| p.len()).sum();
            let mut buf = BytesMut::with_capacity(total);
            for p in pieces {
                buf.extend_from_slice(&p);
            }
            buf.freeze()
        }
    }
}

/// Group sorted, deduped block indices into runs of consecutive integers, splitting any
/// run longer than `max_blocks` so no single upstream fetch exceeds the coalesce cap.
fn coalesce_runs(sorted: &[u32], max_blocks: u32) -> Vec<Vec<u32>> {
    let mut runs: Vec<Vec<u32>> = Vec::new();
    let mut cur: Vec<u32> = Vec::new();
    for &b in sorted {
        let extends = match cur.last() {
            Some(&last) => b == last + 1 && (cur.len() as u32) < max_blocks,
            None => false,
        };
        if !extends && !cur.is_empty() {
            runs.push(std::mem::take(&mut cur));
        }
        cur.push(b);
    }
    if !cur.is_empty() {
        runs.push(cur);
    }
    runs
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn floor_pow2_rounds_down() {
        assert_eq!(floor_pow2(1), 1);
        assert_eq!(floor_pow2(3), 2);
        assert_eq!(floor_pow2(5 << 20), 4 << 20);
        assert_eq!(floor_pow2(16 << 20), 16 << 20);
    }

    #[test]
    fn config_normalizes_block_size_to_pow2_in_range() {
        let c = BlockCacheConfig {
            block_size: 5 << 20,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, 4 << 20);

        let c = BlockCacheConfig {
            block_size: 100 << 20,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, MAX_BLOCK_SIZE);

        let c = BlockCacheConfig {
            block_size: 0,
            ..Default::default()
        }
        .normalized();
        assert_eq!(c.block_size, MIN_BLOCK_SIZE);
    }

    #[test]
    fn coalesce_runs_splits_gaps_and_caps() {
        // adjacent run [1,2,3] and isolated [5]
        assert_eq!(
            coalesce_runs(&[1, 2, 3, 5], 8),
            vec![vec![1, 2, 3], vec![5]]
        );
        // cap at 2 blocks per run
        assert_eq!(
            coalesce_runs(&[1, 2, 3, 4], 2),
            vec![vec![1, 2], vec![3, 4]]
        );
        assert!(coalesce_runs(&[], 4).is_empty());
    }
}

#[cfg(test)]
// Single-element range slices (`&[0..N]`) are intentional here — they exercise the exact
// shape a scan issues.
#[allow(clippy::single_range_in_vec_init)]
mod prefetch_tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Notify;

    const MIB: u64 = 1 << 20;

    fn byte_at(off: u64) -> u8 {
        (off % 251) as u8
    }

    /// Deterministic sequential fetcher: byte `i` is `i % 251`. Counts fetch calls.
    struct SeqFetcher {
        size: u64,
        fetches: AtomicU64,
    }

    impl SeqFetcher {
        fn new(size: u64) -> Self {
            SeqFetcher {
                size,
                fetches: AtomicU64::new(0),
            }
        }
        fn fetch_count(&self) -> u64 {
            self.fetches.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RangeFetcher for SeqFetcher {
        async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)> {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            let mut out = Vec::with_capacity(ranges.len());
            for r in ranges {
                let end = r.end.min(self.size);
                let mut v = Vec::new();
                let mut off = r.start;
                while off < end {
                    v.push(byte_at(off));
                    off += 1;
                }
                out.push(Bytes::from(v));
            }
            Ok((
                out,
                FileVersion {
                    size: self.size,
                    ..Default::default()
                },
            ))
        }
    }

    fn mem_cache(block_size: u64, budget: u64, shards: usize) -> Arc<BlockCache> {
        BlockCache::new(BlockCacheConfig {
            block_size,
            memory_budget: budget,
            num_shards: shards,
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn prefetch_then_demand_consumes_and_stays_single_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = BlockCache::new(BlockCacheConfig {
            block_size: MIB,
            memory_budget: 64 * MIB,
            num_shards: 4,
            ssd_dir: Some(tmp.path().to_path_buf()),
            ssd_limit: 64 * MIB,
            ..Default::default()
        });
        let file = FileKey::new(1, "a.parquet");
        let fetcher = SeqFetcher::new(4 * MIB);
        let ledger = Arc::new(PrefetchLedger::new(32 * MIB));

        let stats = cache
            .prefetch_ranges(
                &file,
                &[0..2 * MIB],
                &fetcher,
                PrefetchIntent::Prefetch,
                &ledger,
                3,
            )
            .await
            .unwrap();
        assert_eq!(stats.blocks_fetched, 2);
        assert_eq!(
            ledger.outstanding(),
            2 * MIB,
            "unconsumed prefetched bytes held"
        );
        assert_eq!(
            fetcher.fetch_count(),
            1,
            "two adjacent blocks coalesce to one GET"
        );

        // Demand read consumes the prefetched blocks — zero additional upstream fetches.
        let out = cache
            .get_ranges(&file, &[0..2 * MIB], &fetcher)
            .await
            .unwrap();
        assert_eq!(fetcher.fetch_count(), 1, "demand read is all hits");
        assert_eq!(out[0].len(), (2 * MIB) as usize);
        for i in 0..2 * MIB {
            assert_eq!(out[0][i as usize], byte_at(i));
        }

        assert_eq!(ledger.outstanding(), 0, "consumption returns all credit");
        assert_eq!(ledger.consumed_bytes(), 2 * MIB);
        let s = cache.stats();
        assert_eq!(s.prefetch_blocks_consumed, 2);
        assert_eq!(s.prefetch_blocks_wasted, 0);

        // Single-pass: consumption set `visited` but not `ever_visited`, so evicting the whole
        // tier admits nothing to SSD (§2.5 — prefetch must not turn a one-pass scan into wear).
        cache.set_memory_budget(0);
        cache.flush_ssd();
        assert_eq!(
            cache.stats().ssd_writes,
            0,
            "prefetch + single scan must not be SSD-admitted"
        );
    }

    #[tokio::test]
    async fn unconsumed_prefetch_evicted_counts_waste() {
        // One shard, budget holds ~2 blocks; prefetch 4 contiguous → 2 evicted unconsumed.
        let cache = mem_cache(MIB, 2 * (MIB + 64), 1);
        let file = FileKey::new(1, "a.parquet");
        let fetcher = SeqFetcher::new(8 * MIB);
        let ledger = Arc::new(PrefetchLedger::new(64 * MIB)); // large so pacing never blocks

        let stats = cache
            .prefetch_ranges(
                &file,
                &[0..4 * MIB],
                &fetcher,
                PrefetchIntent::Prefetch,
                &ledger,
                3,
            )
            .await
            .unwrap();
        assert_eq!(stats.blocks_fetched, 4);

        let s = cache.stats();
        assert_eq!(
            s.prefetch_blocks_wasted, 2,
            "two blocks evicted before any hit"
        );
        assert_eq!(ledger.wasted_bytes(), 2 * MIB);
        assert_eq!(
            ledger.outstanding(),
            2 * MIB,
            "credit for the two still-resident unconsumed blocks stays held"
        );
    }

    #[tokio::test]
    async fn prefetch_probe_is_noop_when_already_cached() {
        let cache = mem_cache(MIB, 64 * MIB, 4);
        let file = FileKey::new(1, "a.parquet");
        let fetcher = SeqFetcher::new(4 * MIB);

        // Warm via a demand read first.
        cache
            .get_ranges(&file, &[0..2 * MIB], &fetcher)
            .await
            .unwrap();
        let after_demand = fetcher.fetch_count();

        let ledger = Arc::new(PrefetchLedger::new(32 * MIB));
        let stats = cache
            .prefetch_ranges(
                &file,
                &[0..2 * MIB],
                &fetcher,
                PrefetchIntent::Prefetch,
                &ledger,
                3,
            )
            .await
            .unwrap();
        assert_eq!(stats.blocks_already_cached, 2);
        assert_eq!(stats.blocks_fetched, 0);
        assert_eq!(fetcher.fetch_count(), after_demand, "no upstream fetch");
        assert_eq!(ledger.outstanding(), 0, "nothing charged for a pure probe");
    }

    /// A fetcher whose first call parks on a gate then errors; later calls succeed. Lets a test
    /// force one task to own a failing/hanging fetch while another waits on it.
    struct GateFetcher {
        size: u64,
        calls: AtomicU64,
        started: Notify,
        release: Notify,
    }

    impl GateFetcher {
        fn new(size: u64) -> Self {
            GateFetcher {
                size,
                calls: AtomicU64::new(0),
                started: Notify::new(),
                release: Notify::new(),
            }
        }
    }

    #[async_trait]
    impl RangeFetcher for GateFetcher {
        async fn fetch(&self, ranges: &[Range<u64>]) -> Result<(Vec<Bytes>, FileVersion)> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                self.started.notify_waiters();
                self.release.notified().await;
                return Err(CacheError::Fetch("first attempt fails".into()));
            }
            let mut out = Vec::with_capacity(ranges.len());
            for r in ranges {
                let end = r.end.min(self.size);
                let len = end.saturating_sub(r.start) as usize;
                out.push(Bytes::from(vec![7u8; len]));
            }
            Ok((
                out,
                FileVersion {
                    size: self.size,
                    ..Default::default()
                },
            ))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiter_retries_once_on_owner_error() {
        let cache = Arc::new(mem_cache(MIB, 64 * MIB, 4));
        let file = FileKey::new(1, "a.parquet");
        let fetcher = Arc::new(GateFetcher::new(MIB));

        // Task A becomes the owner; its fetch parks on the gate, then errors.
        let (ca, fa, filea) = (Arc::clone(&cache), Arc::clone(&fetcher), file.clone());
        let started = fetcher.started.notified();
        let a = tokio::spawn(async move { ca.get_ranges(&filea, &[0..MIB], &*fa).await });
        started.await; // A now owns the in-flight fetch

        // Task B misses the same block and joins as a waiter.
        let (cb, fb, fileb) = (Arc::clone(&cache), Arc::clone(&fetcher), file.clone());
        let b = tokio::spawn(async move { cb.get_ranges(&fileb, &[0..MIB], &*fb).await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        // Release A: it errors, so B's waited-on future errors — B must retry once as owner.
        fetcher.release.notify_waiters();
        let ra = a.await.unwrap();
        let rb = b.await.unwrap();

        assert!(
            ra.is_err(),
            "owner's own fetch error propagates (no in-call retry)"
        );
        let out = rb.expect("waiter retried and fetched the block itself");
        assert_eq!(out[0].len(), MIB as usize);
        assert!(
            fetcher.calls.load(Ordering::SeqCst) >= 2,
            "B issued a second fetch"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_prefetch_does_not_leak_in_flight_entries() {
        let cache = Arc::new(mem_cache(MIB, 64 * MIB, 4));
        let file = FileKey::new(1, "a.parquet");
        let fetcher = Arc::new(GateFetcher::new(MIB));
        let ledger = Arc::new(PrefetchLedger::new(32 * MIB));

        // Prefetch parks in the (gated) fetch after claiming the block's in-flight entry.
        let (c, f, fk) = (Arc::clone(&cache), Arc::clone(&fetcher), file.clone());
        let started = fetcher.started.notified();
        let task = tokio::spawn(async move {
            c.prefetch_ranges(&fk, &[0..MIB], &*f, PrefetchIntent::Prefetch, &ledger, 3)
                .await
        });
        started.await;
        assert_eq!(cache.in_flight_len(), 1, "prefetch claimed the block");

        // Cancel mid-fetch (drop the future). The claim guard must clear the in-flight entry.
        task.abort();
        let _ = task.await;
        // Give the drop a moment to run on the worker.
        for _ in 0..100 {
            if cache.in_flight_len() == 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        assert_eq!(
            cache.in_flight_len(),
            0,
            "cancelled prefetch must not leak in-flight entries"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waiter_retries_once_on_owner_drop() {
        let cache = Arc::new(mem_cache(MIB, 64 * MIB, 4));
        let file = FileKey::new(1, "a.parquet");
        let fetcher = Arc::new(GateFetcher::new(MIB));

        // Task A owns the fetch and parks forever; we abort it mid-fetch (the cancelled-prefetch
        // case). Its in-flight entry is left stale — the reclaim pass must clear and re-own it.
        let (ca, fa, filea) = (Arc::clone(&cache), Arc::clone(&fetcher), file.clone());
        let started = fetcher.started.notified();
        let a = tokio::spawn(async move { ca.get_ranges(&filea, &[0..MIB], &*fa).await });
        started.await;

        let (cb, fb, fileb) = (Arc::clone(&cache), Arc::clone(&fetcher), file.clone());
        let b = tokio::spawn(async move { cb.get_ranges(&fileb, &[0..MIB], &*fb).await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        a.abort(); // owner dropped mid-fetch
        let out = b
            .await
            .unwrap()
            .expect("waiter recovers from a dropped owner");
        assert_eq!(out[0].len(), MIB as usize);
    }
}
