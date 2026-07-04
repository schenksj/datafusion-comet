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

//! SSD tier (OBJECT_STORE_CACHE_DESIGN.md sections 2.3 / 2.5): one append-oriented file per
//! shard, laid out as fixed-size regions. Blocks never span a region. An in-memory index
//! maps a block key to `(region, offset, len, crc32c)`; the index lives only in memory, so
//! the tier cold-starts empty on restart (stale files are unlinked at construction).
//!
//! - **Admission**: only blocks that were hit at least once in the memory tier (accessed
//!   >= 2 times) are admitted, filtering single-pass scan traffic that would only wear the
//!   device.
//! - **Writes**: batched and performed off the read path — admission only enqueues; a
//!   background flush drains the queue and does the disk I/O without holding the shard lock.
//! - **Eviction**: wholesale per region, by a decayed bytes-read score. Evicting whole
//!   regions keeps writes sequential and avoids free-list fragmentation.
//! - **Checksums**: crc32c verified on every read; a mismatch drops the entry and reports a
//!   miss (corruption is never an error).
//!
//! Unix only (positioned `pread`/`pwrite` via `FileExt`); on other platforms the tier is not
//! constructed (`open` returns `None`) and the cache runs memory-only.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use log::warn;

use crate::metrics::Metrics;
use crate::sieve::BlockKey;

/// Default region size (bytes). Blocks (<= 16 MiB) never span a region.
pub(crate) const DEFAULT_REGION_SIZE: u64 = 64 << 20; // 64 MiB

/// Flush a shard's write queue once it holds at least this many bytes.
const FLUSH_THRESHOLD_BYTES: u64 = 8 << 20; // 8 MiB

/// Multiplicative decay applied to every region's read score on each new-region allocation
/// (the tier's coarse clock), so recently-read regions outrank stale-popular ones.
const SCORE_DECAY: f64 = 0.98;

// Positioned I/O. Unix uses `pwrite`/`pread` via `FileExt`; other platforms never reach here
// because `open` returns `None`, but stubs keep the crate compiling everywhere.
#[cfg(unix)]
fn pwrite(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}
#[cfg(unix)]
fn pread(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}
#[cfg(not(unix))]
fn pwrite(_file: &File, _buf: &[u8], _offset: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SSD tier requires a unix platform",
    ))
}
#[cfg(not(unix))]
fn pread(_file: &File, _buf: &mut [u8], _offset: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SSD tier requires a unix platform",
    ))
}

/// Configuration for the SSD tier.
#[derive(Clone, Debug)]
pub(crate) struct SsdConfig {
    pub dir: PathBuf,
    pub total_limit: u64,
    pub num_shards: usize,
    pub region_size: u64,
}

/// Location of a block within a shard file.
#[derive(Clone, Copy, Debug)]
struct SsdEntry {
    region: usize,
    offset: u64,
    len: u32,
    crc: u32,
}

/// A fixed-size region of a shard file.
struct Region {
    /// Next free byte offset within the region.
    write_offset: u64,
    /// Decayed bytes-read score; higher = hotter.
    score: f64,
    /// Keys currently stored in this region (erased wholesale on eviction).
    keys: Vec<BlockKey>,
}

impl Region {
    fn empty() -> Self {
        Region {
            write_offset: 0,
            score: 0.0,
            keys: Vec::new(),
        }
    }
}

/// One SSD shard: a file plus its region metadata, index, and pending-write queue.
struct SsdShard {
    file: Arc<File>,
    region_size: u64,
    max_regions: usize,
    regions: Vec<Region>,
    index: HashMap<BlockKey, SsdEntry>,
    /// Region currently being appended to.
    active: Option<usize>,
    /// Admitted-but-unwritten blocks.
    pending: Vec<(BlockKey, Bytes)>,
    pending_bytes: u64,
    /// A background flush is scheduled/running for this shard.
    flushing: bool,
}

impl SsdShard {
    /// Ensure there is an active region with room for `len` bytes, allocating or evicting a
    /// region if necessary. Returns the active region index.
    fn ensure_region(&mut self, len: u64, metrics: &Metrics) -> usize {
        if let Some(r) = self.active {
            if self.regions[r].write_offset + len <= self.region_size {
                return r;
            }
        }
        // Need a fresh active region. Decay all scores first (coarse clock tick).
        for region in &mut self.regions {
            region.score *= SCORE_DECAY;
        }
        if self.regions.len() < self.max_regions {
            self.regions.push(Region::empty());
            let idx = self.regions.len() - 1;
            self.active = Some(idx);
            return idx;
        }
        // At capacity: evict the lowest-scored region other than the active one.
        let victim = self.pick_victim();
        self.erase_region(victim);
        metrics.record_ssd_region_eviction();
        self.active = Some(victim);
        victim
    }

    /// Index of the lowest-scored evictable (non-active) region.
    fn pick_victim(&self) -> usize {
        let mut best = usize::MAX;
        let mut best_score = f64::INFINITY;
        for (i, region) in self.regions.iter().enumerate() {
            if Some(i) == self.active {
                continue;
            }
            if region.score < best_score {
                best_score = region.score;
                best = i;
            }
        }
        // There is always at least one non-active region when we are at capacity with
        // max_regions >= 1; fall back to region 0 defensively.
        if best == usize::MAX {
            0
        } else {
            best
        }
    }

    /// Erase a region wholesale: drop its index entries and reset it for reuse.
    fn erase_region(&mut self, idx: usize) {
        let keys = std::mem::take(&mut self.regions[idx].keys);
        for key in keys {
            self.index.remove(&key);
        }
        self.regions[idx].write_offset = 0;
        self.regions[idx].score = 0.0;
    }
}

/// The SSD tier: sharded region files behind the memory tier.
pub(crate) struct SsdCache {
    shards: Vec<Arc<Mutex<SsdShard>>>,
    num_shards: usize,
    region_size: u64,
    metrics: Arc<Metrics>,
}

impl SsdCache {
    /// Open (cold-start) the SSD tier. Returns `Ok(None)` if the budget is too small for even
    /// one region, or on any I/O error (the cache then runs memory-only — the SSD tier never
    /// fails a query).
    pub(crate) fn open(config: SsdConfig, metrics: Arc<Metrics>) -> Option<SsdCache> {
        match Self::try_open(config, metrics) {
            Ok(cache) => cache,
            Err(e) => {
                warn!("Comet data cache: SSD tier disabled (init failed): {e}");
                None
            }
        }
    }

    fn try_open(config: SsdConfig, metrics: Arc<Metrics>) -> io::Result<Option<SsdCache>> {
        if !cfg!(unix) {
            return Ok(None);
        }
        let num_shards = config.num_shards.max(1);
        if config.total_limit < config.region_size {
            return Ok(None);
        }
        std::fs::create_dir_all(&config.dir)?;
        // Cold start: remove any stale shard files left by a previous process.
        remove_stale_files(&config.dir);

        let total_regions = config.total_limit / config.region_size;
        let per_shard = (total_regions / num_shards as u64).max(1) as usize;

        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            let path = config.dir.join(format!("comet-data-cache-{i}.bin"));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            shards.push(Arc::new(Mutex::new(SsdShard {
                file: Arc::new(file),
                region_size: config.region_size,
                max_regions: per_shard,
                regions: Vec::new(),
                index: HashMap::new(),
                active: None,
                pending: Vec::new(),
                pending_bytes: 0,
                flushing: false,
            })));
        }
        Ok(Some(SsdCache {
            shards,
            num_shards,
            region_size: config.region_size,
            metrics,
        }))
    }

    fn shard_index(&self, (file_id, block_index): BlockKey) -> usize {
        let mut h = file_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= (block_index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        h ^= h >> 29;
        (h % self.num_shards as u64) as usize
    }

    /// Admit a block for SSD storage (called on memory-tier eviction of a visited block).
    /// Only enqueues — the disk write happens off the read path. A block already resident or
    /// too large for a region is skipped.
    pub(crate) fn admit(&self, key: BlockKey, data: Bytes) {
        if data.len() as u64 > self.region_size {
            return;
        }
        let shard = Arc::clone(&self.shards[self.shard_index(key)]);
        let spawn = {
            let mut g = shard.lock().unwrap();
            if g.index.contains_key(&key) || g.pending.iter().any(|(k, _)| *k == key) {
                return;
            }
            g.pending_bytes += data.len() as u64;
            g.pending.push((key, data));
            if !g.flushing && g.pending_bytes >= FLUSH_THRESHOLD_BYTES {
                g.flushing = true;
                true
            } else {
                false
            }
        };
        if spawn {
            let shard = Arc::clone(&shard);
            let metrics = Arc::clone(&self.metrics);
            // Off the read path: the disk write runs on a blocking thread.
            let _ = tokio::task::spawn_blocking(move || flush_shard(&shard, &metrics));
        }
    }

    /// Serve a block from the SSD tier, verifying its checksum. Returns `None` on a miss or a
    /// crc mismatch (which also drops the corrupt entry).
    pub(crate) async fn get(&self, key: BlockKey) -> Option<Bytes> {
        let shard = Arc::clone(&self.shards[self.shard_index(key)]);
        let metrics = Arc::clone(&self.metrics);
        tokio::task::spawn_blocking(move || read_block(&shard, key, &metrics))
            .await
            .ok()
            .flatten()
    }

    /// Force any pending writes to disk synchronously (a clean-shutdown / test hook).
    /// Production admits trigger background flushes automatically.
    pub(crate) fn flush(&self) {
        for shard in &self.shards {
            flush_shard(shard, &self.metrics);
        }
    }

    /// Drop the in-memory index so nothing is served from disk (the region files keep their
    /// bytes but become unreachable and are overwritten as regions are reused).
    pub(crate) fn clear(&self) {
        for shard in &self.shards {
            let mut g = shard.lock().unwrap();
            g.index.clear();
            g.regions.clear();
            g.active = None;
            g.pending.clear();
            g.pending_bytes = 0;
        }
    }
}

/// Read + verify a single block. Runs on a blocking thread.
fn read_block(shard: &Arc<Mutex<SsdShard>>, key: BlockKey, metrics: &Metrics) -> Option<Bytes> {
    let (file, entry) = {
        let g = shard.lock().unwrap();
        let entry = *g.index.get(&key)?;
        (Arc::clone(&g.file), entry)
    };
    let region_base = entry.region as u64 * shard_region_size(shard);
    let mut buf = vec![0u8; entry.len as usize];
    if let Err(e) = pread(&file, &mut buf, region_base + entry.offset) {
        warn!("Comet data cache: SSD read failed, treating as miss: {e}");
        return None;
    }
    if crc32c::crc32c(&buf) != entry.crc {
        // Corruption (or the region was reused under us): drop the entry, report a miss.
        let mut g = shard.lock().unwrap();
        g.index.remove(&key);
        metrics.record_ssd_corruption();
        return None;
    }
    // Bump the region's read score (if the entry is still valid).
    {
        let mut g = shard.lock().unwrap();
        if let Some(cur) = g.index.get(&key).copied() {
            if cur.region == entry.region {
                g.regions[entry.region].score += entry.len as f64;
            }
        }
    }
    metrics.record_ssd_hit();
    Some(Bytes::from(buf))
}

fn shard_region_size(shard: &Arc<Mutex<SsdShard>>) -> u64 {
    shard.lock().unwrap().region_size
}

/// Drain a shard's write queue to disk. Runs on a blocking thread. Reserves region space
/// under the lock, writes without the lock (so admits are never blocked on disk I/O), then
/// records the index entries under the lock.
fn flush_shard(shard: &Arc<Mutex<SsdShard>>, metrics: &Metrics) {
    loop {
        // Phase 1 (locked): take the queue, reserve region space, compute write plan.
        struct Plan {
            key: BlockKey,
            region: usize,
            offset: u64,
            data: Bytes,
        }
        let (file, region_size, plans) = {
            let mut g = shard.lock().unwrap();
            if g.pending.is_empty() {
                g.flushing = false;
                return;
            }
            let batch = std::mem::take(&mut g.pending);
            g.pending_bytes = 0;
            let region_size = g.region_size;
            let mut plans = Vec::with_capacity(batch.len());
            for (key, data) in batch {
                let len = data.len() as u64;
                let region = g.ensure_region(len, metrics);
                let offset = g.regions[region].write_offset;
                g.regions[region].write_offset += len;
                g.regions[region].keys.push(key);
                plans.push(Plan {
                    key,
                    region,
                    offset,
                    data,
                });
            }
            (Arc::clone(&g.file), region_size, plans)
        };

        // Phase 2 (unlocked): write bytes and compute checksums.
        let mut written = Vec::with_capacity(plans.len());
        for p in plans {
            let crc = crc32c::crc32c(&p.data);
            let pos = p.region as u64 * region_size + p.offset;
            match pwrite(&file, &p.data, pos) {
                Ok(()) => written.push((p.key, p.region, p.offset, p.data.len() as u32, crc)),
                Err(e) => {
                    // Disk-full / I/O error: degrade to passthrough for this batch. The keys
                    // were reserved but not indexed, so they simply won't be served from SSD.
                    warn!("Comet data cache: SSD write failed, skipping block: {e}");
                }
            }
        }

        // Phase 3 (locked): publish index entries.
        {
            let mut g = shard.lock().unwrap();
            for (key, region, offset, len, crc) in written {
                g.index.insert(
                    key,
                    SsdEntry {
                        region,
                        offset,
                        len,
                        crc,
                    },
                );
                metrics.record_ssd_write();
            }
            // If more work arrived while we wrote, loop; otherwise clear the flag.
            if g.pending.is_empty() {
                g.flushing = false;
                return;
            }
        }
    }
}

/// Remove any of our shard files left behind by a previous process (cold start).
fn remove_stale_files(dir: &std::path::Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("comet-data-cache-") && name.ends_with(".bin") {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::fs::FileExt;

    fn open(dir: &std::path::Path, limit: u64, region: u64, shards: usize) -> (SsdCache, Arc<Metrics>) {
        let metrics = Arc::new(Metrics::default());
        let ssd = SsdCache::open(
            SsdConfig {
                dir: dir.to_path_buf(),
                total_limit: limit,
                num_shards: shards,
                region_size: region,
            },
            Arc::clone(&metrics),
        )
        .expect("ssd tier should open");
        (ssd, metrics)
    }

    #[tokio::test]
    async fn write_read_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let (ssd, metrics) = open(tmp.path(), 4 * 4096, 4096, 2);
        let key = (1u64, 0u32);
        let data = Bytes::from((0..1500u32).map(|i| i as u8).collect::<Vec<_>>());
        ssd.admit(key, data.clone());
        ssd.flush();
        assert_eq!(ssd.get(key).await, Some(data));
        let m = metrics.snapshot();
        assert_eq!(m.ssd_writes, 1);
        assert_eq!(m.ssd_hits, 1);
        // A key never admitted is a miss.
        assert_eq!(ssd.get((2, 0)).await, None);
    }

    #[tokio::test]
    async fn crc_mismatch_is_treated_as_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let (ssd, metrics) = open(tmp.path(), 4 * 4096, 4096, 1);
        let key = (9u64, 3u32);
        ssd.admit(key, Bytes::from(vec![0xABu8; 1000]));
        ssd.flush();
        assert!(ssd.get(key).await.is_some());

        // Corrupt the shard file on disk.
        let path = tmp.path().join("comet-data-cache-0.bin");
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        f.write_all_at(&[0u8; 1000], 0).unwrap();
        f.flush().unwrap();

        assert_eq!(ssd.get(key).await, None, "corrupt block must be a miss");
        assert!(metrics.snapshot().ssd_corruptions >= 1);
        // The corrupt entry was dropped, so it stays a miss.
        assert_eq!(ssd.get(key).await, None);
    }

    #[tokio::test]
    async fn region_eviction_reclaims_lowest_scored_region() {
        // 1 shard; region holds 2 blocks of 1500 bytes; cap at 3 regions (6 blocks).
        let tmp = tempfile::tempdir().unwrap();
        let (ssd, metrics) = open(tmp.path(), 3 * 4096, 4096, 1);
        let mk = |n: u8| Bytes::from(vec![n; 1500]);

        // region 0 -> {0,1}, region 1 -> {2,3}, region 2 -> {4,5}.
        for i in 0..6u32 {
            ssd.admit((0, i), mk(i as u8));
        }
        ssd.flush();
        // Warm region 0 and region 2; leave region 1 (blocks 2,3) cold. Region 2 is the
        // active append target, so the victim must be chosen among regions 0 and 1.
        for _ in 0..3 {
            let _ = ssd.get((0, 0)).await;
            let _ = ssd.get((0, 4)).await;
        }
        // Admit another block -> forces a new region, evicting the coldest non-active (1).
        ssd.admit((0, 6), mk(6));
        ssd.flush();

        assert!(metrics.snapshot().ssd_region_evictions >= 1);
        // Hot region 0 and active region 2 survive; cold region 1 was reclaimed.
        assert!(ssd.get((0, 0)).await.is_some());
        assert!(ssd.get((0, 1)).await.is_some());
        assert!(ssd.get((0, 4)).await.is_some());
        assert!(ssd.get((0, 5)).await.is_some());
        assert_eq!(ssd.get((0, 2)).await, None, "coldest region should be evicted");
        assert_eq!(ssd.get((0, 3)).await, None);
    }

    #[tokio::test]
    async fn admit_skips_oversize_and_duplicate() {
        let tmp = tempfile::tempdir().unwrap();
        let (ssd, metrics) = open(tmp.path(), 4 * 4096, 4096, 1);
        // Oversize (> region) is skipped.
        ssd.admit((0, 0), Bytes::from(vec![1u8; 5000]));
        // Duplicate admits collapse to one write.
        ssd.admit((0, 1), Bytes::from(vec![2u8; 100]));
        ssd.admit((0, 1), Bytes::from(vec![2u8; 100]));
        ssd.flush();
        assert_eq!(metrics.snapshot().ssd_writes, 1);
        assert_eq!(ssd.get((0, 0)).await, None);
        assert!(ssd.get((0, 1)).await.is_some());
    }

    #[test]
    fn cold_start_unlinks_stale_files() {
        let tmp = tempfile::tempdir().unwrap();
        // A stray file from a "previous process".
        let stray = tmp.path().join("comet-data-cache-77.bin");
        std::fs::write(&stray, b"junk").unwrap();
        assert!(stray.exists());
        let _ = open(tmp.path(), 4 * 4096, 4096, 1);
        assert!(!stray.exists(), "cold start must unlink stale shard files");
    }

    #[test]
    fn too_small_budget_disables_tier() {
        let tmp = tempfile::tempdir().unwrap();
        // Budget smaller than one region -> no tier.
        let metrics = Arc::new(Metrics::default());
        let ssd = SsdCache::open(
            SsdConfig {
                dir: tmp.path().to_path_buf(),
                total_limit: 100,
                num_shards: 1,
                region_size: 4096,
            },
            metrics,
        );
        assert!(ssd.is_none());
    }
}
