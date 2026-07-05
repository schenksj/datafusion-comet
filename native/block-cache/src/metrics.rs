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

use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide cache counters. Cheap relaxed atomics; read via [`Metrics::snapshot`].
///
/// Phase 1 exposes these through a periodic native log line (the wrapper drives it);
/// Spark-visible SQL metrics are a tracked follow-up.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Block reads served from the memory tier.
    pub hits: AtomicU64,
    /// Block reads that had to go upstream.
    pub misses: AtomicU64,
    /// Upstream fetch calls issued (a coalesced run counts once).
    pub fetches: AtomicU64,
    /// Bytes returned by upstream fetches.
    pub bytes_fetched: AtomicU64,
    /// Blocks evicted from the memory tier.
    pub evictions: AtomicU64,
    /// Files invalidated because their version changed under us.
    pub invalidations: AtomicU64,
    /// Block reads served from the SSD tier (avoided a network fetch).
    pub ssd_hits: AtomicU64,
    /// Blocks written to the SSD tier.
    pub ssd_writes: AtomicU64,
    /// SSD reads that failed crc32c verification and fell through to the network.
    pub ssd_corruptions: AtomicU64,
    /// SSD regions reclaimed wholesale by the eviction policy.
    pub ssd_region_evictions: AtomicU64,
    /// Bytes fetched upstream by the prefetcher (SCAN_PREFETCH_DESIGN.md §2.9).
    pub prefetch_bytes_fetched: AtomicU64,
    /// Upstream fetch calls issued by the prefetcher (a coalesced run counts once).
    pub prefetch_fetch_requests: AtomicU64,
    /// Prefetched blocks consumed by a demand read (first hit on a tagged block).
    pub prefetch_blocks_consumed: AtomicU64,
    /// Prefetched blocks evicted (or cancelled) before any demand read hit them.
    pub prefetch_blocks_wasted: AtomicU64,
    /// Prefetch fetch units that failed (never cached; the demand path retries).
    pub prefetch_errors: AtomicU64,
    /// Files whose data prefetch was skipped (e.g. footer parse failure), counted by the
    /// core-side `ScanPrefetcher` (§2.3, §2.6).
    pub prefetch_files_skipped: AtomicU64,
}

/// A point-in-time copy of [`Metrics`], safe to format/log.
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub fetches: u64,
    pub bytes_fetched: u64,
    pub evictions: u64,
    pub invalidations: u64,
    pub ssd_hits: u64,
    pub ssd_writes: u64,
    pub ssd_corruptions: u64,
    pub ssd_region_evictions: u64,
    pub prefetch_bytes_fetched: u64,
    pub prefetch_fetch_requests: u64,
    pub prefetch_blocks_consumed: u64,
    pub prefetch_blocks_wasted: u64,
    pub prefetch_errors: u64,
    pub prefetch_files_skipped: u64,
}

impl MetricsSnapshot {
    /// Prefetch coverage in `[0.0, 1.0]`: consumed / (consumed + wasted). The single number
    /// that says whether prefetch is paying (§2.9); `0.0` when nothing has resolved yet.
    pub fn prefetch_coverage(&self) -> f64 {
        let total = self.prefetch_blocks_consumed + self.prefetch_blocks_wasted;
        if total == 0 {
            0.0
        } else {
            self.prefetch_blocks_consumed as f64 / total as f64
        }
    }

    /// Hit ratio over block reads in `[0.0, 1.0]`; `0.0` when nothing has been read yet.
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

impl Metrics {
    #[inline]
    pub(crate) fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_fetch(&self, bytes: u64) {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.bytes_fetched.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_eviction(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_ssd_hit(&self) {
        self.ssd_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_ssd_write(&self) {
        self.ssd_writes.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_ssd_corruption(&self) {
        self.ssd_corruptions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_ssd_region_eviction(&self) {
        self.ssd_region_evictions.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_prefetch_fetch(&self, bytes: u64) {
        self.prefetch_fetch_requests.fetch_add(1, Ordering::Relaxed);
        self.prefetch_bytes_fetched
            .fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_prefetch_consumed(&self) {
        self.prefetch_blocks_consumed
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_prefetch_wasted(&self) {
        self.prefetch_blocks_wasted.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn record_prefetch_error(&self) {
        self.prefetch_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a file whose data prefetch was skipped (called by the core `ScanPrefetcher`).
    #[inline]
    pub fn record_prefetch_file_skipped(&self) {
        self.prefetch_files_skipped.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            fetches: self.fetches.load(Ordering::Relaxed),
            bytes_fetched: self.bytes_fetched.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
            ssd_hits: self.ssd_hits.load(Ordering::Relaxed),
            ssd_writes: self.ssd_writes.load(Ordering::Relaxed),
            ssd_corruptions: self.ssd_corruptions.load(Ordering::Relaxed),
            ssd_region_evictions: self.ssd_region_evictions.load(Ordering::Relaxed),
            prefetch_bytes_fetched: self.prefetch_bytes_fetched.load(Ordering::Relaxed),
            prefetch_fetch_requests: self.prefetch_fetch_requests.load(Ordering::Relaxed),
            prefetch_blocks_consumed: self.prefetch_blocks_consumed.load(Ordering::Relaxed),
            prefetch_blocks_wasted: self.prefetch_blocks_wasted.load(Ordering::Relaxed),
            prefetch_errors: self.prefetch_errors.load(Ordering::Relaxed),
            prefetch_files_skipped: self.prefetch_files_skipped.load(Ordering::Relaxed),
        }
    }
}
