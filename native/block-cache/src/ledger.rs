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

//! Prefetch credit accounting (SCAN_PREFETCH_DESIGN.md §2.4).
//!
//! A [`PrefetchLedger`] is a per-plan credit pool bounding how many *unconsumed* prefetched
//! bytes a single scan may hold in the cache at once — the analog of the shuffle fetcher's
//! `maxBytesInFlight` window. The prefetcher [`charge`](PrefetchLedger::charge)s a fetch unit
//! before issuing it (awaiting when the window is full) and the cache
//! [`release`](PrefetchLedger::release)s the credit when the block is first consumed by a
//! demand read or evicted unconsumed. Because the window only advances as the scan consumes,
//! a stalled or `LIMIT`-satisfied consumer naturally freezes prefetch without any plan-shape
//! knowledge.

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Notify;

/// Why a block was inserted into the cache ahead of demand (SCAN_PREFETCH_DESIGN.md §2.1).
///
/// Drives release semantics on hit and eviction. Phase 3a uses only [`Prefetch`](Self::Prefetch)
/// (intra-task read-ahead). The cross-task warm-set (§2.10, phase 3b) adds a `Warm` variant that
/// inverts the SSD-admission rule; it is intentionally not present yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrefetchIntent {
    /// Intra-task read-ahead: the block is expected to be consumed within seconds by the same
    /// scan, so its first demand hit does not set `ever_visited` and it is never SSD-admitted
    /// (§2.5 — single-pass traffic must not become SSD wear).
    Prefetch,
}

/// How a charged prefetch credit was resolved. Fed back to the ledger's waste counters
/// (SCAN_PREFETCH_DESIGN.md §2.4, §2.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReleaseOutcome {
    /// The scan's demand read consumed the prefetched block (the coverage win).
    Consumed,
    /// The block was evicted (or its plan cancelled) before any demand read hit it — the
    /// prefetcher outran the cache's ability to hold its output.
    Wasted,
}

/// Per-call fetch statistics returned by [`crate::BlockCache::prefetch_ranges`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrefetchStats {
    /// Blocks newly fetched from the underlying store and inserted with the prefetch tag.
    pub blocks_fetched: u64,
    /// Bytes fetched from the underlying store (sum of coalesced fetch unit sizes).
    pub bytes_fetched: u64,
    /// Upstream fetch calls issued (a coalesced run counts once).
    pub fetch_requests: u64,
    /// Blocks that were already cached (memory or SSD) — a no-op probe, not fetched.
    pub blocks_already_cached: u64,
    /// Blocks skipped because a concurrent demand or prefetch fetch already owned them.
    pub blocks_in_flight: u64,
    /// Fetch units that failed (counted; never cached).
    pub errors: u64,
}

/// A per-plan credit pool bounding unconsumed prefetched bytes (SCAN_PREFETCH_DESIGN.md §2.4).
///
/// `outstanding` counts in-flight + fetched-but-unconsumed prefetched bytes. [`charge`] awaits
/// while a new reservation would push `outstanding` over `budget` (unless nothing is
/// outstanding, so a single fetch unit larger than the whole budget still makes progress).
/// [`release`] returns credit and wakes any awaiting charger.
///
/// [`charge`]: PrefetchLedger::charge
/// [`release`]: PrefetchLedger::release
#[derive(Debug)]
pub struct PrefetchLedger {
    budget: AtomicU64,
    outstanding: AtomicU64,
    consumed_bytes: AtomicU64,
    wasted_bytes: AtomicU64,
    notify: Notify,
}

impl PrefetchLedger {
    /// Create a ledger with the given ahead budget in bytes.
    pub fn new(budget: u64) -> Self {
        PrefetchLedger {
            budget: AtomicU64::new(budget),
            outstanding: AtomicU64::new(0),
            consumed_bytes: AtomicU64::new(0),
            wasted_bytes: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    /// Reserve `bytes` of ahead-budget, awaiting while the reservation would exceed the budget.
    ///
    /// Returns immediately when nothing is outstanding (so a lone oversized fetch unit cannot
    /// deadlock) or when the window has room. Because prefetch is best-effort, a caller that is
    /// cancelled while parked here simply never issues the fetch.
    pub async fn charge(&self, bytes: u64) {
        loop {
            // Register interest *before* checking so a release that races the check still
            // wakes us (canonical `Notify` lost-wakeup avoidance).
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if self.try_reserve(bytes) {
                return;
            }
            notified.await;
        }
    }

    /// Attempt to reserve `bytes` without awaiting. Returns whether the reservation succeeded.
    fn try_reserve(&self, bytes: u64) -> bool {
        let budget = self.budget.load(Ordering::Relaxed);
        let mut cur = self.outstanding.load(Ordering::Acquire);
        loop {
            // Always admit when idle so an oversized unit makes progress; otherwise gate on budget.
            if cur != 0 && cur.saturating_add(bytes) > budget {
                return false;
            }
            match self.outstanding.compare_exchange_weak(
                cur,
                cur + bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Return `bytes` of credit, recording the outcome, and wake any awaiting charger.
    pub fn release(&self, bytes: u64, outcome: ReleaseOutcome) {
        // Saturating so a stray double-release can never wrap the counter.
        let mut cur = self.outstanding.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.outstanding.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        match outcome {
            ReleaseOutcome::Consumed => {
                self.consumed_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            ReleaseOutcome::Wasted => {
                self.wasted_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
        }
        self.notify.notify_waiters();
    }

    /// Return `bytes` of credit that were charged but never turned into a prefetched block
    /// (e.g. a demand read claimed the block during the charge), without counting it as
    /// consumed or wasted — it is neither. Wakes any awaiting charger.
    pub fn refund(&self, bytes: u64) {
        let mut cur = self.outstanding.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.outstanding.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        self.notify.notify_waiters();
    }

    /// Current unconsumed (in-flight + fetched-but-unconsumed) prefetched bytes.
    pub fn outstanding(&self) -> u64 {
        self.outstanding.load(Ordering::Acquire)
    }

    /// The ahead budget in bytes.
    pub fn budget(&self) -> u64 {
        self.budget.load(Ordering::Relaxed)
    }

    /// Bytes released as consumed by a demand read.
    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes.load(Ordering::Relaxed)
    }

    /// Bytes released as wasted (evicted or cancelled unconsumed).
    pub fn wasted_bytes(&self) -> u64 {
        self.wasted_bytes.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn charge_within_budget_does_not_block() {
        let ledger = PrefetchLedger::new(100);
        ledger.charge(40).await;
        ledger.charge(40).await;
        assert_eq!(ledger.outstanding(), 80);
    }

    #[tokio::test]
    async fn charge_blocks_until_release() {
        let ledger = Arc::new(PrefetchLedger::new(100));
        ledger.charge(80).await;

        // This charge would exceed the budget (80 + 40 > 100); it must park.
        let l2 = Arc::clone(&ledger);
        let handle = tokio::spawn(async move {
            l2.charge(40).await;
        });

        // Give the task a chance to park.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !handle.is_finished(),
            "charge should block while window is full"
        );

        // Freeing enough credit wakes it.
        ledger.release(80, ReleaseOutcome::Consumed);
        handle.await.unwrap();
        assert_eq!(ledger.outstanding(), 40);
        assert_eq!(ledger.consumed_bytes(), 80);
    }

    #[tokio::test]
    async fn oversized_unit_admitted_when_idle() {
        let ledger = PrefetchLedger::new(100);
        // 500 > budget, but nothing outstanding, so it must not deadlock.
        ledger.charge(500).await;
        assert_eq!(ledger.outstanding(), 500);
    }

    #[test]
    fn release_saturates_and_tracks_waste() {
        let ledger = PrefetchLedger::new(100);
        assert!(ledger.try_reserve(50));
        ledger.release(50, ReleaseOutcome::Wasted);
        assert_eq!(ledger.outstanding(), 0);
        assert_eq!(ledger.wasted_bytes(), 50);
        // Over-release cannot wrap.
        ledger.release(50, ReleaseOutcome::Wasted);
        assert_eq!(ledger.outstanding(), 0);
    }
}
