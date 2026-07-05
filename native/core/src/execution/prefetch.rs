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

//! Asynchronous scan prefetch (SCAN_PREFETCH_DESIGN.md, phase 3a).
//!
//! A [`ScanPrefetcher`] runs entirely on Comet's tokio runtime — never on the Spark task
//! thread — and warms the object-store data cache ahead of a native scan's demand reads. For
//! each file, in scan-consumption order, it computes the byte ranges the scan is about to read
//! (footer → row-group prune → projected column-chunk ranges) and pulls them into the cache
//! several requests in parallel under a strict ahead-budget. Correctness never depends on the
//! prefetcher: it writes only through the same cache/fetch path demand reads use, and a
//! missing, failed, or cancelled prefetch only costs latency.
//!
//! The planner accumulates a [`PrefetchSpec`] per scan; `jni_api` spawns one task per spec and
//! stores a [`PrefetchHandle`] to cancel it cooperatively at plan teardown.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::parquet::{
    ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
};
use datafusion::physical_expr::utils::conjunction_opt;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::{Count, ExecutionPlanMetricsSet};
use datafusion_datasource::FileRange;
use datafusion_pruning::build_pruning_predicate;
use futures::future::{BoxFuture, FutureExt};
use log::debug;
use object_store::path::Path;
use object_store::ObjectStoreExt;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::arrow::ProjectionMask;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::ParquetMetaDataReader;
use tokio::sync::Notify;

use datafusion_comet_block_cache::{PrefetchIntent, PrefetchLedger};
use datafusion_comet_object_store_cache::CachingObjectStore;

use crate::execution::spark_config::{
    SparkConfig, COMET_PREFETCH_AHEAD_BUDGET, COMET_PREFETCH_ENABLED, COMET_PREFETCH_FILTER_AWARE,
    COMET_PREFETCH_MAX_CONCURRENT_REQUESTS,
};

/// Parquet footer prefetch hint — shared with the scan's own metadata read
/// (`parquet_exec::METADATA_SIZE_HINT`) so the prefetcher lands byte-identical footer blocks.
const FOOTER_SIZE_HINT: usize = crate::parquet::parquet_exec::METADATA_SIZE_HINT;

/// After this many files whose data prefetch was skipped (e.g. footer parse failure), the
/// plan's prefetch task gives up (SCAN_PREFETCH_DESIGN.md §2.6).
const MAX_FILES_SKIPPED: u32 = 3;

/// Defaults mirroring `CometConf` (SCAN_PREFETCH_DESIGN.md §2.8) so native code has sane values
/// even if a config fails to reach it.
const DEFAULT_AHEAD_BUDGET: u64 = 32 << 20;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 3;

/// Runtime knobs read from the Spark config at plan creation. `Default` is the disabled
/// placeholder the planner carries when prefetch is off.
#[derive(Clone, Copy, Debug, Default)]
pub struct PrefetchConfig {
    pub enabled: bool,
    pub ahead_budget: u64,
    pub max_concurrent_requests: usize,
    pub filter_aware: bool,
}

impl PrefetchConfig {
    /// Parse from the Spark config map. `enabled` is gated on the data cache being enabled by
    /// the caller (the cache is the prefetch buffer); this only reads the prefetch knobs.
    pub fn from_spark(cfg: &HashMap<String, String>) -> Self {
        PrefetchConfig {
            enabled: cfg.get_bool(COMET_PREFETCH_ENABLED),
            ahead_budget: cfg.get_u64(COMET_PREFETCH_AHEAD_BUDGET, DEFAULT_AHEAD_BUDGET),
            max_concurrent_requests: cfg
                .get_usize(
                    COMET_PREFETCH_MAX_CONCURRENT_REQUESTS,
                    DEFAULT_MAX_CONCURRENT_REQUESTS,
                )
                .max(1),
            filter_aware: cfg
                .get(COMET_PREFETCH_FILTER_AWARE)
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(true),
        }
    }
}

/// One file's prefetch input, captured at plan time.
#[derive(Clone, Debug)]
pub struct PrefetchFile {
    /// Object path within the store.
    pub path: Path,
    /// Full object size in bytes.
    pub size: u64,
    /// The split this task reads, `[start, end)`, or `None` for the whole file.
    pub range: Option<Range<u64>>,
}

/// Format-specific inputs driving range computation.
pub enum PrefetchKind {
    Parquet {
        /// Root (top-level) column indices the scan projects, into the file schema.
        projection: Vec<usize>,
        /// Compiled scan `data_filters` (column indices into `filter_schema`).
        data_filters: Vec<Arc<dyn PhysicalExpr>>,
        /// Schema the `data_filters` were bound against (for statistics pruning).
        filter_schema: SchemaRef,
        /// Whether the file is encrypted (degrades to footer-only prefetch).
        encryption: bool,
    },
    /// CSV: the split range *is* what the reader consumes — sequential prefetch, no metadata.
    Csv,
}

/// Everything needed to run one scan's prefetch, accumulated on the planner.
pub struct PrefetchSpec {
    pub store: Arc<CachingObjectStore>,
    pub files: Vec<PrefetchFile>,
    pub kind: PrefetchKind,
    pub config: PrefetchConfig,
}

/// Cooperative cancellation state shared between a [`PrefetchHandle`] and its running task.
#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    notify: Notify,
}

/// A cancel handle for a spawned prefetch task, stored in the `ExecutionContext` and tripped by
/// `releasePlan`. Cancellation is cooperative — never `JoinHandle::abort` — so at most the
/// in-flight requests complete (and still benefit later queries) before the task exits
/// (SCAN_PREFETCH_DESIGN.md §2.2).
#[derive(Clone)]
pub struct PrefetchHandle {
    state: Arc<CancelState>,
}

impl Default for PrefetchHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl PrefetchHandle {
    pub fn new() -> Self {
        PrefetchHandle {
            state: Arc::new(CancelState::default()),
        }
    }

    /// Request cancellation and wake a task parked on the ahead-budget.
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Relaxed);
        self.state.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.state.cancelled.load(Ordering::Relaxed)
    }

    /// Resolve once cancellation is requested (for use in `select!`).
    async fn cancelled(&self) {
        loop {
            let notified = self.state.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            notified.await;
        }
    }
}

/// A [`MetadataFetch`] that reads footer bytes through the caching store, so the footer is
/// warmed in the cache as a side effect of parsing it (byte-identical to the scan's own read).
struct StoreMetadataFetch {
    store: Arc<CachingObjectStore>,
    path: Path,
}

impl MetadataFetch for StoreMetadataFetch {
    fn fetch(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        let store = Arc::clone(&self.store);
        let path = self.path.clone();
        async move {
            store
                .get_range(&path, range)
                .await
                .map_err(|e| ParquetError::General(format!("prefetch footer read: {e}")))
        }
        .boxed()
    }
}

/// Drive prefetch for one scan spec until the file list is exhausted or the handle is cancelled.
/// This is the task body spawned onto the tokio runtime by `jni_api`.
pub async fn run(spec: PrefetchSpec, handle: PrefetchHandle) {
    let ledger = Arc::new(PrefetchLedger::new(spec.config.ahead_budget));
    let max_concurrent = spec.config.max_concurrent_requests;
    let mut files_skipped = 0u32;

    for file in &spec.files {
        if handle.is_cancelled() {
            break;
        }

        let ranges = match compute_ranges(&spec, file).await {
            Ok(ranges) => ranges,
            Err(e) => {
                debug!("prefetch: skipping {} ({e})", file.path);
                spec.store.note_prefetch_file_skipped();
                files_skipped += 1;
                if files_skipped >= MAX_FILES_SKIPPED {
                    debug!("prefetch: too many skipped files; stopping this plan's prefetch");
                    break;
                }
                continue;
            }
        };
        if ranges.is_empty() {
            continue;
        }

        // Warm this file's ranges, paced by the ledger + the process-wide semaphore. Racing the
        // whole call against cancellation bounds teardown latency to the in-flight requests;
        // dropping it mid-fetch leaves stale in-flight entries that the demand path's
        // waiter-retry-once cleanly recovers (§2.2, §2.6).
        tokio::select! {
            _ = handle.cancelled() => break,
            res = spec.store.prefetch_ranges(
                &file.path, &ranges, PrefetchIntent::Prefetch, &ledger, max_concurrent,
            ) => {
                if let Err(e) = res {
                    debug!("prefetch: fetch error for {}: {e}", file.path);
                }
            }
        }
    }
}

/// Compute the byte ranges to prefetch for one file. Returns an empty vec when there is nothing
/// worth prefetching; returns `Err` only for a genuine failure (counted as a skipped file).
async fn compute_ranges(
    spec: &PrefetchSpec,
    file: &PrefetchFile,
) -> Result<Vec<Range<u64>>, ParquetError> {
    match &spec.kind {
        PrefetchKind::Csv => {
            // The split `[start, end)` is exactly what the reader consumes.
            let range = file.range.clone().unwrap_or(0..file.size);
            if range.start >= range.end {
                Ok(Vec::new())
            } else {
                Ok(vec![range])
            }
        }
        PrefetchKind::Parquet {
            projection,
            data_filters,
            filter_schema,
            encryption,
        } => {
            // Footer tail range — byte-identical to the scan's metadata read.
            let hint = FOOTER_SIZE_HINT.min(file.size as usize) as u64;
            let footer = file.size.saturating_sub(hint)..file.size;

            if *encryption {
                // Encrypted footer bytes are still what the scan reads first; warm only those.
                // Plaintext metadata is unavailable without key material (deferred, §2.11).
                return Ok(if footer.start < footer.end {
                    vec![footer]
                } else {
                    Vec::new()
                });
            }

            // Parse metadata via the caching store (warms the footer through the cache).
            let fetch = StoreMetadataFetch {
                store: Arc::clone(&spec.store),
                path: file.path.clone(),
            };
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(FOOTER_SIZE_HINT))
                .load_and_finish(fetch, file.size)
                .await?;

            let rg_metadata = metadata.row_groups();
            if rg_metadata.is_empty() {
                return Ok(Vec::new());
            }
            let schema_descr = metadata.file_metadata().schema_descr();

            // Start from "scan every row group", then prune (never under-select).
            let mut filter =
                RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(rg_metadata.len()));

            // Range pruning: only row groups whose first page falls in this task's split — the
            // same midpoint rule the opener applies (opener/mod.rs).
            if let Some(range) = &file.range {
                filter.prune_by_range(
                    rg_metadata,
                    &FileRange {
                        start: range.start as i64,
                        end: range.end as i64,
                    },
                );
            }

            // Statistics pruning (best-effort, `filterAware`). Any failure — predicate not
            // convertible, schema mismatch, missing stats — silently degrades to range pruning.
            if spec.config.filter_aware && !data_filters.is_empty() {
                if let Some(predicate) = conjunction_opt(data_filters.iter().cloned()) {
                    let errors = Count::new();
                    if let Some(pruning_predicate) =
                        build_pruning_predicate(predicate, filter_schema, &errors)
                    {
                        let metrics = ParquetFileMetrics::new(
                            0,
                            file.path.as_ref(),
                            &ExecutionPlanMetricsSet::new(),
                        );
                        filter.prune_by_statistics(
                            filter_schema,
                            schema_descr,
                            rg_metadata,
                            &pruning_predicate,
                            &metrics,
                        );
                    }
                }
            }

            // Projected leaf column chunks of each surviving row group, in file-offset order.
            let num_roots = schema_descr.root_schema().get_fields().len();
            let roots: Vec<usize> = projection
                .iter()
                .copied()
                .filter(|&r| r < num_roots)
                .collect();
            if roots.is_empty() {
                return Ok(Vec::new());
            }
            let mask = ProjectionMask::roots(schema_descr, roots);

            let mut ranges: Vec<Range<u64>> = Vec::new();
            for rg_idx in filter.row_group_indexes() {
                let rg = &rg_metadata[rg_idx];
                for leaf in 0..schema_descr.num_columns() {
                    if mask.leaf_included(leaf) {
                        let (start, len) = rg.column(leaf).byte_range();
                        if len > 0 {
                            ranges.push(start..start + len);
                        }
                    }
                }
            }

            // Enqueue in file-offset order (the order the record-batch stream requests them);
            // the cache dedups/coalesces at block granularity.
            ranges.sort_by_key(|r| r.start);
            ranges.dedup();
            Ok(ranges)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_when_absent() {
        let cfg = PrefetchConfig::from_spark(&HashMap::new());
        assert!(!cfg.enabled);
        assert_eq!(cfg.ahead_budget, DEFAULT_AHEAD_BUDGET);
        assert_eq!(cfg.max_concurrent_requests, DEFAULT_MAX_CONCURRENT_REQUESTS);
        assert!(cfg.filter_aware);
    }

    #[test]
    fn config_parses_explicit_values() {
        let cfg = PrefetchConfig::from_spark(&HashMap::from([
            (COMET_PREFETCH_ENABLED.to_string(), "true".to_string()),
            (
                COMET_PREFETCH_AHEAD_BUDGET.to_string(),
                (64u64 << 20).to_string(),
            ),
            (
                COMET_PREFETCH_MAX_CONCURRENT_REQUESTS.to_string(),
                "5".to_string(),
            ),
            (COMET_PREFETCH_FILTER_AWARE.to_string(), "false".to_string()),
        ]));
        assert!(cfg.enabled);
        assert_eq!(cfg.ahead_budget, 64 << 20);
        assert_eq!(cfg.max_concurrent_requests, 5);
        assert!(!cfg.filter_aware);
    }

    #[test]
    fn max_concurrent_never_zero() {
        let cfg = PrefetchConfig::from_spark(&HashMap::from([(
            COMET_PREFETCH_MAX_CONCURRENT_REQUESTS.to_string(),
            "0".to_string(),
        )]));
        assert_eq!(cfg.max_concurrent_requests, 1);
    }

    #[tokio::test]
    async fn parquet_prefetch_selects_projected_chunks_and_warms_cache() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion_comet_block_cache::{BlockCache, BlockCacheConfig};
        use object_store::memory::InMemory;
        use object_store::{ObjectStore, PutPayload};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        // A 3-column file spanning several cache blocks but exactly two row groups. Values are
        // incompressible (LCG-derived) so the file stays multi-MiB and the footer read (which
        // warms only the tail block) leaves earlier data chunks for prefetch to fetch.
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let n: usize = 400_000;
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
            s
        };
        let a: Vec<i64> = (0..n).map(|_| next() as i64).collect();
        let b: Vec<f64> = (0..n).map(|_| (next() >> 11) as f64).collect();
        let c: Vec<i64> = (0..n).map(|_| next() as i64).collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(a)),
                Arc::new(Float64Array::from(b)),
                Arc::new(Int64Array::from(c)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(200_000))
            .build();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let size = buf.len() as u64;

        let mem = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        mem.put(&path, PutPayload::from(buf)).await.unwrap();

        let cache = BlockCache::new(BlockCacheConfig {
            block_size: 1 << 20,
            memory_budget: 64 << 20,
            num_shards: 4,
            ..Default::default()
        });
        let store = Arc::new(CachingObjectStore::new(mem, Arc::clone(&cache), 7));

        // Project columns a (0) and c (2), skipping b (1).
        let spec = PrefetchSpec {
            store: Arc::clone(&store),
            files: vec![PrefetchFile {
                path: path.clone(),
                size,
                range: None,
            }],
            kind: PrefetchKind::Parquet {
                projection: vec![0, 2],
                data_filters: vec![],
                filter_schema: Arc::clone(&schema),
                encryption: false,
            },
            config: PrefetchConfig {
                enabled: true,
                ahead_budget: 32 << 20,
                max_concurrent_requests: 3,
                filter_aware: true,
            },
        };

        // Two row groups × two projected columns = four column-chunk ranges (not six).
        let ranges = compute_ranges(&spec, &spec.files[0]).await.unwrap();
        assert_eq!(
            ranges.len(),
            4,
            "only projected columns' chunks, both row groups"
        );

        // Running the prefetch warms those blocks; the ledger holds them until consumed.
        run(spec, PrefetchHandle::new()).await;
        let after_prefetch = cache.stats();
        assert!(
            after_prefetch.prefetch_bytes_fetched > 0,
            "prefetch fetched data"
        );
        assert!(
            after_prefetch.prefetch_blocks_consumed == 0,
            "not yet consumed"
        );

        // A demand read over a prefetched range consumes the tagged blocks (no new upstream GET
        // for those blocks) — coverage.
        let demand = store.get_ranges(&path, &[ranges[0].clone()]).await.unwrap();
        assert!(!demand.is_empty());
        assert!(
            cache.stats().prefetch_blocks_consumed > 0,
            "demand read consumed prefetched blocks"
        );
    }

    #[tokio::test]
    async fn handle_cancel_resolves() {
        let handle = PrefetchHandle::new();
        assert!(!handle.is_cancelled());
        let h2 = handle.clone();
        let waiter = tokio::spawn(async move { h2.cancelled().await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        handle.cancel();
        // Must resolve promptly once cancelled.
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("cancelled() should resolve")
            .unwrap();
        assert!(handle.is_cancelled());
    }
}
