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
    use arrow::array::{Float64Array, Int64Array, StructArray};
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use arrow::record_batch::RecordBatch;
    use bytes::Bytes;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::scalar::ScalarValue;
    use datafusion_comet_block_cache::{BlockCache, BlockCacheConfig};
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::{
        GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as OsResult,
    };
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
    use parquet::file::properties::WriterProperties;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    use std::time::Duration;

    // ---- shared test helpers ----

    const BLOCK: u64 = 1 << 20;

    /// Incompressible i64 column of length `n` (LCG-derived) so files stay multi-block.
    fn lcg(n: usize, seed: u64) -> Vec<i64> {
        let mut s = seed;
        (0..n)
            .map(|_| {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
                s as i64
            })
            .collect()
    }

    /// Write `batch` to an in-memory Parquet buffer, row groups capped at `rg_rows` rows.
    fn write_parquet(batch: &RecordBatch, rg_rows: usize) -> Vec<u8> {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rg_rows))
            .build();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
            w.write(batch).unwrap();
            w.close().unwrap();
        }
        buf
    }

    /// Parse Parquet metadata from a full in-memory buffer (to derive expected offsets in tests).
    fn parse_metadata(bytes: &[u8]) -> ParquetMetaData {
        ParquetMetaDataReader::new()
            .parse_and_finish(&Bytes::copy_from_slice(bytes))
            .unwrap()
    }

    /// Put `bytes` at `data.parquet` in a fresh `InMemory` and wrap it in a caching store.
    async fn store_over(bytes: Vec<u8>) -> (Arc<CachingObjectStore>, Arc<BlockCache>, Path, u64) {
        let inner = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        inner
            .put(&path, PutPayload::from(bytes.clone()))
            .await
            .unwrap();
        wrap(inner, path, bytes.len() as u64)
    }

    fn wrap(
        inner: Arc<dyn ObjectStore>,
        path: Path,
        size: u64,
    ) -> (Arc<CachingObjectStore>, Arc<BlockCache>, Path, u64) {
        let cache = BlockCache::new(BlockCacheConfig {
            block_size: BLOCK,
            memory_budget: 64 << 20,
            num_shards: 4,
            ..Default::default()
        });
        let store = Arc::new(CachingObjectStore::new(inner, Arc::clone(&cache), 7));
        (store, cache, path, size)
    }

    #[allow(clippy::too_many_arguments)]
    fn parquet_spec(
        store: Arc<CachingObjectStore>,
        path: Path,
        size: u64,
        projection: Vec<usize>,
        data_filters: Vec<Arc<dyn PhysicalExpr>>,
        filter_schema: SchemaRef,
        range: Option<Range<u64>>,
        filter_aware: bool,
    ) -> PrefetchSpec {
        PrefetchSpec {
            store,
            files: vec![PrefetchFile { path, size, range }],
            kind: PrefetchKind::Parquet {
                projection,
                data_filters,
                filter_schema,
                encryption: false,
            },
            config: PrefetchConfig {
                enabled: true,
                ahead_budget: 32 << 20,
                max_concurrent_requests: 3,
                filter_aware,
            },
        }
    }

    /// An `ObjectStore` that counts `get_opts` calls, tracks peak concurrent in-flight `get_opts`
    /// (with a small delay to force overlap), and optionally fails the first `get_opts` whose
    /// range starts at `fail_first_at`. Everything else delegates to the inner store.
    #[derive(Debug)]
    struct ProbeStore {
        inner: Arc<dyn ObjectStore>,
        calls: AtomicU64,
        in_flight: AtomicU64,
        max_in_flight: AtomicU64,
        delay: Duration,
        fail_first_at: Option<u64>,
        failed: AtomicU64,
    }

    impl ProbeStore {
        fn new(
            inner: Arc<dyn ObjectStore>,
            delay: Duration,
            fail_first_at: Option<u64>,
        ) -> Arc<Self> {
            Arc::new(ProbeStore {
                inner,
                calls: AtomicU64::new(0),
                in_flight: AtomicU64::new(0),
                max_in_flight: AtomicU64::new(0),
                delay,
                fail_first_at,
                failed: AtomicU64::new(0),
            })
        }
        fn calls(&self) -> u64 {
            self.calls.load(AtomicOrdering::SeqCst)
        }
        fn max_in_flight(&self) -> u64 {
            self.max_in_flight.load(AtomicOrdering::SeqCst)
        }
    }

    impl std::fmt::Display for ProbeStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ProbeStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ProbeStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> OsResult<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OsResult<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if let (Some(at), Some(object_store::GetRange::Bounded(r))) =
                (self.fail_first_at, options.range.as_ref())
            {
                if r.start == at && self.failed.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                    return Err(object_store::Error::Generic {
                        store: "ProbeStore",
                        source: "injected transient failure".into(),
                    });
                }
            }
            let now = self.in_flight.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            self.max_in_flight.fetch_max(now, AtomicOrdering::SeqCst);
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            let result = self.inner.get_opts(location, options).await;
            self.in_flight.fetch_sub(1, AtomicOrdering::SeqCst);
            result
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OsResult<Path>>,
        ) -> BoxStream<'static, OsResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> OsResult<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

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

    /// A 3-column (a:i64, b:f64, c:i64) file, two row groups, incompressible so it spans several
    /// cache blocks — the footer read warms only the tail, leaving data chunks for prefetch.
    fn three_col_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let b: Vec<f64> = lcg(n, 0x1234).iter().map(|&v| (v as f64).abs()).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(lcg(n, 0x9E37))),
                Arc::new(Float64Array::from(b)),
                Arc::new(Int64Array::from(lcg(n, 0xABCD))),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn parquet_prefetch_selects_projected_chunks_and_warms_cache() {
        let batch = three_col_batch(400_000);
        let schema = batch.schema();
        let (store, cache, path, size) = store_over(write_parquet(&batch, 200_000)).await;

        // Project columns a (0) and c (2), skipping b (1).
        let spec = parquet_spec(
            Arc::clone(&store),
            path.clone(),
            size,
            vec![0, 2],
            vec![],
            Arc::clone(&schema),
            None,
            true,
        );

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
    async fn split_range_pruning_matches_first_page_rule() {
        // Three row groups of one i64 column.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(lcg(300, 7)))],
        )
        .unwrap();
        let bytes = write_parquet(&batch, 100);
        let meta = parse_metadata(&bytes);
        assert_eq!(meta.num_row_groups(), 3);

        // The prune rule keeps a row group iff its first column's page offset is within the split.
        // Target exactly row group 1.
        let (rg1_start, rg1_len) = meta.row_group(1).column(0).byte_range();
        let (store, _cache, path, size) = store_over(bytes).await;
        let spec = parquet_spec(
            store,
            path,
            size,
            vec![0],
            vec![],
            Arc::clone(&schema),
            Some(rg1_start..rg1_start + 1),
            false,
        );

        let ranges = compute_ranges(&spec, &spec.files[0]).await.unwrap();
        assert_eq!(
            ranges,
            vec![rg1_start..rg1_start + rg1_len],
            "only RG1 survives"
        );
    }

    #[tokio::test]
    async fn stats_pruning_respects_filter_aware() {
        // Two row groups with disjoint, sorted `a` ranges: RG0 in [0,99], RG1 in [1000,1099].
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let a: Vec<i64> = (0..200)
            .map(|i| if i < 100 { i } else { 900 + i })
            .collect();
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(a))]).unwrap();
        let (store, _cache, path, size) = store_over(write_parquet(&batch, 100)).await;

        // `a >= 500` cannot be true for RG0 (max 99); stats pruning drops it.
        let predicate = binary(
            col("a", &schema).unwrap(),
            Operator::GtEq,
            lit(ScalarValue::Int64(Some(500))),
            &schema,
        )
        .unwrap();

        let aware = parquet_spec(
            Arc::clone(&store),
            path.clone(),
            size,
            vec![0],
            vec![Arc::clone(&predicate)],
            Arc::clone(&schema),
            None,
            true,
        );
        assert_eq!(
            compute_ranges(&aware, &aware.files[0]).await.unwrap().len(),
            1,
            "filterAware prunes RG0 by statistics"
        );

        let unaware = parquet_spec(
            store,
            path,
            size,
            vec![0],
            vec![predicate],
            Arc::clone(&schema),
            None,
            false,
        );
        assert_eq!(
            compute_ranges(&unaware, &unaware.files[0])
                .await
                .unwrap()
                .len(),
            2,
            "filterAware=false keeps both row groups"
        );
    }

    #[tokio::test]
    async fn nested_projection_maps_to_struct_leaves() {
        // Schema: struct s { x, y } (root 0, leaves 0,1) and flat z (root 1, leaf 2).
        let struct_fields = Fields::from(vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Int64, false),
        ]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Struct(struct_fields.clone()), false),
            Field::new("z", DataType::Int64, false),
        ]));
        let s = StructArray::new(
            struct_fields,
            vec![
                Arc::new(Int64Array::from(lcg(100, 1))),
                Arc::new(Int64Array::from(lcg(100, 2))),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(s), Arc::new(Int64Array::from(lcg(100, 3)))],
        )
        .unwrap();
        let (store, _cache, path, size) = store_over(write_parquet(&batch, 1000)).await;

        // Project the struct (root 0) -> its two leaf chunks (x, y); z excluded.
        let struct_spec = parquet_spec(
            Arc::clone(&store),
            path.clone(),
            size,
            vec![0],
            vec![],
            Arc::clone(&schema),
            None,
            true,
        );
        assert_eq!(
            compute_ranges(&struct_spec, &struct_spec.files[0])
                .await
                .unwrap()
                .len(),
            2,
            "struct projection maps to its two leaves"
        );

        // Project only z (root 1) -> its single leaf chunk.
        let z_spec = parquet_spec(store, path, size, vec![1], vec![], schema, None, true);
        assert_eq!(
            compute_ranges(&z_spec, &z_spec.files[0])
                .await
                .unwrap()
                .len(),
            1,
            "flat projection maps to a single leaf"
        );
    }

    #[tokio::test]
    async fn encrypted_spec_degrades_to_footer_only() {
        let batch = three_col_batch(2000);
        let schema = batch.schema();
        let (store, _cache, path, size) = store_over(write_parquet(&batch, 1000)).await;
        let mut spec = parquet_spec(store, path, size, vec![0, 1, 2], vec![], schema, None, true);
        if let PrefetchKind::Parquet { encryption, .. } = &mut spec.kind {
            *encryption = true;
        }

        let ranges = compute_ranges(&spec, &spec.files[0]).await.unwrap();
        let hint = (FOOTER_SIZE_HINT as u64).min(size);
        assert_eq!(
            ranges,
            vec![size - hint..size],
            "encrypted files prefetch only the footer tail"
        );
    }

    #[tokio::test]
    async fn csv_spec_covers_exactly_the_split() {
        let (store, _cache, path, _size) = store_over(vec![b'x'; 100]).await;
        let mk = |range: Option<Range<u64>>| PrefetchSpec {
            store: Arc::clone(&store),
            files: vec![PrefetchFile {
                path: path.clone(),
                size: 100,
                range,
            }],
            kind: PrefetchKind::Csv,
            config: PrefetchConfig {
                enabled: true,
                ahead_budget: 32 << 20,
                max_concurrent_requests: 3,
                filter_aware: true,
            },
        };
        assert_eq!(
            compute_ranges(&mk(Some(10..50)), &mk(Some(10..50)).files[0])
                .await
                .unwrap(),
            vec![10..50]
        );
        assert_eq!(
            compute_ranges(&mk(None), &mk(None).files[0]).await.unwrap(),
            vec![0..100],
            "no split = whole file"
        );
        assert!(
            compute_ranges(&mk(Some(5..5)), &mk(Some(5..5)).files[0])
                .await
                .unwrap()
                .is_empty(),
            "empty split = nothing"
        );
    }

    #[tokio::test]
    async fn footer_parse_failure_skips_and_counts() {
        // Not a Parquet file — footer parse must fail, the file is skipped and counted.
        let (store, cache, path, size) = store_over(b"this is not parquet".to_vec()).await;
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let spec = parquet_spec(store, path, size, vec![0], vec![], schema, None, true);

        assert!(compute_ranges(&spec, &spec.files[0]).await.is_err());
        run(spec, PrefetchHandle::new()).await;
        assert_eq!(
            cache.stats().prefetch_files_skipped,
            1,
            "a footer-parse failure skips the file and counts it"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prefetch_drives_concurrent_upstream_requests() {
        // Projecting two non-adjacent columns yields two separate coalesced runs, which
        // buffer_unordered fetches concurrently — the #3817 single-outstanding-I/O fix.
        let batch = three_col_batch(400_000);
        let schema = batch.schema();
        let inner = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        inner
            .put(&path, PutPayload::from(write_parquet(&batch, 200_000)))
            .await
            .unwrap();
        let size = inner.head(&path).await.unwrap().size;
        let probe = ProbeStore::new(inner, Duration::from_millis(40), None);
        let (store, _cache, _p, _s) = wrap(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            path.clone(),
            size,
        );

        let spec = parquet_spec(store, path, size, vec![0, 2], vec![], schema, None, true);
        run(spec, PrefetchHandle::new()).await;
        assert!(
            probe.max_in_flight() > 1,
            "prefetch must issue concurrent upstream requests (saw {})",
            probe.max_in_flight()
        );
    }

    #[tokio::test]
    async fn second_scan_issues_zero_upstream_requests() {
        let batch = three_col_batch(400_000);
        let schema = batch.schema();
        let inner = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        inner
            .put(&path, PutPayload::from(write_parquet(&batch, 200_000)))
            .await
            .unwrap();
        let size = inner.head(&path).await.unwrap().size;
        let probe = ProbeStore::new(inner, Duration::ZERO, None);
        let (store, _cache, _p, _s) = wrap(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            path.clone(),
            size,
        );

        let spec = parquet_spec(
            Arc::clone(&store),
            path.clone(),
            size,
            vec![0, 2],
            vec![],
            Arc::clone(&schema),
            None,
            true,
        );
        let ranges = compute_ranges(&spec, &spec.files[0]).await.unwrap();
        run(spec, PrefetchHandle::new()).await;
        // Demand-read every prefetched range to fully warm the cache.
        store.get_ranges(&path, &ranges).await.unwrap();
        let calls_after_warm = probe.calls();
        assert!(calls_after_warm > 0);

        // A second identical scan is served entirely from the cache.
        store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(
            probe.calls(),
            calls_after_warm,
            "second scan must issue zero upstream requests"
        );
    }

    #[tokio::test]
    async fn injected_prefetch_failure_is_counted_and_query_still_succeeds() {
        let batch = three_col_batch(400_000);
        let schema = batch.schema();
        let inner = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        inner
            .put(&path, PutPayload::from(write_parquet(&batch, 200_000)))
            .await
            .unwrap();
        let size = inner.head(&path).await.unwrap().size;
        let meta = {
            let bytes = inner.get(&path).await.unwrap().bytes().await.unwrap();
            parse_metadata(&bytes)
        };
        // Fail the first fetch of row group 0's column a (block-aligned start).
        let (col_start, _) = meta.row_group(0).column(0).byte_range();
        let fail_at = (col_start / BLOCK) * BLOCK;
        let probe = ProbeStore::new(inner, Duration::ZERO, Some(fail_at));
        let (store, cache, _p, _s) = wrap(
            Arc::clone(&probe) as Arc<dyn ObjectStore>,
            path.clone(),
            size,
        );

        let spec = parquet_spec(
            Arc::clone(&store),
            path.clone(),
            size,
            vec![0, 2],
            vec![],
            Arc::clone(&schema),
            None,
            true,
        );
        let ranges = compute_ranges(&spec, &spec.files[0]).await.unwrap();
        run(spec, PrefetchHandle::new()).await;
        assert!(
            cache.stats().prefetch_errors >= 1,
            "the injected upstream failure is counted"
        );

        // The demand read of the failed range succeeds (errors are never cached; it re-fetches).
        let out = store.get_ranges(&path, &ranges).await.unwrap();
        assert_eq!(out.len(), ranges.len());
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
