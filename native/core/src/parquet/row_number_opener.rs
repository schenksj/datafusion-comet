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

//! A focused `FileSource` / `FileOpener` decorator that emits the parquet
//! reader's **RowNumber virtual column** alongside the projected real columns.
//!
//! # Why this exists
//!
//! Delta Lake row tracking synthesizes `_metadata.row_id = baseRowId +
//! physical_row_index`. Comet's native Delta scan normally computes the
//! physical row index from a running sequential counter
//! (`current_row_offset` in `synthetic_columns.rs`). That counter is only
//! correct if the parquet reader returns **every** row in physical order, so
//! the Delta glue disables parquet data-filter pushdown whenever a
//! position-derived synthetic (row_index / row_id / is_row_deleted) is
//! emitted. Disabling pushdown kills row-group skipping when a selective
//! filter is combined with a projected `_metadata.row_id`.
//!
//! parquet 58.1 exposes a `RowNumber` virtual column that reports each row's
//! TRUE physical position even under row-group pruning / row filtering. By
//! emitting it we can keep filter pushdown ON and feed the true positions into
//! the synthetic-column synthesis.
//!
//! # Why a custom opener (and not `ParquetSource`)
//!
//! DataFusion 53.1.0's `ParquetSource` / `ParquetOpener` does NOT expose
//! virtual columns: its `expr_adapter_factory` is `pub(crate)` and it never
//! calls `builder.with_virtual_columns(..)`. Until DataFusion exposes virtual
//! columns on `ParquetSource` we wrap the inner `ParquetSource`'s `FileSource`
//! with this decorator and substitute a custom `FileOpener` that mirrors the
//! relevant slice of `ParquetOpener` (metadata load, row-filter pushdown,
//! projection mask) while additionally registering the `RowNumber` virtual
//! column.
//!
//! Upstream feature request: <https://github.com/apache/datafusion/issues/22517>.
//! Once that lands this whole module can be removed and the Delta glue can ask
//! `ParquetSource` for the virtual column directly.
//!
//! # Contract
//!
//! The opener produces batches whose columns are: the projected REAL columns
//! (in projection order) followed by a single appended `Int64` column named
//! [`ROW_NUMBER_COLUMN_NAME`] carrying the true physical row position.
//! `DeltaSyntheticColumnsExec` detects that trailing column by name, uses it
//! to compute the synthetics, and drops it from its output.

use std::any::Any;
use std::fmt::{self, Formatter};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::common::Result;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    build_row_filter, ParquetFileMetrics, ParquetFileReaderFactory,
};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::table_schema::TableSchema;
use futures::{StreamExt, TryStreamExt};
use log::debug;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask, RowNumber};

/// Name of the appended parquet RowNumber virtual column. Must match the name
/// `DeltaSyntheticColumnsExec` looks for to detect the true-row-number path.
pub const ROW_NUMBER_COLUMN_NAME: &str = "__comet_delta_row_number";

/// Build the `Field` describing the RowNumber virtual column. The parquet
/// reader requires it to be `Int64`, non-nullable, and tagged with the
/// `RowNumber` extension type (so `is_virtual_column` recognises it).
fn row_number_field() -> Arc<Field> {
    Arc::new(Field::new(ROW_NUMBER_COLUMN_NAME, DataType::Int64, false).with_extension_type(RowNumber))
}

/// `FileOpener` that opens a single parquet file emitting the projected real
/// columns plus an appended `RowNumber` Int64 column. See the module docs for
/// the rationale and DataFusion #22517.
pub struct RowNumberParquetOpener {
    /// Reader factory (the Comet caching factory) used to create the
    /// `AsyncFileReader` for each file.
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Physical file schema (the table's file schema; this is the schema
    /// `build_row_filter` and the projection mask are computed against).
    physical_file_schema: SchemaRef,
    /// Indices (into `physical_file_schema`) of the REAL columns to project,
    /// in projection order. Excludes the virtual row_number column.
    real_column_indices: Vec<usize>,
    /// Optional predicate to push into the parquet reader as a `RowFilter`.
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Target batch size.
    batch_size: usize,
    /// Whether to let `build_row_filter` reorder the predicate conjuncts.
    reorder_filters: bool,
    /// Execution partition index (for metrics labelling).
    partition_index: usize,
    /// Metrics set shared with the surrounding plan.
    metrics: ExecutionPlanMetricsSet,
}

impl FileOpener for RowNumberParquetOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let reader_factory = Arc::clone(&self.reader_factory);
        let physical_file_schema = Arc::clone(&self.physical_file_schema);
        let real_column_indices = self.real_column_indices.clone();
        let predicate = self.predicate.clone();
        let batch_size = self.batch_size;
        let reorder_filters = self.reorder_filters;
        let partition_index = self.partition_index;
        let metrics = self.metrics.clone();

        // Per-file metrics object (mirrors ParquetOpener::open).
        let file_name = partitioned_file.object_meta.location.to_string();
        let metadata_size_hint = partitioned_file.metadata_size_hint;

        // Obtain the AsyncFileReader for this file from the reader factory. Note
        // the binding type is `Box<dyn AsyncFileReader>` (no `+ Send`): parquet
        // only impls `AsyncFileReader` for `Box<dyn AsyncFileReader + '_>`, so we
        // unsize the `+ Send` box the factory returns down to that. This mirrors
        // DataFusion's own `ParquetOpener::open`. The future stays `Send` because
        // the underlying reader is moved into it before the box's `Send` is erased.
        let async_file_reader: Box<dyn AsyncFileReader> = reader_factory.create_reader(
            partition_index,
            partitioned_file,
            metadata_size_hint,
            &metrics,
        )?;

        Ok(Box::pin(async move {
            let file_metrics = ParquetFileMetrics::new(partition_index, &file_name, &metrics);

            // Register the RowNumber virtual column via ArrowReaderOptions (the
            // builder reads it from the options at construction). parquet appends
            // it as the last output column (Int64) carrying the true physical row
            // index, even under row-group pruning / row filtering. In parquet
            // 58.1 `with_virtual_columns` lives on `ArrowReaderOptions`, not the
            // builder -- see DataFusion #22517 for the upstream gap this works around.
            let options = ArrowReaderOptions::new()
                .with_virtual_columns(vec![row_number_field()])?;

            // Load parquet metadata and build the stream builder with those options.
            let mut builder =
                ParquetRecordBatchStreamBuilder::new_with_options(async_file_reader, options)
                    .await?;

            // Filter pushdown: build a parquet `RowFilter` from the predicate so
            // matching is done during decode and non-matching rows / row groups
            // can be skipped. The RowNumber virtual column still reports the TRUE
            // physical position of every emitted row, so the synthetic columns
            // stay correct under pruning. We deliberately keep pushdown ON here
            // (unlike the running-counter path in core_glue.rs).
            if let Some(predicate) = predicate.as_ref() {
                match build_row_filter(
                    predicate,
                    &physical_file_schema,
                    builder.metadata(),
                    reorder_filters,
                    &file_metrics,
                ) {
                    Ok(Some(filter)) => {
                        builder = builder.with_row_filter(filter);
                    }
                    Ok(None) => {}
                    Err(e) => {
                        debug!(
                            "RowNumberParquetOpener: ignoring error building row filter \
                             for '{predicate:?}': {e}"
                        );
                    }
                }
            }

            // Project only the REAL columns via the ProjectionMask. The virtual
            // row_number column is supplied separately by `with_virtual_columns`
            // and is appended to the output AFTER the masked real columns.
            let mask =
                ProjectionMask::roots(builder.parquet_schema(), real_column_indices.iter().copied());

            let stream = builder
                .with_projection(mask)
                .with_batch_size(batch_size)
                .build()?;

            // Map parquet errors into DataFusion errors and box the stream.
            let stream = stream
                .map_err(|e| datafusion::error::DataFusionError::from(e))
                .boxed();
            Ok(stream)
        }))
    }
}

/// `FileSource` decorator that wraps an inner `ParquetSource`-backed
/// `FileSource` and substitutes a [`RowNumberParquetOpener`] for the parquet
/// opener so the scan emits a trailing `RowNumber` Int64 column. All other
/// `FileSource` trait methods delegate to the inner source verbatim.
///
/// See the module docs and DataFusion #22517: this whole decorator becomes
/// unnecessary once `ParquetSource` exposes virtual columns directly.
#[derive(Clone)]
pub struct RowNumberFileSource {
    inner: Arc<dyn FileSource>,
    /// Reader factory captured at construction (same factory the inner
    /// `ParquetSource` was configured with). Stored explicitly because the
    /// `FileSource` trait does not expose the inner reader factory and
    /// `ParquetSource`'s accessor is `pub(crate)`.
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    /// Whether to let `build_row_filter` reorder predicate conjuncts. Captured
    /// at construction because `ParquetSource::reorder_filters()` is
    /// `pub(crate)`. Comet always configures this `true` (see
    /// `parquet_exec::get_options`).
    reorder_filters: bool,
    /// Batch size. `None` until DataFusion calls `with_batch_size`. The
    /// `ParquetOpener` panics if it's missing, so we mirror that contract.
    batch_size: Option<usize>,
}

impl RowNumberFileSource {
    /// Wrap `inner` so its parquet opener emits the RowNumber virtual column.
    /// `reader_factory` must be the same factory the inner `ParquetSource` was
    /// configured with.
    pub fn new(
        inner: Arc<dyn FileSource>,
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        reorder_filters: bool,
    ) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner,
            reader_factory,
            reorder_filters,
            batch_size: None,
        })
    }

    /// Re-wrap a (possibly transformed) inner source, preserving the captured
    /// reader factory / reorder flag / batch size.
    fn rewrap(&self, inner: Arc<dyn FileSource>) -> Arc<dyn FileSource> {
        Arc::new(Self {
            inner,
            reader_factory: Arc::clone(&self.reader_factory),
            reorder_filters: self.reorder_filters,
            batch_size: self.batch_size,
        })
    }
}

impl FileSource for RowNumberFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        // Extract projection / predicate / physical-file schema the same way
        // `ParquetSource::create_file_opener` does, but reading from the
        // `FileSource` trait surface (the only public access).
        //
        // The projection column indices are over the table schema's file
        // schema. The LAST projected index is the virtual row_number column
        // (added in `parquet_exec::init_datasource_exec`); exclude it so the
        // parquet `ProjectionMask` covers only the real columns. The row_number
        // column is supplied to the reader via `with_virtual_columns`.
        let table_schema = self.inner.table_schema();
        let physical_file_schema = Arc::clone(table_schema.file_schema());

        let mut real_column_indices: Vec<usize> = self
            .inner
            .projection()
            .map(|p| p.column_indices())
            .unwrap_or_else(|| (0..physical_file_schema.fields().len()).collect());

        // Drop the trailing virtual row_number index. The row_number field is
        // the last field of the file schema (we appended it in
        // init_datasource_exec) and it is always projected last.
        let row_number_idx = physical_file_schema.fields().len().saturating_sub(1);
        real_column_indices.retain(|&i| i != row_number_idx);

        let predicate = self.inner.filter();

        let batch_size = self
            .batch_size
            .expect("Batch size must be set before creating RowNumberParquetOpener");

        Ok(Arc::new(RowNumberParquetOpener {
            reader_factory: Arc::clone(&self.reader_factory),
            physical_file_schema,
            real_column_indices,
            predicate,
            batch_size,
            reorder_filters: self.reorder_filters,
            partition_index: partition,
            metrics: self.inner.metrics().clone(),
        }))
    }

    fn as_any(&self) -> &dyn Any {
        // Delegate to the inner source so DataFusion optimizations that downcast
        // a `FileSource` to `ParquetSource` continue to see through this
        // decorator (mirrors `IgnoreMissingFileSource`). Nothing downcasts to
        // `RowNumberFileSource` itself; the row-number behaviour rides on this
        // wrapper's trait methods.
        self.inner.as_any()
    }

    fn table_schema(&self) -> &TableSchema {
        self.inner.table_schema()
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        // Capture the batch size AND propagate it to the inner source so a
        // later `as_any()`-based downcast still observes a configured source.
        Arc::new(Self {
            inner: self.inner.with_batch_size(batch_size),
            reader_factory: Arc::clone(&self.reader_factory),
            reorder_filters: self.reorder_filters,
            batch_size: Some(batch_size),
        })
    }

    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.inner.filter()
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        self.inner.projection()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        self.inner.metrics()
    }

    fn file_type(&self) -> &str {
        self.inner.file_type()
    }

    fn fmt_extra(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        self.inner.fmt_extra(t, f)
    }

    fn supports_repartitioning(&self) -> bool {
        self.inner.supports_repartitioning()
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
        config: &FileScanConfig,
    ) -> Result<Option<FileScanConfig>> {
        self.inner.repartitioned(
            target_partitions,
            repartition_file_min_size,
            output_ordering,
            config,
        )
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        config: &datafusion::config::ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>> {
        let prop = self.inner.try_pushdown_filters(filters, config)?;
        Ok(FilterPushdownPropagation {
            filters: prop.filters,
            updated_node: prop.updated_node.map(|n| self.rewrap(n)),
        })
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>> {
        match self.inner.try_pushdown_sort(order, eq_properties)? {
            SortOrderPushdownResult::Exact { inner } => Ok(SortOrderPushdownResult::Exact {
                inner: self.rewrap(inner),
            }),
            SortOrderPushdownResult::Inexact { inner } => Ok(SortOrderPushdownResult::Inexact {
                inner: self.rewrap(inner),
            }),
            SortOrderPushdownResult::Unsupported => Ok(SortOrderPushdownResult::Unsupported),
        }
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        Ok(self
            .inner
            .try_pushdown_projection(projection)?
            .map(|n| self.rewrap(n)))
    }
}
