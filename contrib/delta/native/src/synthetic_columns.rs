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

//! Delta synthetic column emitters.
//!
//! Delta's `DeltaParquetFileFormat` injects two parquet-virtual columns into the scan
//! output for UPDATE/DELETE/MERGE flows:
//!
//!   - `__delta_internal_row_index` (Long): the physical row position within the file
//!   - `__delta_internal_is_row_deleted` (Int): 0 = keep, nonzero = drop (from the DV)
//!
//! Delta's reader synthesizes these from parquet row positions + the DV bitmap. Comet's
//! native parquet path (DataFusion 53) doesn't expose virtual row-index columns; this
//! module provides equivalent synthesis as small `ExecutionPlan` wrappers that sit
//! between the inner parquet scan and the rest of the plan.
//!
//! Same physical-order invariant as `DeltaDvFilterExec` — these execs rely on one file
//! per partition and the parquet scan emitting rows in file row order. Both
//! `maintains_input_order() = [true]` and `benefits_from_input_partitioning() = [false]`
//! are overridden to pin the contract so future optimizer rewrites are forced to bail
//! rather than silently re-order rows out from under the row-index emit.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Int32Array, Int64Array, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

/// Delta's internal name for the row-index column.
pub const ROW_INDEX_COLUMN_NAME: &str = "__delta_internal_row_index";
/// Delta's internal name for the is-row-deleted column.
pub const IS_ROW_DELETED_COLUMN_NAME: &str = "__delta_internal_is_row_deleted";
/// Delta's logical row-id column. Synthesised as `baseRowId + physical_row_index`.
pub const ROW_ID_COLUMN_NAME: &str = "row_id";
/// Delta's logical row-commit-version column. Constant per file = `defaultRowCommitVersion`.
pub const ROW_COMMIT_VERSION_COLUMN_NAME: &str = "row_commit_version";

/// Build an output schema = input fields + the appended synthetic columns. Order is
/// fixed: row_index, is_row_deleted, row_id, row_commit_version. Scala-side caller
/// asserts these are a suffix of `scan.requiredSchema` in the same order so the proto
/// layout aligns with what Spark expects.
fn build_output_schema(
    input: &SchemaRef,
    emit_row_index: bool,
    emit_is_row_deleted: bool,
    emit_row_id: bool,
    emit_row_commit_version: bool,
    row_index_column_name: &str,
) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = input.fields().iter().cloned().collect();
    if emit_row_index {
        fields.push(Arc::new(Field::new(row_index_column_name, DataType::UInt64, false)));
    }
    if emit_is_row_deleted {
        fields.push(Arc::new(Field::new(
            IS_ROW_DELETED_COLUMN_NAME,
            DataType::Int32,
            false,
        )));
    }
    if emit_row_id {
        fields.push(Arc::new(Field::new(ROW_ID_COLUMN_NAME, DataType::Int64, true)));
    }
    if emit_row_commit_version {
        fields.push(Arc::new(Field::new(
            ROW_COMMIT_VERSION_COLUMN_NAME,
            DataType::Int64,
            true,
        )));
    }
    Arc::new(Schema::new(fields))
}

/// `ExecutionPlan` wrapper that appends Delta's synthetic `__delta_internal_row_index`
/// (UInt64) and/or `__delta_internal_is_row_deleted` (Int32) columns to its child's
/// output batches.
///
/// `deleted_row_indexes_by_partition[i]` is the sorted DV for partition `i`. When
/// `emit_is_row_deleted` is true, each row's is-deleted column is computed by checking
/// membership in this list. When `emit_row_index` is true, each row's row_index column
/// is set to its physical position within the file (running offset across batches).
///
/// Unlike `DeltaDvFilterExec`, this exec does NOT filter rows — it surfaces the
/// information for an outer operator (e.g. Delta's MERGE/UPDATE writer) to decide what
/// to do.
#[derive(Debug)]
pub struct DeltaSyntheticColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    /// One entry per output partition. Length must match the input's partition count.
    /// Empty vec means no DV for that partition (all rows are kept).
    deleted_row_indexes_by_partition: Vec<Vec<u64>>,
    /// `AddFile.baseRowId` per partition; `None` when the table doesn't have row
    /// tracking enabled for this file. Required to be present (Some(_)) on every
    /// partition when `emit_row_id` is true.
    base_row_ids_by_partition: Vec<Option<i64>>,
    /// `AddFile.defaultRowCommitVersion` per partition; same semantics as
    /// `base_row_ids_by_partition` but for `emit_row_commit_version`.
    default_row_commit_versions_by_partition: Vec<Option<i64>>,
    emit_row_index: bool,
    emit_is_row_deleted: bool,
    emit_row_id: bool,
    emit_row_commit_version: bool,
    /// Column name to emit for the row_index synthetic. Stored so with_new_children
    /// can reconstruct correctly. Defaults to ROW_INDEX_COLUMN_NAME but DV-aware
    /// Delta plans may use `_tmp_metadata_row_index` instead.
    row_index_column_name: String,
    output_schema: SchemaRef,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaSyntheticColumnsExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_row_indexes_by_partition: Vec<Vec<u64>>,
        base_row_ids_by_partition: Vec<Option<i64>>,
        default_row_commit_versions_by_partition: Vec<Option<i64>>,
        emit_row_index: bool,
        emit_is_row_deleted: bool,
        emit_row_id: bool,
        emit_row_commit_version: bool,
        row_index_column_name: &str,
    ) -> DFResult<Self> {
        if !emit_row_index && !emit_is_row_deleted && !emit_row_id && !emit_row_commit_version {
            return Err(DataFusionError::Internal(
                "DeltaSyntheticColumnsExec constructed with nothing to emit".to_string(),
            ));
        }
        let input_props = input.properties();
        let num_partitions = input_props.output_partitioning().partition_count();
        if deleted_row_indexes_by_partition.len() != num_partitions
            || base_row_ids_by_partition.len() != num_partitions
            || default_row_commit_versions_by_partition.len() != num_partitions
        {
            return Err(DataFusionError::Internal(format!(
                "DeltaSyntheticColumnsExec: per-partition vec lengths don't match input partitions \
                 ({}): dv={}, base_row_ids={}, default_commit_versions={}",
                num_partitions,
                deleted_row_indexes_by_partition.len(),
                base_row_ids_by_partition.len(),
                default_row_commit_versions_by_partition.len()
            )));
        }
        let output_schema = build_output_schema(
            &input.schema(),
            emit_row_index,
            emit_is_row_deleted,
            emit_row_id,
            emit_row_commit_version,
            row_index_column_name,
        );
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            input_props.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            deleted_row_indexes_by_partition,
            base_row_ids_by_partition,
            default_row_commit_versions_by_partition,
            emit_row_index,
            emit_is_row_deleted,
            emit_row_id,
            emit_row_commit_version,
            row_index_column_name: row_index_column_name.to_string(),
            output_schema,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for DeltaSyntheticColumnsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DeltaSyntheticColumnsExec: row_index={}, is_row_deleted={}, row_id={}, row_commit_version={}",
            self.emit_row_index,
            self.emit_is_row_deleted,
            self.emit_row_id,
            self.emit_row_commit_version
        )
    }
}

impl ExecutionPlan for DeltaSyntheticColumnsExec {
    fn name(&self) -> &str {
        "DeltaSyntheticColumnsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    // Same physical-order invariant as DeltaDvFilterExec.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "DeltaSyntheticColumnsExec takes exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.deleted_row_indexes_by_partition.clone(),
            self.base_row_ids_by_partition.clone(),
            self.default_row_commit_versions_by_partition.clone(),
            self.emit_row_index,
            self.emit_is_row_deleted,
            self.emit_row_id,
            self.emit_row_commit_version,
            &self.row_index_column_name,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, context)?;
        let deleted = self
            .deleted_row_indexes_by_partition
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let base_row_id = self.base_row_ids_by_partition.get(partition).copied().flatten();
        let default_row_commit_version = self
            .default_row_commit_versions_by_partition
            .get(partition)
            .copied()
            .flatten();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(DeltaSyntheticColumnsStream {
            inner: child_stream,
            deleted,
            current_row_offset: 0,
            next_delete_idx: 0,
            output_schema: Arc::clone(&self.output_schema),
            emit_row_index: self.emit_row_index,
            emit_is_row_deleted: self.emit_is_row_deleted,
            emit_row_id: self.emit_row_id,
            emit_row_commit_version: self.emit_row_commit_version,
            base_row_id,
            default_row_commit_version,
            baseline_metrics: baseline,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct DeltaSyntheticColumnsStream {
    inner: SendableRecordBatchStream,
    deleted: Vec<u64>,
    current_row_offset: u64,
    next_delete_idx: usize,
    output_schema: SchemaRef,
    emit_row_index: bool,
    emit_is_row_deleted: bool,
    emit_row_id: bool,
    emit_row_commit_version: bool,
    base_row_id: Option<i64>,
    default_row_commit_version: Option<i64>,
    baseline_metrics: BaselineMetrics,
}

impl DeltaSyntheticColumnsStream {
    fn augment(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let batch_rows = batch.num_rows() as u64;
        let batch_start = self.current_row_offset;
        let batch_end = batch_start + batch_rows;

        // Build the row_index column: monotonically increasing UInt64 starting at
        // batch_start.
        let row_index_array: Option<UInt64Array> = if self.emit_row_index {
            Some(UInt64Array::from_iter_values(batch_start..batch_end))
        } else {
            None
        };

        // Build the is_row_deleted column: walk the deleted indexes alongside the batch
        // row range, advancing `next_delete_idx` as we go. Both arrays share the same
        // O(rows + deletes) sweep; allocation is one Int32Array of length batch_rows.
        let is_deleted_array: Option<Int32Array> = if self.emit_is_row_deleted {
            let mut values = vec![0i32; batch_rows as usize];
            // Skip deleted entries that fall before this batch.
            while self.next_delete_idx < self.deleted.len()
                && self.deleted[self.next_delete_idx] < batch_start
            {
                self.next_delete_idx += 1;
            }
            // Mark every deleted index within [batch_start, batch_end). Advance
            // `self.next_delete_idx` past them so the next batch's skip-before-start
            // loop is O(1) instead of re-walking the entire prior batch.
            let mut idx = self.next_delete_idx;
            while idx < self.deleted.len() && self.deleted[idx] < batch_end {
                let local = (self.deleted[idx] - batch_start) as usize;
                values[local] = 1;
                idx += 1;
            }
            self.next_delete_idx = idx;
            Some(Int32Array::from(values))
        } else {
            None
        };

        // row_id: baseRowId + physical row index. Nullable because tables without row
        // tracking won't have baseRowId; in that case we emit a null-valued column so the
        // schema still matches.
        let row_id_array: Option<Int64Array> = if self.emit_row_id {
            match self.base_row_id {
                Some(base) => {
                    let values: Vec<i64> = (batch_start..batch_end)
                        .map(|idx| base.saturating_add(idx as i64))
                        .collect();
                    Some(Int64Array::from(values))
                }
                None => Some(Int64Array::from(vec![None as Option<i64>; batch_rows as usize])),
            }
        } else {
            None
        };

        // row_commit_version: defaultRowCommitVersion (constant per file). Same nullable
        // semantics as row_id.
        let row_commit_version_array: Option<Int64Array> = if self.emit_row_commit_version {
            match self.default_row_commit_version {
                Some(v) => Some(Int64Array::from(vec![v; batch_rows as usize])),
                None => Some(Int64Array::from(vec![None as Option<i64>; batch_rows as usize])),
            }
        } else {
            None
        };

        self.current_row_offset = batch_end;

        // Append synthetic columns to the batch. Order matches build_output_schema:
        // row_index, is_row_deleted, row_id, row_commit_version.
        let mut columns: Vec<Arc<dyn arrow::array::Array>> = batch.columns().to_vec();
        if let Some(arr) = row_index_array {
            columns.push(Arc::new(arr));
        }
        if let Some(arr) = is_deleted_array {
            columns.push(Arc::new(arr));
        }
        if let Some(arr) = row_id_array {
            columns.push(Arc::new(arr));
        }
        if let Some(arr) = row_commit_version_array {
            columns.push(Arc::new(arr));
        }
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|e| {
            DataFusionError::Internal(format!(
                "DeltaSyntheticColumnsExec: failed to append synthetic columns: {e}"
            ))
        })
    }
}

impl Stream for DeltaSyntheticColumnsStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        let result = match poll {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.augment(batch))),
            other => other,
        };
        self.baseline_metrics.record_poll(result)
    }
}

impl RecordBatchStream for DeltaSyntheticColumnsStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, ArrayRef, Int64Array};

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]))
    }

    fn batch(rows: &[i64]) -> RecordBatch {
        let arr: ArrayRef = Arc::new(Int64Array::from(rows.to_vec()));
        RecordBatch::try_new(input_schema(), vec![arr]).unwrap()
    }

    /// Helper: build a `DeltaSyntheticColumnsStream` directly, without an exec, so we
    /// can drive `augment()` in isolation. Mirrors the real construction path.
    fn make_stream(
        emit_row_index: bool,
        emit_is_row_deleted: bool,
        emit_row_id: bool,
        emit_row_commit_version: bool,
        deleted: Vec<u64>,
        base_row_id: Option<i64>,
        default_row_commit_version: Option<i64>,
    ) -> DeltaSyntheticColumnsStream {
        let schema = build_output_schema(
            &input_schema(),
            emit_row_index,
            emit_is_row_deleted,
            emit_row_id,
            emit_row_commit_version,
            ROW_INDEX_COLUMN_NAME,
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics, 0);
        let (_tx, rx) = futures::channel::mpsc::unbounded::<DFResult<RecordBatch>>();
        let inner: SendableRecordBatchStream = Box::pin(EmptyStream {
            schema: input_schema(),
            inner: rx,
        });
        DeltaSyntheticColumnsStream {
            inner,
            deleted,
            current_row_offset: 0,
            next_delete_idx: 0,
            output_schema: schema,
            emit_row_index,
            emit_is_row_deleted,
            emit_row_id,
            emit_row_commit_version,
            base_row_id,
            default_row_commit_version,
            baseline_metrics: baseline,
        }
    }

    struct EmptyStream {
        schema: SchemaRef,
        inner: futures::channel::mpsc::UnboundedReceiver<DFResult<RecordBatch>>,
    }
    impl Stream for EmptyStream {
        type Item = DFResult<RecordBatch>;
        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }
    impl RecordBatchStream for EmptyStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    // ---- build_output_schema combinations ----

    #[test]
    fn schema_only_row_index() {
        let s = build_output_schema(&input_schema(), true, false, false, false);
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["v", ROW_INDEX_COLUMN_NAME]);
    }

    #[test]
    fn schema_all_four_in_order() {
        let s = build_output_schema(&input_schema(), true, true, true, true);
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "v",
                ROW_INDEX_COLUMN_NAME,
                IS_ROW_DELETED_COLUMN_NAME,
                ROW_ID_COLUMN_NAME,
                ROW_COMMIT_VERSION_COLUMN_NAME,
            ]
        );
        // Nullability: row_id and row_commit_version are nullable; the other two are not.
        let nullables: Vec<bool> = s.fields().iter().map(|f| f.is_nullable()).collect();
        assert_eq!(nullables, vec![false, false, false, true, true]);
    }

    #[test]
    fn schema_emit_subset_preserves_order() {
        // Skip row_index, keep is_row_deleted and row_commit_version -> appended in that order.
        let s = build_output_schema(&input_schema(), false, true, false, true);
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec!["v", IS_ROW_DELETED_COLUMN_NAME, ROW_COMMIT_VERSION_COLUMN_NAME]
        );
    }

    // ---- augment correctness ----

    #[test]
    fn augment_row_index_single_batch() {
        let mut s = make_stream(true, false, false, false, vec![], None, None);
        let out = s.augment(batch(&[10, 20, 30])).unwrap();
        let idx = out
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let vals: Vec<u64> = idx.iter().map(Option::unwrap).collect();
        assert_eq!(vals, vec![0, 1, 2]);
    }

    #[test]
    fn augment_row_index_multi_batch_monotonic() {
        let mut s = make_stream(true, false, false, false, vec![], None, None);
        let out1 = s.augment(batch(&[1, 2, 3])).unwrap();
        let out2 = s.augment(batch(&[4, 5])).unwrap();
        let idx1: Vec<u64> = out1
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        let idx2: Vec<u64> = out2
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(idx1, vec![0, 1, 2]);
        assert_eq!(idx2, vec![3, 4]);
    }

    #[test]
    fn augment_is_row_deleted_marks_correct_indexes() {
        let mut s = make_stream(false, true, false, false, vec![1, 3], None, None);
        let out = s.augment(batch(&[10, 20, 30, 40, 50])).unwrap();
        let flags = out
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let vals: Vec<i32> = flags.iter().map(Option::unwrap).collect();
        assert_eq!(vals, vec![0, 1, 0, 1, 0]);
    }

    #[test]
    fn augment_is_row_deleted_writes_back_next_delete_idx() {
        // After one batch consuming deletes 0,1,2, the second batch should start with
        // next_delete_idx already past them — verifying the writeback fix.
        let mut s = make_stream(false, true, false, false, vec![0, 1, 2, 7], None, None);
        let _ = s.augment(batch(&[10, 20, 30])).unwrap(); // 0,1,2 deleted, next_delete_idx -> 3
        assert_eq!(s.next_delete_idx, 3);
        let out2 = s.augment(batch(&[40, 50, 60, 70, 80])).unwrap(); // covers 3..8; 7 deleted
        let flags: Vec<i32> = out2
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(flags, vec![0, 0, 0, 0, 1]);
        assert_eq!(s.next_delete_idx, 4);
    }

    #[test]
    fn augment_row_id_with_base() {
        let mut s = make_stream(false, false, true, false, vec![], Some(1000), None);
        let out = s.augment(batch(&[10, 20, 30])).unwrap();
        let ids = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals: Vec<i64> = ids.iter().map(Option::unwrap).collect();
        assert_eq!(vals, vec![1000, 1001, 1002]);

        // Second batch: row_id continues from where current_row_offset left off.
        let out2 = s.augment(batch(&[40])).unwrap();
        let v2: Vec<i64> = out2
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(v2, vec![1003]);
    }

    #[test]
    fn augment_row_id_without_base_emits_nulls() {
        let mut s = make_stream(false, false, true, false, vec![], None, None);
        let out = s.augment(batch(&[10, 20])).unwrap();
        let ids = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.null_count(), 2);
    }

    #[test]
    fn augment_row_commit_version_constant() {
        let mut s = make_stream(false, false, false, true, vec![], None, Some(7));
        let out = s.augment(batch(&[10, 20, 30])).unwrap();
        let v = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals: Vec<i64> = v.iter().map(Option::unwrap).collect();
        assert_eq!(vals, vec![7, 7, 7]);
    }

    #[test]
    fn augment_row_commit_version_without_default_emits_nulls() {
        let mut s = make_stream(false, false, false, true, vec![], None, None);
        let out = s.augment(batch(&[1, 2])).unwrap();
        let v = out
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(v.null_count(), 2);
    }

    #[test]
    fn augment_all_four_columns_combined() {
        let mut s = make_stream(true, true, true, true, vec![1], Some(500), Some(42));
        let out = s.augment(batch(&[10, 20, 30])).unwrap();
        assert_eq!(out.schema().fields().len(), 5);

        // col 0: data
        // col 1: row_index 0,1,2
        let ri: Vec<u64> = out
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(ri, vec![0, 1, 2]);
        // col 2: is_row_deleted 0,1,0
        let dl: Vec<i32> = out
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(dl, vec![0, 1, 0]);
        // col 3: row_id 500,501,502
        let id: Vec<i64> = out
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(id, vec![500, 501, 502]);
        // col 4: row_commit_version 42,42,42
        let cv: Vec<i64> = out
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(cv, vec![42, 42, 42]);
    }

    #[test]
    fn augment_empty_batch_preserves_schema() {
        let mut s = make_stream(true, true, true, true, vec![], Some(0), Some(0));
        let out = s.augment(batch(&[])).unwrap();
        assert_eq!(out.schema().fields().len(), 5);
        assert_eq!(out.num_rows(), 0);
    }

    #[test]
    fn new_validates_partition_count_mismatch() {
        use datafusion::physical_plan::empty::EmptyExec;
        let inner = Arc::new(EmptyExec::new(input_schema())) as Arc<dyn ExecutionPlan>;
        // EmptyExec has 1 partition; pass 2 entries.
        let err = DeltaSyntheticColumnsExec::new(
            inner,
            vec![vec![], vec![]],
            vec![None, None],
            vec![None, None],
            true, false, false, false,
        )
        .unwrap_err();
        assert!(format!("{err}").contains("partition count mismatch") || format!("{err}").contains("partitions"));
    }
}
