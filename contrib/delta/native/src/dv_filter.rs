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

//! Delta Lake deletion-vector filter operator.
//!
//! Wraps a child `ExecutionPlan` (produced by `init_datasource_exec` over the
//! list of Delta parquet files) and applies Delta deletion vectors at the
//! batch level. One `Vec<u64>` of deleted row indexes per partition drives
//! the filter.
//!
//! Design notes:
//!
//!   - **One file per partition.** The planner match arm places each DV'd
//!     file in its own `FileGroup`, so when this operator sees partition
//!     `i`, it knows the full set of rows that `ParquetSource` is going to
//!     emit for that partition is exactly the physical rows of one file
//!     in physical order. That's the only assumption we rely on for the
//!     "subtract deleted indexes by tracking a running row offset" strategy
//!     to be correct.
//!
//!   - **Indexes are pre-materialized.** Kernel already turned the DV
//!     (inline bitmap / on-disk file / UUID reference) into a sorted
//!     `Vec<u64>` on the driver via `DvInfo::get_row_indexes`. That's what
//!     `plan_delta_scan` returns. We don't touch DV bytes on the executor
//!     side at all.
//!
//!   - **Filter uses arrow `filter_record_batch`.** Builds a per-batch
//!     `BooleanArray` mask where `true` means "keep". One mask per batch,
//!     allocated fresh — the batch sizes are small and allocation overhead
//!     is negligible compared with decoding parquet.

use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{Stream, StreamExt};

/// Execution-plan wrapper that applies per-partition deletion-vector filters
/// to the output of a child parquet scan.
///
/// `deleted_row_indexes_by_partition[i]` is the sorted list of physical row
/// indexes to drop from partition `i`'s output. An empty vec means "no DV
/// for this partition — pass through untouched".
#[derive(Debug)]
pub struct DeltaDvFilterExec {
    input: Arc<dyn ExecutionPlan>,
    /// One entry per output partition. Length must match the input's
    /// partition count.
    deleted_row_indexes_by_partition: Vec<Vec<u64>>,
    plan_properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DeltaDvFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        deleted_row_indexes_by_partition: Vec<Vec<u64>>,
    ) -> DFResult<Self> {
        let input_props = input.properties();
        let num_partitions = input_props.output_partitioning().partition_count();
        if deleted_row_indexes_by_partition.len() != num_partitions {
            return Err(DataFusionError::Internal(format!(
                "DeltaDvFilterExec: got {} DV entries for {} partitions",
                deleted_row_indexes_by_partition.len(),
                num_partitions
            )));
        }
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input_props.output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            deleted_row_indexes_by_partition,
            plan_properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for DeltaDvFilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let total_dv: usize = self
            .deleted_row_indexes_by_partition
            .iter()
            .map(|v| v.len())
            .sum();
        let dv_partitions = self
            .deleted_row_indexes_by_partition
            .iter()
            .filter(|v| !v.is_empty())
            .count();
        write!(
            f,
            "DeltaDvFilterExec: {dv_partitions} partitions with DVs, \
             {total_dv} total deleted rows"
        )
    }
}

impl ExecutionPlan for DeltaDvFilterExec {
    fn name(&self) -> &str {
        "DeltaDvFilterExec"
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

    // DV filtering relies on `current_row_offset` matching the child's physical row
    // index. That invariant only holds if (a) the child preserves its input order and
    // (b) DataFusion doesn't slip in a RepartitionExec / SortPreservingMergeExec that
    // interleaves rows between the parquet scan and this exec. Override both to pin
    // the contract: if either ever stops being true the optimizer is forced to bail
    // rather than silently re-order rows.
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
                "DeltaDvFilterExec takes exactly one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            self.deleted_row_indexes_by_partition.clone(),
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
        let metrics = DeltaDvFilterMetrics::new(&self.metrics, partition);
        metrics.num_deleted.add(deleted.len());
        Ok(Box::pin(DeltaDvFilterStream {
            inner: child_stream,
            deleted,
            current_row_offset: 0,
            next_delete_idx: 0,
            schema: self.input.schema(),
            baseline_metrics: metrics.baseline,
            rows_dropped_metric: metrics.rows_dropped,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct DeltaDvFilterMetrics {
    baseline: BaselineMetrics,
    num_deleted: Count,
    rows_dropped: Count,
}

impl DeltaDvFilterMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            num_deleted: MetricBuilder::new(metrics).counter("dv_rows_scheduled_delete", partition),
            rows_dropped: MetricBuilder::new(metrics).counter("dv_rows_dropped", partition),
        }
    }
}

struct DeltaDvFilterStream {
    inner: SendableRecordBatchStream,
    /// Sorted deleted row indexes for this partition.
    deleted: Vec<u64>,
    /// Physical row offset into the file that the NEXT batch starts at.
    current_row_offset: u64,
    /// Index into `deleted` of the first entry that hasn't been applied yet.
    /// `deleted[..next_delete_idx]` are all strictly less than
    /// `current_row_offset`.
    next_delete_idx: usize,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
    rows_dropped_metric: Count,
}

impl DeltaDvFilterStream {
    /// Drop rows from `batch` whose physical row index is in the DV. Returns
    /// the filtered batch (possibly empty) and advances `current_row_offset`.
    fn apply(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let batch_rows = batch.num_rows() as u64;
        if batch_rows == 0 || self.deleted.is_empty() {
            self.current_row_offset += batch_rows;
            return Ok(batch);
        }

        let batch_start = self.current_row_offset;
        let batch_end = batch_start + batch_rows;

        // Fast-path: if no remaining deletes fall into this batch's row
        // range, pass it through untouched.
        if self.next_delete_idx >= self.deleted.len()
            || self.deleted[self.next_delete_idx] >= batch_end
        {
            self.current_row_offset = batch_end;
            return Ok(batch);
        }

        // Build the keep-mask. Walk forward through `deleted` popping entries
        // that fall inside [batch_start, batch_end).
        let mut mask_buf: Vec<bool> = vec![true; batch_rows as usize];
        let mut dropped: usize = 0;
        // Loop is safe: next_delete_idx < deleted.len() is checked by the while
        // condition, and deleted is sorted ascending by the kernel contract.
        while self.next_delete_idx < self.deleted.len() {
            let d = self.deleted[self.next_delete_idx];
            if d >= batch_end {
                break;
            }
            if d < batch_start {
                return Err(DataFusionError::Internal(format!(
                    "DV index {d} predates batch start {batch_start}"
                )));
            }
            let local = (d - batch_start) as usize;
            if local < mask_buf.len() && mask_buf[local] {
                mask_buf[local] = false;
                dropped += 1;
            }
            self.next_delete_idx += 1;
        }

        self.current_row_offset = batch_end;
        self.rows_dropped_metric.add(dropped);

        if dropped == 0 {
            return Ok(batch);
        }
        let mask = BooleanArray::from(mask_buf);
        filter_record_batch(&batch, &mask).map_err(DataFusionError::from)
    }
}

impl Stream for DeltaDvFilterStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.poll_next_unpin(cx);
        let result = match poll {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(self.apply(batch))),
            other => other,
        };
        self.baseline_metrics.record_poll(result)
    }
}

impl RecordBatchStream for DeltaDvFilterStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use std::sync::Arc as StdArc;

    fn schema() -> SchemaRef {
        StdArc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn batch(rows: &[i64]) -> RecordBatch {
        let arr: ArrayRef = StdArc::new(Int64Array::from(rows.to_vec()));
        RecordBatch::try_new(schema(), vec![arr]).unwrap()
    }

    fn stream_with(deleted: Vec<u64>) -> DeltaDvFilterStream {
        // Construct directly without an inner stream — apply() is the unit under test
        // and inner is never polled in these tests.
        let (_dummy_tx, dummy_rx) = futures::channel::mpsc::unbounded::<DFResult<RecordBatch>>();
        let inner: SendableRecordBatchStream = Box::pin(EmptyStream {
            schema: schema(),
            inner: dummy_rx,
        });
        let metrics_set = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics_set, 0);
        let dropped = MetricBuilder::new(&metrics_set).counter("dv_rows_dropped", 0);
        DeltaDvFilterStream {
            inner,
            deleted,
            current_row_offset: 0,
            next_delete_idx: 0,
            schema: schema(),
            baseline_metrics: baseline,
            rows_dropped_metric: dropped,
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
            StdArc::clone(&self.schema)
        }
    }

    #[test]
    fn apply_empty_batch_passes_through() {
        let mut s = stream_with(vec![1, 3]);
        let out = s.apply(batch(&[])).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(s.current_row_offset, 0);
        assert_eq!(s.next_delete_idx, 0);
    }

    #[test]
    fn apply_no_deletes_is_passthrough() {
        let mut s = stream_with(vec![]);
        let b = batch(&[10, 20, 30, 40]);
        let out = s.apply(b).unwrap();
        assert_eq!(out.num_rows(), 4);
        assert_eq!(s.current_row_offset, 4);
        assert_eq!(s.next_delete_idx, 0);
    }

    #[test]
    fn apply_deletes_in_batch() {
        // Delete rows at indexes 1 and 3 from a 5-row batch -> keep rows 0, 2, 4.
        let mut s = stream_with(vec![1, 3]);
        let b = batch(&[10, 20, 30, 40, 50]);
        let out = s.apply(b).unwrap();
        let arr = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kept: Vec<i64> = arr.iter().map(Option::unwrap).collect();
        assert_eq!(kept, vec![10, 30, 50]);
        assert_eq!(s.current_row_offset, 5);
        assert_eq!(s.next_delete_idx, 2);
    }

    #[test]
    fn apply_delete_at_batch_boundaries() {
        // Delete row 0 (batch_start) and row 4 (batch_end-1) from a 5-row batch.
        let mut s = stream_with(vec![0, 4]);
        let b = batch(&[10, 20, 30, 40, 50]);
        let out = s.apply(b).unwrap();
        let arr = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kept: Vec<i64> = arr.iter().map(Option::unwrap).collect();
        assert_eq!(kept, vec![20, 30, 40]);
    }

    #[test]
    fn apply_multi_batch_with_deletes_spanning_boundary() {
        let mut s = stream_with(vec![1, 5, 7]);
        // First batch: rows 0..4. Deletes index 1 -> keep 10, 30, 40.
        let out1 = s.apply(batch(&[10, 20, 30, 40])).unwrap();
        let kept1: Vec<i64> = out1
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(kept1, vec![10, 30, 40]);
        assert_eq!(s.current_row_offset, 4);
        assert_eq!(s.next_delete_idx, 1);

        // Second batch: rows 4..8. Deletes index 5 and 7 -> keep 50, 70.
        let out2 = s.apply(batch(&[50, 60, 70, 80])).unwrap();
        let kept2: Vec<i64> = out2
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(Option::unwrap)
            .collect();
        assert_eq!(kept2, vec![50, 70]);
        assert_eq!(s.current_row_offset, 8);
        assert_eq!(s.next_delete_idx, 3);
    }

    #[test]
    fn apply_deletes_beyond_batch_pass_through() {
        // All deletes are at indexes 100+ but batch only spans 0..4 -> passthrough.
        let mut s = stream_with(vec![100, 200]);
        let b = batch(&[10, 20, 30, 40]);
        let out = s.apply(b).unwrap();
        assert_eq!(out.num_rows(), 4);
        assert_eq!(s.current_row_offset, 4);
        assert_eq!(s.next_delete_idx, 0);
    }

    #[test]
    fn apply_all_rows_deleted() {
        let mut s = stream_with(vec![0, 1, 2]);
        let b = batch(&[10, 20, 30]);
        let out = s.apply(b).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(s.current_row_offset, 3);
        assert_eq!(s.next_delete_idx, 3);
    }

    #[test]
    fn apply_delete_index_predating_batch_errors() {
        // Pre-set state: we've already consumed up to row 5, but a stale entry
        // in `deleted` claims index 3 should be dropped now. That's a contract
        // violation and we error out rather than silently producing wrong rows.
        let mut s = stream_with(vec![3]);
        s.current_row_offset = 5;
        // next_delete_idx still 0 -> apply will see 3 < 5 = batch_start.
        let err = s.apply(batch(&[100, 200])).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("predates batch start"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn new_validates_partition_count() {
        use datafusion::physical_plan::empty::EmptyExec;
        let inner = StdArc::new(EmptyExec::new(schema())) as Arc<dyn ExecutionPlan>;
        // EmptyExec has 1 partition; passing 2 DV entries must be rejected.
        let err =
            DeltaDvFilterExec::new(inner, vec![vec![1u64], vec![2u64]]).unwrap_err();
        assert!(format!("{err}").contains("got 2 DV entries for 1 partitions"));
    }
}
