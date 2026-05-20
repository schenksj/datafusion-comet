# Native execution: from proto bytes to Arrow batches

## Entry point

When a Spark executor processes its partition, it calls into JNI with the
encoded proto. The relevant symbol is

```
Java_org_apache_comet_Native_planDeltaScan
```

declared in `contrib/delta/native/src/jni.rs`. The function:

1. Decodes the `DeltaScan` proto into `(DeltaScanCommon, Vec<DeltaScanTask>)`
2. Calls `build_plan` in `native/core/src/execution/planner/contrib_delta_scan.rs`
3. Returns a pointer to the `Arc<dyn ExecutionPlan>` to be wired into Comet's
   existing native executor framework

`build_plan` is where the wrapping stack gets assembled. The rest of this
document walks through that stack.

## The wrapping stack

Conceptually:

```
DataSourceExec(ParquetSource over the file list)
    ↓  (optional, if any task has a DV)
DeltaDvFilterExec
    ↓  (optional, if column mapping = name)
ProjectionExec  (physical → logical rename)
    ↓  (optional, if any emit_* flag is set)
DeltaSyntheticColumnsExec
    ↓  (optional, if synthetics are not a suffix of required_schema)
ProjectionExec  (reorder via final_output_indices)
```

Each layer is added only when needed; the simplest case (no DV, no CM, no
synthetics) is just `DataSourceExec`.

### Layer 1: `ParquetSource`

This is DataFusion's existing parquet reader. We build a `FileScanConfig`
from the per-task file lists, passing:

- Partition values as `wrap_partition_value_in_dict` columns
- Pushable filters as `PhysicalExpr` (already translated by the planner)
- `with_field_id(true)` when `common.use_field_id` is set, so the reader
  matches by `PARQUET:field_id` rather than by name

**FileGroup layout**. When any `emit_*` flag is on, every file gets its own
`FileGroup`. This matters because `DeltaSyntheticColumnsExec` maintains a
per-partition row counter and a per-partition DV-walk cursor — both of which
must reset at file boundaries. If two files shared a group, the counter would
keep climbing across the boundary and produce wrong `row_index` / `row_id`
values.

When no synthetics are emitted, files can pack into shared groups for better
parallelism.

### Layer 2: `DeltaDvFilterExec`

If any task in the partition has a non-empty `deleted_row_indexes` (computed
by kernel-rs on the driver from the DV file), we wrap the parquet output with
this filter exec. It:

1. Maintains a `current_row_offset: u64` across batches (assumes
   physical-order input)
2. For each incoming batch, walks the sorted `deleted_row_indexes` and builds
   a `BooleanArray` mask
3. Returns the masked batch (or skips empty batches entirely)

Two safeguards:

- `maintains_input_order() = [true]` — declares to optimisers that we depend
  on input order
- `benefits_from_input_partitioning() = [false]` — declares that we don't
  want a `RepartitionExec` inserted upstream

Without these, a future optimiser rule that inserts a repartition above the
parquet source would silently reshuffle rows and the offset-based filter
would produce garbage. The DV filter would still "work" without errors —
it'd just delete the wrong rows.

### Layer 3: column-mapping rename projection

When `column_mapping_mode = "name"`, the parquet read produced columns under
their physical names (e.g. `col-1a2b3c`). The synthetic-column detection
downstream looks for `row_id` / `__delta_internal_row_index` etc. by *logical*
name. We insert a `ProjectionExec` that renames physical → logical so the
downstream layers can see what they expect.

This projection runs BEFORE the synthetic wrap. Two reasons:

- Synthetic columns have fixed names that are never CM-renamed; we want the
  parquet output already in logical form when we append synthetics
- The synthetic exec's "is this column already in the input?" check uses
  logical names; running rename first makes that check correct

### Layer 4: `DeltaSyntheticColumnsExec`

This is the most Delta-specific piece. Source: `contrib/delta/native/src/synthetic_columns.rs`.

The exec appends up to four columns onto the parquet output:

| Column | Type | How it's computed |
|---|---|---|
| `__delta_internal_row_index` | UInt64 | per-file row counter, starts at 0, increments by batch size |
| `__delta_internal_is_row_deleted` | Int32 (0/1) | walks the per-task DV sorted indexes against the current row offset |
| `row_id` | Int64 | `task.base_row_id + physical_row_index` (per file) |
| `row_commit_version` | Int64 | `task.default_row_commit_version` (constant per file) |

The exec sees task boundaries via `FileGroup` partitioning (see Layer 1 note).
Inside a task, it processes batches in physical order; the per-task state is
`{file_row_offset, next_delete_idx}`. After each batch:

- `file_row_offset += batch.num_rows()`
- `next_delete_idx` advances past any DV indexes consumed in this batch
  (this writeback was a review-fix — earlier versions re-walked from 0 each batch)

When a task finishes (the parquet reader signals end-of-file via a stream
boundary), the per-task state resets.

**Why we synthesise rather than read from a materialised column**. Delta
*can* materialise `row_id` / `row_commit_version` into the parquet files at
write time, in which case we'd just read them directly. But Delta only
materialises them when row tracking has been on since the file was written —
files written before row tracking was enabled have a `baseRowId` table-level
constant and we must compute `row_id` arithmetically. Our path covers both
cases uniformly: the planner sets `emit_row_id = true` only when materialisation
is NOT available; when materialisation IS available, the column comes through
the parquet read like any other column.

### Layer 5: reorder projection

`required_schema` might want synthetics in non-suffix positions. The driver
computed `final_output_indices` (a permutation of `[0..n]`) and put it in the
proto. If the indices aren't the identity, we wrap with a final
`ProjectionExec` that reorders columns. Identity → skip.

The driver's assertion `assert(emitIdx >= 0)` ensures we never compute an
out-of-bounds permutation; if a synthetic is in `required_schema` but its
emit flag wasn't set (somehow), we fail fast on the JVM side rather than
producing wrong output natively.

## The output stream

What leaves the topmost exec is an `Arc<dyn ExecutionPlan>` whose
`execute()` returns an Arrow `RecordBatchStream`. Comet's existing native
executor framework consumes that stream, moves the batches across JNI into
the JVM via the Arrow C Data Interface, and hands them to Spark as
`ColumnarBatch`es.

There is nothing Delta-specific in the cross-JNI machinery. As far as the
JVM is concerned, the result looks like any other Comet native scan.

## Error handling at the native edge

Failures at any layer (parquet decode error, DV file checksum mismatch,
schema-adaptor mismatch) propagate up as DataFusion `DataFusionError`s and
are converted to Java `RuntimeException`s by the JNI shim. The JVM-side
wrapper in `ShimSparkErrorConverter.wrapNativeParquetError` recognises
parquet-flavoured errors and wraps them with `FAILED_READ_FILE.NO_HINT`
including the file path — this matches Spark's standard error surface for
parquet read failures.

If the failure happens at the kernel-rs layer on the driver (during plan
construction), we never get to native execution. The planner catches the
error, calls `withInfo(plan, "delta-kernel-rs error: …")`, and falls back
to Spark's Delta reader. See `06-fallback-and-ops.md` for the full
catalogue.

## What this stack does NOT do

- **No vectorised expression evaluation here.** Filters that get pushed
  into `ParquetSource` use DataFusion's PhysicalExpr, but anything above
  the scan (joins, aggregates, post-scan projections from the user's
  query) goes through Comet's regular operator stack, not this contrib.
- **No write-side anything.** No commit logic, no `_delta_log` writes,
  no protocol upgrade checks. Reads only.
- **No streaming-source semantics.** Each plan invocation resolves to a
  single Delta snapshot version. Structured Streaming's
  `DeltaSource`/`DeltaSink` paths fall back to Spark.
