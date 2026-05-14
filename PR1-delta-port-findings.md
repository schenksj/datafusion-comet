# Delta-port confidence-check findings

Working document — informs whether PR1's SPI shape needs revision before review. Not
committed.

Source of truth: `delta-kernel-phase-1` (worktree at `/tmp/delta-source`).
Target: `comet-contrib-spi` branch's SPI.

## Methodology

Walked through the Delta integration touch points file-by-file, asking "can this be
expressed in the new SPI as it stands?"

## Existing Delta files (≈3,200 lines)

### Native (Rust, 1,501 lines)
- `native/core/src/delta/{engine,error,jni,mod,predicate,scan,integration_tests}.rs`
- `native/core/src/execution/operators/delta_dv_filter.rs`

### JVM (Scala, 1,677 lines + Exec)
- `spark/.../comet/delta/{DeltaReflection,RowTrackingAugmentedFileIndex}.scala`
- `spark/.../comet/serde/operator/CometDeltaNativeScan.scala` (886 lines)
- `spark/.../sql/comet/CometDeltaNativeScanExec.scala`

## Integration touch points on core (the things the SPI must subsume)

### CometScanRule (`delta-kernel-phase-1` branch)
- `_apply` runs `stripDeltaDvWrappers(plan)` **before** per-scan transformation —
  unwraps Delta's `PreprocessTableWithDVs` Catalyst Strategy.
- Populates a mutable `dvProtectedScans: Set[FileSourceScanExec]` during the pre-pass;
  consulted by per-scan transform to decide whether to keep the scan Spark-native.
- `transformV1Scan` calls `withDeltaColumnMappingMetadata`, `applyRowTrackingRewrite`,
  and `nativeDeltaScan` — all private methods on `CometScanRule`.
- Tags relation options for input_file_name handling (#75 design A).

### CometExecRule
- Dedicated `case scan: CometScanExec if scan.scanImpl == SCAN_NATIVE_DELTA_COMPAT`
  arm that routes to `CometDeltaNativeScan` (the serde).

### Native dispatcher
- Dedicated `OpStruct::DeltaScan(...)` arm at planner.rs:1637 (≈150 lines of body)
  building a `delta_exec` with column-mapping rewrites, DV filter wrapping,
  ProjectionExec renames.

### Proto
- Six Delta-specific messages live in core's `operator.proto`:
  `DeltaScan`, `DeltaScanCommon`, `DeltaScanTask`, `DeltaScanTaskList`,
  `DeltaColumnMapping`, `DeltaPartitionValue`.

## What maps cleanly onto the current SPI

| Touch point | SPI mechanism | Notes |
|---|---|---|
| Per-scan transformation | `CometScanRuleExtension.matchesV1` / `transformV1` | Delta's `nativeDeltaScan` body becomes `transformV1`'s body. `matchesV1` keys on `DeltaReflection.isDeltaFileFormat(r.fileFormat)`. Returning `None` from `transformV1` falls back to vanilla Spark+Delta, identical to today's `getOrElse(scanForDelta)`. |
| `withDeltaColumnMappingMetadata`, `applyRowTrackingRewrite`, etc. | Private methods on the contrib's extension class | No SPI exposure needed; contrib owns them. |
| Operator serde for `CometDeltaNativeScanExec` | `CometOperatorSerdeExtension.serdes` map | Contrib defines `CometDeltaNativeScanExec` as its own class (already is); registers `Map(classOf[CometDeltaNativeScanExec] -> CometDeltaNativeScan)`. CometExecRule's class-based dispatch picks it up via `(allExecs ++ contribSerdes).get(op.getClass)`. |
| Native dispatcher arm | `ContribOperatorPlanner.plan(payload, children)` | The ~150-line `OpStruct::DeltaScan` body moves into the contrib's planner impl. Children (the file-scan children) come in as `Vec<Arc<dyn ExecutionPlan>>` — Delta uses zero children today, but the API supports both. |
| DV filter wrapping | Done inside `ContribOperatorPlanner.plan` | Contrib's planner returns `Arc<dyn ExecutionPlan>` wrapped in `DeltaDvFilterExec` when needed. Core never knows. |
| Proto layout | Contrib ships its own proto package | Already planned: `contrib/delta/proto/`. |

## SPI gaps (must add to PR1 before review)

### Gap #1: `CometScanRuleExtension` needs a tree-level pre-pass hook

**Why:** Delta's existing `stripDeltaDvWrappers` walks the **whole plan tree once**
before per-scan transformation begins, unwrapping the `ProjectExec(FilterExec(...))`
subtree Catalyst's `PreprocessTableWithDVs` strategy inserted. Currently `_apply` calls
it directly on the top-level plan. With the SPI, this rewrite belongs inside the
contrib — but the SPI today only exposes per-scan transformation.

**Proposed addition** to `CometScanRuleExtension`:

```scala
/** Tree-level pre-pass run once per plan before per-scan dispatch. Default: identity.
  * Use this to undo wrapper rewrites that a format's own Catalyst strategy applied
  * (e.g., Delta's PreprocessTableWithDVs). */
def preTransform(plan: SparkPlan, session: SparkSession): SparkPlan = plan
```

Then `CometScanRule._apply` becomes:

```scala
val prepped = CometExtensionRegistry.scanExtensions
  .foldLeft(plan)((p, ext) => ext.preTransform(p, session))
prepped.transform { case scan if isSupportedScanNode(scan) => transformScan(scan) }
```

Shared state between the pre-pass and `transformV1` is the contrib's problem — it can
use Spark's `TreeNodeTag` mechanism (the Delta integration already uses this pattern
for the `SKIP_COMET_SCAN_TAG` in `CometSpark34AqeDppFallbackRule`).

**Impact on PR1:** ~10 lines of new SPI surface + a documentation update in the
contributor guide. Low risk; backwards-compatible default (`identity`).

### Gap #2: `CometScanRuleExtension.transformV1` arity is sufficient — but only if we drop the `scanImpl` tag pattern

**Why:** Delta's existing code uses `CometScanExec(scanWithMappedSchema, session, SCAN_NATIVE_DELTA_COMPAT)`
— a generic `CometScanExec` with a stringly-typed `scanImpl = "native_delta_compat"`
tag. `CometExecRule` dispatches on the tag.

For the contrib SPI, the `scanImpl` tag has no analogue (the SPI dispatches by
**class**, not by tag). The contrib must define its own `CometScanExec` subclass —
e.g. `class CometDeltaScanExec(...) extends CometScanExec(..., SCAN_NATIVE_DELTA_COMPAT)`
or even a stand-alone class — and register a serde keyed on the new class.

**No SPI change needed**, but documenting this convention in the contributor guide
prevents the next contrib author from getting confused.

### Gap #3: Build wiring for contrib protos

**Why:** Delta-specific proto messages need to move out of core's `operator.proto`.
The contrib gets its own proto crate (`contrib/delta/proto/`) and runs its own protoc
invocation. The `contrib/example/` reference module does NOT exercise this — it has no
proto of its own; the example contrib's payload is empty bytes.

**Proposed addition to PR1:** extend `contrib/example/` with a trivial proto so future
contrib authors have a reference for proto generation. Concretely:

- `contrib/example/proto/example_op.proto` with a `message ExampleConstantScan { int32 value = 1; }`
- `contrib/example/native/build.rs` running `prost-build` over it
- `contrib/example/pom.xml` running `protobuf-maven-plugin`
- The example's `ContribOperatorPlanner` decodes the payload as `ExampleConstantScan`
  and uses `value` (e.g., emits `value` rows of a constant batch)

**Impact on PR1:** modest (~30 lines of new code + build wiring). Worth it because
proto generation is the trickiest setup step for new contribs and the worked reference
without it leaves a gap.

### Gap #4: `ContribOperatorPlanner::plan` needs access to core's parquet / expr machinery — **CLOSED**

> **Resolution (commit `adbdbce1` on `contrib-delta-port`):** SPI extended with
> `ContribPlannerContext` trait + `ParquetDatasourceParams` argument bundle. Core
> implements the trait via a thin `CorePlannerContext<'a>` adapter that borrows a
> `&PhysicalPlanner`; dispatcher constructs one per `ContribOp` arm. Delta's full
> ~150-line dispatcher body was lifted into `contrib/delta/native/src/planner.rs` (with
> `ColumnMappingFilterRewriter`, `build_delta_partitioned_files`, and
> `parse_delta_partition_scalar` going along for the ride). `cargo build` is green for
> core (default features, both contribs linked) and for contrib/delta standalone;
> 4 SPI/example tests pass.



**Surfaced during:** P4 of the Delta port (worktree at `contrib-delta-port`). Attempting
to lift the `OpStruct::DeltaScan` arm into a contrib `ContribOperatorPlanner` impl ran
into five core-side dependencies that `comet-contrib-spi` does not expose:

1. **`init_datasource_exec(...)`** -- the 16-arg builder that turns
   `(required_schema, data_schema, partition_schema, object_store_url, file_groups,
   projection, filters, default_values, timezone, case_sensitive, ...)` into a
   `DataSourceExec` over Comet's tuned `ParquetSource`. Lives in
   `crate::parquet::parquet_exec::init_datasource_exec`. **Every** file-scan contrib
   (Delta, Iceberg) will need this; reimplementing it per-contrib would defeat the
   point of Comet's tuned ParquetSource.

2. **`prepare_object_store_with_configs(runtime_env, file_url, options)`** -- registers
   the right object store on the runtime env (S3, GCS, ABS) and returns an
   `ObjectStoreUrl`. Same story -- needed by every file-scan contrib, lives in core
   today.

3. **`convert_spark_types_to_arrow_schema(&[SparkStructField])`** -- pure proto-to-arrow
   conversion. Easiest to expose; no runtime deps.

4. **A `create_expr` capability** -- builds a `PhysicalExpr` from a Catalyst `Expr`
   proto + a schema. Today this is a method on `PhysicalPlanner` that recurses through
   the full Catalyst expression library. Delta's `common.data_filters` arrive as
   serialized Catalyst `Expr` protos and need this conversion before they can be wrapped
   with `ColumnMappingFilterRewriter`.

5. **`SessionContext` reference** -- for runtime-env mutation
   (`prepare_object_store_with_configs`) and session-config lookups (timezone, etc).

**Proposed addition** -- thread a `ContribPlannerContext` through the trait:

```rust
pub trait ContribPlannerContext: Send + Sync {
    fn session_ctx(&self) -> &SessionContext;
    fn build_physical_expr(
        &self,
        expr_proto_bytes: &[u8],
        schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExpr>, ContribError>;
    fn convert_spark_schema(
        &self,
        fields_proto_bytes: &[u8],
    ) -> Result<SchemaRef, ContribError>;
    fn prepare_object_store(
        &self,
        any_file_url: &str,
        configs: &HashMap<String, String>,
    ) -> Result<ObjectStoreUrl, ContribError>;
    fn build_parquet_datasource_exec(
        &self,
        params: ParquetDatasourceParams<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError>;
}

pub trait ContribOperatorPlanner: Send + Sync {
    fn plan(
        &self,
        ctx: &dyn ContribPlannerContext,
        payload: &[u8],
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError>;
}
```

Core (`datafusion-comet`) implements `ContribPlannerContext` as a thin adapter over its
existing `PhysicalPlanner` and `SessionContext`. The four free functions stay where
they live; the trait methods are one-line forwards.

`ParquetDatasourceParams<'_>` is a struct with the 16 fields `init_datasource_exec`
already takes -- moves to `comet-contrib-spi` so the API is a public trait method, not
a private function signature.

**Impact on PR1:**
- ~80 lines of new SPI trait (`ContribPlannerContext` + `ParquetDatasourceParams`)
- ~30 lines in `datafusion-comet` to implement the trait
- One signature change on `ContribOperatorPlanner::plan` (adds `ctx` param)
- Update `contrib/example/` to take and ignore `ctx`
- Update core's `OpStruct::ContribOp` dispatcher to construct a `ContribPlannerContext`
  and pass it through

This is a meaningful expansion of PR1's surface but it's load-bearing -- without it the
SPI cannot host file-scan contribs, which is the headline use case.

**Validation outcome:** Gap #4 closed (see resolution box above). The native side of
the Delta port is complete; the SPI surface has been proven to host a real
file-scan contrib. The remaining P5 (JVM port, ~1700 LoC mechanical Scala) and P6/P7
(smoke + regression sweep) are PR2 implementation work that doesn't surface any further
SPI design questions.

## Summary of SPI changes needed for PR1

After validation, the SPI now carries these surfaces beyond what the original PR1 had:

  * `CometScanRuleExtension.preTransform(plan, session)` -- tree-level pre-pass
    (Gap #1).
  * `ContribPlannerContext` trait + `ParquetDatasourceParams` struct (Gap #4).
  * `ContribOperatorPlanner::plan` first arg is `&dyn ContribPlannerContext`.
  * Convention documented for class-keyed serde dispatch (Gap #2, doc-only).
  * Example contrib's proto layer demonstrates the proto-generation wiring (Gap #3).

All four gaps were surfaced by attempting to host a real Delta port on top of the
proposed SPI. Without that exercise, gaps #1 and #4 in particular would only have shown
up at PR2 review time. The validation experiment did its job.

## Things that are NOT SPI gaps (just port mechanics)

- All of the `DeltaReflection.scala` reflection probes move verbatim into the contrib.
- `CometDeltaNativeScan.scala`'s `convert` method (886 lines) is mostly proto-building
  logic that moves verbatim — the only refactor is producing a `ContribOp` envelope
  with `kind="delta-scan"` and the Delta proto bytes as the payload, instead of
  producing `OpStruct.DeltaScan` directly.
- `CometDeltaNativeScanExec.scala` becomes the contrib's published class; serde
  registration uses `Map(classOf[CometDeltaNativeScanExec] -> CometDeltaNativeScan)`.
- Native `delta/{engine,scan,jni,predicate}.rs` move verbatim into
  `contrib/delta/native/src/`. The `jni.rs` JNI entry point (driver-side log replay)
  stays a separate JNI symbol; it doesn't go through the ContribOp dispatch path.
- `delta_dv_filter.rs` moves to `contrib/delta/native/src/dv_filter.rs`.

## Recommended action on PR1

**Before opening:**

1. **Add Gap #1's `preTransform` hook** to `CometScanRuleExtension`. One small commit
   to `comet-contrib-spi`.
2. **Extend `contrib/example/`** with a minimal proto + `build.rs` + protobuf-maven
   wiring (Gap #3). Demonstrates the proto layer end-to-end. One commit.
3. **Document the class-subclass convention for serde dispatch** (Gap #2) in the
   contributor guide. Tiny edit to `contrib-extensions.md`.
4. **Update `docs/contrib-delta-migration-plan.md`** to reflect the actual
   three-crate-per-contrib shape (`contrib-spi` leaf + contrib proto + contrib native)
   and the `preTransform` SPI addition.

**After opening:** PR2's Delta port can proceed mechanically — no further SPI
surprises identified. The port is large (~3,200 lines moving + 150-line dispatcher
refactor) but every piece has a clear destination.

## Summary

The SPI is **mostly the right shape** for a real consumer. Three small additions /
clarifications close the gaps that came up. Nothing in the survey suggests a deeper
architectural rework is needed.
