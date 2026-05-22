# Spark 3.5 + Delta 3.3 support: feasibility evaluation

**Status:** evaluation only — no implementation in this PR. Estimates assume
a developer already familiar with this contrib.

## TL;DR

| Effort tier | Time | Scope |
|---|---|---|
| **Minimal viable** | 2–3 dev-days | spark-3.5 build + most coverage passing, row-tracking degraded |
| **Production-equivalent** | 1–2 dev-weeks | full coverage on Delta 3.3, regression diff ported, all 49 contrib tests green |
| **Full multi-version** | 3–4 dev-weeks | spark-3.4 + spark-3.5 + spark-4.x all green, separate Delta versions per Spark, CI matrix |

Recommend the **minimal viable** tier as a follow-up PR after this lands, gated on a user actually asking for 3.5 support. The minimal version is small enough that the surface stays maintainable; the multi-version expansion has decaying ROI.

## What's already in the contrib's favour

1. **Comet's Spark-version shim pattern is already established.** `spark/src/main/spark-3.5/`, `spark-3.x/`, `spark-4.0/`, `spark-4.1/`, `spark-4.x/` source roots already exist and contain shim files for the APIs that diverge across Spark versions (`ShimCometScanExec`, `ShimSubqueryBroadcast`, `ShimSparkErrorConverter`, etc.). The contrib slots into this pattern — we just add a `contrib/delta/src/main/scala-spark-3.5/` overlay or use `src/main/scala/` files conditional on profile.

2. **`delta-kernel-rs` is Delta-version-independent.** Kernel reads the Delta *log* format (transaction log JSON + checkpoint parquet + DV bitmaps), not Spark's Delta Scala API. The on-disk Delta 3.x and 4.x log formats are compatible (same protocol versions). Kernel 0.19.x reads both. Zero native work to support Delta 3.3.

3. **No `io.delta.spark` typed imports in contrib Scala.** The contrib's only `io.delta.*` reference is in a doc comment. Every Delta API call is reflective (`Class.forName`, `findAccessor`, `invokeNoArg`). Class names match Delta 3.x layout already in many cases.

4. **`OpStruct::DeltaScan` proto is version-agnostic.** No Spark-version-specific fields.

5. **The `contrib-delta` build gate is enforced.** `dev/verify-contrib-delta-gate.sh` proves default builds carry zero Delta surface. Adding a second Delta version reuses the gate.

## What needs new work for Spark 3.5 + Delta 3.3

### Spark 4-only APIs the contrib leans on

| Surface | Spark 4 form | Spark 3.5 form | Mitigation |
|---|---|---|---|
| `_metadata.row_index` | Native virtual column on `_metadata` struct | **Not present** | Falls back: Delta's strategy in 3.x surfaces `_tmp_metadata_row_index` directly. Already supported in our synthetic-column emit code via the alias. |
| `_metadata.row_id` | Native virtual column on `_metadata` | **Not present** | Row-tracking reads via `_metadata.row_id` won't engage. Direct read of materialised `_row-id-col-<uuid>` still works. |
| `scan.output` carrying `_metadata.*` appended | Yes | Same; identical FileSourceScanExec contract | No change |
| `CometScanExec` / `FileSourceScanExec` API | Same shape | Some method signatures differ (e.g. `relation`, `output`, `requiredSchema` all present) | Already shimmed in `ShimCometScanExec` |
| `HadoopFsRelation` | Same | Same | No change |
| `InputFileBlockHolder.set(path, 0L, 0L)` | 3-arg | Same in 3.5 | No change |
| `BindReferences.bindReference(attr, inputs, allowFailures)` | Available | Available in 3.5 | No change |
| `ColumnarToRowExec` / `AdaptiveSparkPlanExec` | Same | Same | No change |

**Net Spark API gap:** essentially just `_metadata.row_index` and `_metadata.row_id`. Both can be detected and have graceful-degradation paths (decline-to-vanilla for those queries; everything else works).

### Delta-internal reflective accessors

The contrib accesses ~12 reflective names against Delta classes:

| Reflective name | Delta 4.0 | Delta 3.3 | Delta 2.4 | Notes |
|---|---|---|---|---|
| `org.apache.spark.sql.delta.actions.DeletionVectorDescriptor` | ✅ | ✅ | ✅ | Stable since 2.0 |
| `org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore` | ✅ | ✅ (renamed pkg?) | ✅ | Needs verification |
| `org.apache.spark.sql.delta.DeltaParquetFileFormat` | ✅ | ✅ | ✅ | Stable |
| `org.apache.spark.sql.delta.DeltaScan` | ✅ | ✅ | ✅ | Stable |
| `org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex` | ✅ | ✅ | ✅ | Stable since ~3.0 |
| `TahoeBatchFileIndex`, `TahoeLogFileIndex`, `CdcAddFileIndex` | ✅ | ✅ | ✅ | Stable |
| `AddFile.{path, size, partitionValues, stats, deletionVector, baseRowId, defaultRowCommitVersion, modificationTime}` | ✅ | partial | partial | `baseRowId` / `defaultRowCommitVersion` are row-tracking, **Delta 3.2+ only** |
| `deltaLog.update(stalenessAcceptable, ...)` | 3-arg | 1-arg or 2-arg in 3.x | varies | **Needs shim** |
| `snapshot.filesForScan(filters, keepNumRecords)` | 2-arg | varies in 3.x | varies | **Needs shim** |
| `delta.enableRowTracking` table property | ✅ | ✅ (3.2+) | ❌ | Absent in 2.4 — row-tracking unsupported |
| `delta.columnMapping.mode` = "name" / "id" | ✅ | ✅ | ✅ | Stable since 2.0 |
| `_row-id-col-<uuid>` materialised column prefix | ✅ | ✅ (3.2+) | ❌ | Row-tracking only |

**Net Delta API gap:** the reflective accessors mostly use stable names. The two that are version-sensitive (`deltaLog.update` arity, `snapshot.filesForScan` arity) are already wrapped in `try/catch` in our reflection helpers, but the **fallback path needs to actually try the older signatures** (currently it tries only the Delta-4 form and returns `None` if not present, which would force decline-to-vanilla).

### Build / packaging changes

Concrete file-level changes for Spark-3.5 support:

1. **`spark/pom.xml`** — make the `contrib-delta` profile pick a Delta version that matches the active Spark profile:
   ```xml
   <!-- in the spark-3.5 profile -->
   <delta.version>3.3.2</delta.version>  <!-- or 3.3.x latest -->
   <!-- in the spark-4.1 profile (current default) -->
   <delta.version>4.1.0</delta.version>
   ```
   Currently `<delta.version>` is hardcoded in the `contrib-delta` profile. Move it to a per-Spark-profile property override.

2. **`contrib/delta/src/main/scala-spark-3.5/`** (new) — overlay sources for the handful of Spark-4-only call sites. Expected size: 2–4 small files, ~150–300 lines total. Likely contents:
   - Adapter for `deltaLog.update(...)` arity differences
   - Adapter for `snapshot.filesForScan(...)` arity differences
   - Optional: a stub that declines synthesis of `_metadata.row_index` / `_metadata.row_id` references on Spark 3.5

3. **`contrib/delta/dev/run-regression.sh`** — accept a `--delta-version` argument and dispatch to the matching diff:
   - `dev/diffs/delta/4.1.0.diff` (current, for Delta 4.1 on Spark 4.1)
   - `dev/diffs/delta/3.3.x.diff` (new, for Delta 3.3 on Spark 3.5)

4. **`dev/verify-contrib-delta-gate.sh`** — extend to also verify the gate on the `spark-3.5,contrib-delta` profile combination, not just `spark-4.1,contrib-delta`.

5. **Native: zero changes.** Kernel 0.19 handles both protocols. `DeltaSyntheticColumnsExec`, `DeltaDvFilterExec`, the `core_glue.rs` dispatcher — all version-agnostic on the Spark side.

### Test coverage shape

| Suite | Delta 4.1 / Spark 4.1 today | Delta 3.3 / Spark 3.5 expected |
|---|---|---|
| `CometDeltaFeaturesSuite` (DV, time travel, complex types, aggregates, joins, row tracking, input_file_name) | 8/8 | ~7/8 (the `_metadata.row_id` unmaterialised test won't engage on 3.5) |
| `CometDeltaNativeSuite` (basic SELECT, projection, type widening) | 4/4 | 4/4 |
| `CometDeltaColumnMappingSuite` (CM-name, CM-id, CM+DV, CM+schema evolution) | 5/5 | 5/5 |
| `CometDeltaCoverageSuite` (24-test SQL surface matrix) | 24/24 | ~22/24 (window-function variants may shim differently in 3.5; verifiable) |
| Delta own-regression diff | 4.1.0 baseline | 3.3.x baseline (port required) |

## What "minimal viable" (2–3 days) gets

- `mvn -Pspark-3.5,contrib-delta` compiles and links a working contrib jar
- Plain SELECT / projection / filter / aggregate / join works against Delta 3.3 tables
- Column mapping (name + id) works
- Deletion vectors work
- Row tracking partial: materialised reads work; `_metadata.row_id` unmaterialised reads fall back to vanilla on Spark 3.5
- `dev/verify-contrib-delta-gate.sh` extended to cover both Spark profiles

## What "production-equivalent" (1–2 weeks) gets

Everything in minimal-viable PLUS:
- Port `dev/diffs/delta/4.1.0.diff` → `dev/diffs/delta/3.3.x.diff` (mechanically similar; some test-name shifts)
- Full Delta 3.3 regression green on a developer machine
- CI matrix: build × test on both `(spark-3.5, delta-3.3)` and `(spark-4.1, delta-4.1)`
- Coverage-suite gap-by-gap documented (which 1–2 tests are 3.5-only fallbacks, with reason)

## What "full multi-version" (3–4 weeks) gets

Everything above PLUS:
- Spark 3.4 + Delta 2.4 support (DV / row-tracking absent at this tier; degraded coverage but still drop-in)
- Cross-Spark-version contrib jar publishing
- Documentation overlay describing per-Spark coverage differences
- Comprehensive bisect against any Delta 3.x test that diverges from 4.x

## Risk assessment

**Low risk:**
- Maven profile / Delta dep version (1-line property override + tested)
- Native side (kernel handles it)
- `delta-kernel-rs` engine cache / DV resolution

**Medium risk:**
- `deltaLog.update()` / `snapshot.filesForScan()` arity drift across Delta 3.x patch versions
- Delta 3.3's `PreprocessTableWithDVs` strategy may rewrite plans slightly differently — coverage gaps to discover empirically
- `_tmp_metadata_row_index` semantics on Delta 3.x vs 4.x — may need a feature-detection shim

**High risk:**
- *None expected.* Every Delta internal we touch is via `Class.forName` and reflective accessors, so name drift is recoverable rather than a hard break.

## Recommendation

Defer to a follow-up PR. The minimal-viable tier is small enough that one engineer can complete it in a sprint, and the value scales with actual user demand. Wait until a user reports needing 3.5 before investing — at which point the work is concrete (target their Spark + Delta version exactly) rather than speculative (cover the matrix preemptively).

If/when we do it, sequence:
1. Make `<delta.version>` per-Spark-profile in `spark/pom.xml`
2. Extend `dev/verify-contrib-delta-gate.sh` to run on both Spark profiles
3. Add the 2–4 file shim overlay under `contrib/delta/src/main/scala-spark-3.5/`
4. Run the existing contrib Scala suite under the 3.5 profile; fix what breaks (expected: 2–3 tests fall back, fixable individually)
5. Port the regression diff
6. Full regression on Delta 3.3
