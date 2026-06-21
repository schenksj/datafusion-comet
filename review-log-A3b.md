# Unit A.3b — Rust executor side — review log

Branch: `pr/delta-A3b-rust-executor` (based on `pr/delta-A3a-rust-driver`).
Commit: `38e2312c6` (single commit; review fixes amended in).

## Carve (from `feat/delta-kernel-read`)
7 files, +2530/-62. Completes the contrib native crate:
- Verbatim executor modules: `planner.rs` (stub -> real, 561), `kernel_scan.rs` (1553),
  `dv_reader.rs` (388). `planner` <-> `kernel_scan` are mutually dependent and ship together.
- `lib.rs`: re-adds the `dv_reader`/`kernel_scan` module decls + the `DeltaScan`/`DeltaScanCommon`
  proto re-exports trimmed in A.3a. Kept A.3a's clarified planner doc-link.
- `Cargo.toml`: re-adds the executor deps + kept crate version 0.18.0 (monolith's 0.17.0 is stale).
- Both lockfiles regenerated.

## Carve invariants (verified)
- Stub planner FULLY replaced (no `NotImplemented` / "build-gate stub" left in planner.rs).
- A.3b touches ONLY `contrib/delta/native/` + `native/Cargo.lock`. No core, no gate script, no JVM.
- The unchanged core shim reaches the real `planner::plan_delta_scan` (4-arg sig; gated build links).
- contrib native crate is now equivalent to the monolith (modulo version 0.18.0, the doc-link, and
  the dep cleanup below).

## §5 verification (all green)
gated native build, **89 in-crate unit tests** (54 driver + 35 executor), default native build
unchanged, clippy (both feature states), gate-verify script (contrib libcomet +13 MB), cargo fmt.
(Freed disk mid-run: the executor delta_kernel tree filled the volume; cleaned the contrib standalone
target after the 89 tests passed.)

## Review round (manual + /code-review high, 29-agent workflow: 1 refuted, 5 reported — all in Cargo.toml, no correctness bugs)
**Fixed (folded into the commit) — dead deps the monolith carried, verified unused by grep AND by the
gated rebuild compiling without them:**
- **`roaring`** (CONFIRMED): zero usage in src/ (DV decode goes through delta_kernel, not the roaring
  crate). Removing it also eliminated a DUPLICATE roaring 0.10.12 that coexisted with the
  transitively-pulled 0.11.4. Now a single roaring 0.11.4.
- **`datafusion-datasource`** (CONFIRMED): zero usage (no `TableSchema`/`ignore_missing_files`/
  `FileSource`); its comment described an old removed path. Dropped.
- **direct `parquet` dep + datafusion `parquet` feature** (PLAUSIBLE): no direct `parquet::` use and
  no datafusion parquet-feature types (all parquet I/O goes through `delta_kernel::parquet`; the exec
  is `DeltaKernelScanExec`). Dropped both. Verified: the contrib uses only core DataFusion types
  (DataFusionError, SendableRecordBatchStream, TaskContext).
- **stale `dv_filter` comment** (CONFIRMED): the comet-common dep comment cited
  `dv_filter::map_dv_error_to_datafusion`; the function lives in `dv_reader.rs`. comet-common IS used
  (`SparkError`), so kept the dep, fixed the comment.

NOTE: these dead deps also exist in the monolith's contrib Cargo.toml (the modules are byte-identical);
the cleanup is an improvement over the monolith and should flow back when the monolith reconciles.

**Refuted (1):** a duplicate framing of the parquet finding.

**State:** A.3b review clean. The Rust side of the contrib is complete. Ready to open upstream once its
base chain (A.1 #4700 -> A.2 -> A.3a -> A.3b) lands.
