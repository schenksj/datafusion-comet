# Unit A.3a — Rust driver side — review log

Branch: `pr/delta-A3a-rust-driver` (based on `pr/delta-A2-buildgate`).
Commit: `1a774d02d` (single commit; review fixes amended in).

## Carve (from `feat/delta-kernel-read`)
10 files, +4374/-50 (the bulk is the two Cargo.lock regens: native +202, contrib standalone +~1.5k).
- Verbatim copies: `error.rs` (61), `engine.rs` (468), `predicate.rs` (697), `scan.rs` (779),
  `jni.rs` (617) = 2622 lines of driver code, proven in the monolith (54 unit tests).
- Carved `lib.rs`: declares the 5 driver modules + keeps the `planner` STUB; drops the
  `dv_reader`/`kernel_scan` module decls (A.3b).
- Trimmed real `Cargo.toml`: only the deps the driver set uses (delta_kernel, object_store, arrow,
  url, jni, prost, serde_json, thiserror, log + proto/jni-bridge path deps). Executor deps
  (parquet, roaring, datafusion-datasource, futures, chrono*, comet-common, tokio) deferred to A.3b.
- Gate-verify `cargo-tree` assertion re-tightened to require `delta_kernel` (now real, not a stub).

## Carve invariants (verified)
- Driver set has ZERO non-comment references to deferred modules (dv_reader/kernel_scan/planner).
- A.3a touches ONLY `contrib/delta/native/` + `native/Cargo.lock` + the gate script. No core, no JVM.
- The A.2 stub `planner.rs` is byte-unchanged and still returns NotImplemented.

## §5 verification (all green)
gated native build; 54 in-crate unit tests (cargo test); default native build unchanged; clippy
(both feature states); gate-verify script (now asserts delta_kernel, contrib libcomet +11 MB);
cargo fmt (native + contrib standalone).

## Review round (manual + /code-review high, 22-agent workflow: 6 refuted, 6 survived)
**Fixed (folded into the commit):**
- **[lib.rs] dead `crate::proto` re-export** (CONFIRMED): `DeltaScan`/`DeltaScanCommon` were re-exported
  but no driver module uses them via `crate::proto` (the stub planner imports them directly from
  `datafusion_comet_proto`). Trimmed to the driver's real proto surface
  (`DeltaDvDescriptor`/`DeltaPartitionValue`/`DeltaScanTask`/`DeltaScanTaskList`); A.3b re-adds the
  envelopes when the real planner lands. Consistent with the Cargo.toml "declare what you use" trim.
- **[Cargo.toml] unused `datafusion` `parquet` feature** (CONFIRMED): the driver only needs
  `ExecutionPlan`/`SchemaRef`/`DataFusionError` (stub planner); dropped the feature (A.3b re-adds it
  for the real parquet scan). Lockfile unchanged (parquet stays via delta_kernel transitively).
- **[lib.rs] misleading `[plan_delta_scan]` doc-link** (CONFIRMED): the module doc implied the
  crate-root `scan::plan_delta_scan` is what core's dispatcher calls, but core calls
  `planner::plan_delta_scan` (the stub). Reworded to disambiguate the two same-named functions.

**Documented (not fixed — verbatim monolith code, latent, not carve-introduced):**
- **[scan.rs:691/693] DV offset/sizeInBytes unguarded Arrow reads** (PLAUSIBLE): `a.value(i)` without
  `is_null` on the DV size/cardinality fields, and an i32->u64 offset cast that would wrap a negative.
  Both are real mechanisms but gated behind Delta DV invariants (non-null sizeInBytes, non-negative
  offset) that valid tables never violate. This is verbatim, test-passing monolith code; fixing it in
  the carve would diverge A.3a from the monolith. Candidate for a separate monolith-level hardening
  pass, NOT an A.3a change.

**Refuted (6):** the `scan::plan_delta_scan` vs `planner::plan_delta_scan` name "collision" (two fns
in different modules is fine in Rust), the arrow feature set (transitively fine), the CONTRIB_HITS>=2
gate brittleness.

**State:** A.3a review clean. Ready to open upstream once A.2 (#4 → upstream) lands; depends on A.3a's
base chain (A.1 #4700 -> A.2 -> A.3a).
