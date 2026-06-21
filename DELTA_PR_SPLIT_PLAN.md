# Delta Contrib PR Split — Execution Plan

> Working/process document for the fork. **Never stage this file into any split PR.**
> Created 2026-06-09 from analysis of `feat/delta-kernel-read` (97 commits, +27.5k lines, 121 files)
> vs `main`.

---

## 1. Objectives and ground rules

1. Replace the unreviewable monolith (#4366, ~27.5k lines) with a sequence of PRs, each
   independently reviewable and independently mergeable to `main`.
2. **Preserve the complete work product** on a protected reference branch + tag. Nothing is lost.
3. The seven already-open extraction PRs (#4524, #4525, #4532, #4533, #4535, #4536, #4588) are
   **not** part of the sequence and are **not** hard dependencies of its front. They gate only the
   final test/regression PRs (A.6a/A.6b).
4. Every split-PR branch must pass `/review-comet-pr` **clean** locally (via draft PRs on the
   fork) *before* the upstream PR is opened.
5. Every cumulative prefix of the sequence must: (a) leave default builds bit-identical in
   behavior, (b) compile and pass targeted tests in gated (`contrib-delta`) builds, (c) fall back
   gracefully for any capability not yet landed.
6. Don't replay history. Each commit set is constructed by copying **final-state** files from the
   reference branch (with the decoupling rework below), so reviewers never see dead intermediate
   code.

Key enabler: the build gate (`-Pcontrib-delta` / `--features contrib-delta`) means default builds
carry zero Delta surface, and the contrib declines gracefully at runtime — so every intermediate
state is safe on `main`.

---

## 2. Target PR sequence

| Unit | Title | Content | Review-dense size | Depends on |
|------|-------|---------|-------------------|------------|
| A.1 | Core SPI for contrib leaf scans | `CometScanWithPlanData` trait, leaf-scan matching generalization, DPP in-place rewrite, reflective `PlanDataInjector` contrib slot | ~300–400 | — (main only) |
| A.2 | Build gate + inert Delta wiring | Maven profile, Cargo feature, proto `DeltaScan` messages, native dispatch shim, `DeltaIntegration` bridge, rule hooks, gate-verify script + CI gate job, stub contrib crate | ~800 + stubs | A.1 |
| A.3a | Rust: driver-side planning | `error.rs`, `engine.rs`, `predicate.rs`, `scan.rs`, `jni.rs` | ~2.6k (54 unit tests) | A.2 |
| A.3b | Rust: executor-side read path | `dv_reader.rs`, `kernel_scan.rs`, `planner.rs` (replaces stub) | ~2.4k (35 unit tests) | A.3a |
| A.4a | Scala: claim/decline + reflection | `DeltaConf`, `DeltaReflection`, `DeltaScanMetadata`, `CometDeltaScanMarker`, `RowTrackingAugmentedFileIndex`, `DeltaScanRule` | ~2.3k | A.2 (parallel with A.3x) |
| A.4b | Scala: execution (end-to-end native reads) | `CometDeltaNativeScan` serde, `Native.scala`, `CometDeltaNativeScanExec`, `DeltaPlanDataInjector` + foundational suites | ~2.4k + tests | A.3b, A.4a |
| A.5 | CDF native reads | `CometDeltaCdfScanExec`, `CometExecRule` CDF hook, CDC suites | ~1.2k | A.4b |
| A.6a | Contrib test battery | Remaining repro/audit suites, `delta_contrib_test.yml`, `check-suites.py` | ~4k (low density) | A.4b/A.5 + **all extractions** |
| A.6b | Delta own-suite regression harness | `dev/diffs/*.diff`, `run-regression.sh`, `run-test.sh`, `delta_regression_test.yml` | ~2.1k (low density) | A.4b/A.5 + **all extractions** |
| A.7 | Docs | `contrib/delta/docs/*`, user-guide pages | ~3k prose | A.4b (textually) |
| A.8 | Follow-up: FAILED_READ_FILE parity for Delta | `perPartitionFilePaths` on the trait + threading + Delta exec impl | ~150 | #4536 merged |

```
extractions (#4524…#4588, %-path fix)──────────────────────────────┐  (no front-of-cycle dependency)
                                                                   ▼
A.1 ─► A.2 ─┬─► A.3a ─► A.3b ─┐                          A.6a   A.6b   A.8
            └─► A.4a ─────────┴─► A.4b ─► A.5 ─► A.7  ─►  (gated on extractions)
```

---

## 3. Phase 0 — Preserve the complete work product

```bash
cd /Users/schenksj/tmp/x/datafusion-comet
git checkout feat/delta-kernel-read

# Frozen reference branch + annotated tag at the current head (0e9372080 + local WIP committed first
# or stashed — decide per the dirty files; if the WIP is wanted, commit it before tagging).
git branch ref/delta-complete
git tag -a delta-complete-2026-06-09 -m "Complete Delta contrib work product before PR split"
git push origin ref/delta-complete delta-complete-2026-06-09
```

Rules:
- `ref/delta-complete` is never rebased, never force-pushed. It is the canonical "this is the
  whole thing, and it's green" artifact and the source for all file copies in Phase 1.
- Optional but recommended: keep `feat/delta-kernel-read` alive as the *integration-proof*
  branch — periodically merge `main` into it and run the full regression, so we always know the
  end-state is still green while slices land. (Merge, don't rebase: memory says clean rebases
  have silently duplicated hunks here before.)
- Note current uncommitted changes on `feat/delta-kernel-read` (kernel_scan.rs, planner.rs,
  CometDeltaNativeScan.scala, DeltaConf.scala, CometDeltaTestBase.scala, operator.proto). Commit
  or discard them *deliberately* before tagging.

---

## 4. Phase 1 — Build the reorganized branch

### Method decision: fresh reconstruction, NOT interactive rebase

The reorganized history is built by **fresh reconstruction**: branch from `main`, and for each
unit copy the **final-state** files out of `ref/delta-complete` (plus the decoupling edits),
committing in unit order. The old history is a *source of files*, never a sequence to replay.

Why `git rebase -i` over the ~105 working commits is rejected:
- The commits don't partition by unit — `kernel_scan.rs`, `CometDeltaNativeScan.scala`,
  `DeltaScanRule.scala`, `operator.proto` were each touched by dozens of commits, so reordering
  produces a conflict cascade at nearly every step (days of error-prone work vs hours).
- The history contains states that must never reach reviewers: the legacy ParquetSource path
  (built, then deleted), the pre-contrib-crate planner location, three intermediate shapes of
  the schema handling, plus an upstream merge commit (`2df9acc1c`) mid-stream.
- This repo has a documented history of rebases silently duplicating hunks (the #4525 scheme
  gate); a 97-step reorder is exactly the operation that produces undetected drift.
- Fresh reconstruction is conflict-free by construction and yields a result identical to what a
  perfect rebase would produce; correctness is checked by the Phase 4 whole-stack diff audit
  against `ref/delta-complete`, not by trusting the replay.

Corollaries:
- The 105-commit working history is **never published** — not in the sequence PRs and not in
  the #4366 umbrella (Phase 4 force-pushes the reconstructed stack over it). It is **kept
  forever** on `ref/delta-complete`: it holds the decision trail (#44–#86 issue archaeology)
  and is the baseline for the Phase 4 diff audit.
- Commit granularity: **2–4 logical commits inside the larger units**, not one monolith per
  unit (e.g. A.2 as build-gate / proto / JVM hooks / stubs; A.4b as serde / exec node / test
  suites). Upstream reviewers who step commit-by-commit get a guided tour; the unit boundary is
  still where each PR is cut. Original working commits are never preserved as-is.
- Each unit's lead commit message is written as the seed of that unit's PR description.

### Mechanics

Create `feat/delta-split` from `main`, with **one commit set per unit, in sequence order**. Each
unit's tip also gets its own PR branch (`pr/delta-A1-spi`, `pr/delta-A2-gate`, …) so PRs can be
opened/rebased independently.

```bash
git checkout -b feat/delta-split main
# per unit:
git checkout ref/delta-complete -- <whole-file paths for this unit>
# hand-carve the shared files (see per-unit notes), then:
git add -A && git commit -m "<unit title>"
git branch pr/delta-A1-spi          # cumulative prefix pointer
```

Two file classes:
- **Whole-file copies** — everything under `contrib/delta/`, new core files
  (`DeltaIntegration.scala`, `delta_scan.rs`), proto additions, workflows. Cheap and exact.
- **Hand-carved shared files** — files where the branch diff mixes Delta hunks with
  extraction-PR hunks. Strip the extraction hunks (they land via their own PRs):
  `operators.scala`, `CometScanRule.scala`, `CometExecRule.scala`, `CometExecIterator.scala`,
  `CometExecRDD.scala`, `CometNativeScanExec.scala`, `pom.xml`, `pr_build_*.yml`. Verify each
  carve compiles before committing.

### Unit construction notes (the decoupling rework)

**A.1 — Core SPI** (`CometLeafExec` already exists on `main`; A.1 is smaller than first sized)
- `operators.scala`: add `trait CometScanWithPlanData` **without** `perPartitionFilePaths`
  (that member exists only to feed #4536's error conversion → moves to A.8); generalize the
  input-boundary match to `case _: CometLeafExec` and the `ensureSubqueriesResolved` /
  plan-data collection paths to match on the trait.
- `operators.scala`: add the reflective `DeltaPlanDataInjector$` contrib slot — **rewritten
  against main's current injector registry** (the O(N) loop), not #4535's O(1) refactor.
  Whichever of A.1/#4535 lands second takes a small mechanical conflict; that's accepted.
- `CometPlanAdaptiveDynamicPruningFilters.scala`: the `CometScanWithPlanData` DPP in-place
  rewrite arm.
- Tests: a focused suite for the contrib slot + trait matching (new file; do NOT reuse
  `PlanDataInjectorSuite` — that file belongs to #4535).
- Pitch in the PR description: extension contract for contrib leaf scans, Iceberg-precedent,
  benefits future contribs (Hudi etc.).

**A.2 — Build gate + inert wiring**
- Maven: `pom.xml` `delta.version` property; `spark/pom.xml` `contrib-delta` profile,
  per-Spark-profile `delta.version` (3.3.2 / 4.0.0 / 4.1.0), `failureaccess` test dep.
- **Do NOT carry the `spark.version` 4.1.2→4.1.1 pin in the default pom** (decision point §10.1
  — default plan is CI-only `-Dspark.version=4.1.1` in the Delta workflows + a docs note).
- Cargo: `native/Cargo.toml` workspace `exclude = ["../contrib"]`; `native/core/Cargo.toml`
  optional `comet-contrib-delta` path dep + `contrib-delta` feature; lockfile updates.
- Proto: all `Delta*` messages in `operator.proto` (one-time wire-format review; includes the
  reserved field numbers from the deleted legacy path — keep the comments explaining them).
- Native: `planner.rs` dispatch arm (incl. the unconditional not-compiled-in error),
  `planner/delta_scan.rs` shim, `operator_registry.rs` + `jni_api.rs` exhaustive-match arms,
  `pub(crate) convert_spark_types_to_arrow_schema` promotion.
- **Stub contrib crate**: `contrib/delta/native/{Cargo.toml,Cargo.lock,src/lib.rs,src/error.rs}`
  plus a stub `planner::plan_delta_scan` returning `Err("contrib-delta read path not yet
  implemented")`. This makes the core shim's exact contract visible in the small PR.
- Scala stubs: the maven profile's `add-source` needs `contrib/delta/src/main/scala` to exist —
  ship `DeltaConf.scala` here (or a placeholder) so gated JVM builds compile. Decide during
  carve; prefer `DeltaConf` (it's leaf and tiny) over an artificial placeholder.
- JVM hooks: `DeltaIntegration.scala` complete (its CDF accessors are inert until A.5 —
  reflective lookups simply return `None`); `CometExecRule` **marker hook only** (the CDF hook
  hunk moves to A.5); `CometScanRule` Delta delegation + metadata-col-guard reordering,
  **carved against main's current structure** (without #4525's scheme-gate hunks).
- Gate enforcement: `.gitignore` entries, `dev/verify-contrib-delta-gate.sh`, and a CI job
  running it (carve the build-gate job out of `delta_contrib_test.yml` into a minimal workflow
  here; the full test workflow lands in A.6a).
- Tests: assert (a) default build has zero delta surface (gate script), (b) gated build with
  stubs compiles and a Delta read falls back cleanly to vanilla Spark.

**A.3a — Rust driver side**
- `error.rs` (replaces stub), `engine.rs`, `predicate.rs`, `scan.rs`, `jni.rs`; Cargo dep +
  lockfile growth. Imports verified self-contained (`jni → scan → engine → error`; `predicate`
  standalone).
- Story for reviewers: "open table, replay log, push predicates, return `DeltaScanTaskList`
  over JNI." 54 in-crate unit tests run via `cargo test`.
- Held in reserve if reviewers want smaller: `predicate.rs` (706 lines, 28 tests) can split out
  ahead of `scan.rs`/`jni.rs`.

**A.3b — Rust executor side**
- `dv_reader.rs`, `kernel_scan.rs`, `planner.rs` (replaces the A.2 stub). `planner` ↔
  `kernel_scan` are mutually dependent — they cannot be separated.
- Story: "given a task, read through delta-kernel-rs, apply transform + DV." 35 unit tests.

**A.4a — Scala claim/decline** (parallel with A.3x; needs only A.2)
- `DeltaReflection.scala` (ship complete, incl. CDF accessors — inert), `DeltaScanMetadata`,
  `CometDeltaScanMarker`, `RowTrackingAugmentedFileIndex`, `DeltaScanRule` (+ `DeltaConf` if it
  didn't ship in A.2).
- **Required edit**: `DeltaScanRule` references `CometDeltaNativeScan.ScanImpl` in one fallback
  message — move that constant to `DeltaScanMetadata` (A.4b re-points the serde at it).
- Mergeable because: rule plants the marker, `DeltaIntegration.scanHandler` finds no serde →
  marker falls back to the original scan. Net behavior unchanged; all claim/decline logic
  reviewed in isolation.
- Tests: `CometDeltaTestBase` + a marker-placement/fallback/decline-reason suite (carve the
  decline-path cases out of `CometDeltaNativeSuite`).

**A.4b — Scala execution**
- `CometDeltaNativeScan.scala` (serde), `Native.scala` (JNI decls), `CometDeltaNativeScanExec`,
  `DeltaPlanDataInjector`.
- Interim error semantics (until A.8): the exec/iterator path must not reference #4536's
  `taskFilePaths` overloads — Delta read failures surface as generic `CometNativeException`.
  Carve accordingly.
- Tests: `CometDeltaNativeSuite`, `CometDeltaColumnMappingSuite`, `CometDeltaFeaturesSuite`,
  `CometDeltaCoverageSuite` (+ `ColumnMappingPhysicalNameReproSuite`). This PR is the
  red-to-green moment: gated builds do end-to-end native Delta reads.

**A.5 — CDF**
- `CometDeltaCdfScanExec.scala`, the `CometExecRule` CDF hook hunk (deferred from A.2),
  CDF wiring in the contrib (the Rust `TableChanges` path already landed in A.3x).
- **Also deferred from A.2 (review round 2):** `DeltaIntegration`'s CDF members
  (`isCdfRelation`, `transformCdf`, `convertCdfBinding` + its cache, and the
  `RowDataSourceScanExec` import) — A.2 stripped them as dead code (no caller until the CDF
  hook lands). Re-add them here alongside the CometExecRule CDF arm.
- Tests: `CometDeltaCdcSuite`, `CometDeltaCdfReflectionReproSuite`.

**A.6a — Test battery** *(gated on all extraction PRs + %-path fix being merged)*
- All remaining contrib suites (manifest §12), `delta_contrib_test.yml` (full version),
  `dev/ci/check-suites.py` registration.

**A.6b — Regression harness** *(gated on all extraction PRs + %-path fix being merged)*
- `contrib/delta/dev/diffs/{3.3.2,4.0.0,4.1.0}.diff`, `run-regression.sh`, `run-test.sh`,
  `delta_regression_test.yml`. If the CI-only 4.1.1 pin decision stands (§10.1), it lives here.
- Review focus to pre-empt in the description: CI minutes, diff-maintenance burden per Delta
  version, how a contributor reruns one failing suite locally (`run-test.sh`).

**A.7 — Docs**
- `contrib/delta/docs/*.md` (13 files), `docs/source/user-guide/latest/{delta.md,
  datasources.md, index.rst}`. Audit references against what actually landed (paths/configs may
  have drifted during carving).

**A.8 — Follow-up after #4536 merges**
- Add `perPartitionFilePaths` to `CometScanWithPlanData`, the threading in `operators.scala` /
  `CometExecIterator`, and the Delta exec's implementation → Delta read failures become
  `FAILED_READ_FILE`. Red-green: a Delta corrupt-file test asserting the error class (red
  before, green after).

### Phase 1 gotchas (from project memory)

- Build in the main checkout, never in a worktree agent (duplicate `native/target` ≈ 11G fills
  the disk).
- `cargo build -j 4` always.
- The contrib is **not** a Maven module: JVM builds/tests use `-Pcontrib-delta -pl spark`,
  never `-pl contrib/delta`.
- Local Spark-4.1 contrib tests need `-Dspark.version=4.1.1` while the pom pins 4.1.2.
- Stale-dylib gotcha: after rebuilding native, make sure the refreshed lib is the one the JVM
  tests load.
- Past clean rebases silently duplicated hunks (e.g. the #4525 scheme gate) — after any
  rebase/carve, `git diff` audit the shared files against `ref/delta-complete`.

---

## 5. Phase 2 — Per-unit verification matrix

Run at **every** unit boundary on `feat/delta-split` (cumulative prefix), before the review loop:

| Check | Command (from repo root) | Applies from |
|---|---|---|
| Default native build unchanged | `cd native && cargo build -j 4` | A.1 |
| Gated native build | `cd native && cargo build -j 4 --features contrib-delta` | A.2 |
| Contrib crate unit tests | `cd contrib/delta/native && cargo test -j 4` | A.3a |
| Clippy (all targets, both feature states) | `cargo clippy --all-targets [-—features contrib-delta]` | A.2 |
| Default JVM build + core smoke suites | `./mvnw -pl spark test -Dsuites=...` (a couple of core suites) | A.1 |
| Gated JVM compile | `./mvnw -Pcontrib-delta -pl spark test-compile` (+ `-Dspark.version=4.1.1` on spark-4.1) | A.2 |
| Gate-verify script | `dev/verify-contrib-delta-gate.sh` | A.2 |
| Contrib suites for the unit | scalatest selectors for the suites the unit ships | A.4a |
| Spotless/scalastyle/scalafix | `./mvnw spotless:check` etc., both with and without `-Pcontrib-delta` | A.1 |
| Full lite regression (gated) | targeted suites first, then the long run (raise macOS thread caps first: `kern.maxprocperuid` / `ulimit`) | A.4b, A.5, A.6b |

Also at A.1 and A.2: verify the *default* build's `comet-spark` JAR contains no
`org/apache/comet/contrib` classes and `libcomet` exports no delta symbols (this is what the
gate script asserts — confirm it covers both).

---

## 6. Phase 3 — `/review-comet-pr` loop (after reorganizing, before upstream PRs)

Work **bottom-up** (A.1 first) so fixes ripple forward exactly once.

Per unit:
1. Push the unit's cumulative branch to the fork and open a **draft PR on the fork**
   (`schenksj/datafusion-comet`, base = previous unit's branch, or `main` for A.1). This gives
   the review skill a real PR-shaped diff containing *only that unit's commits*, and dry-runs CI.
2. Run `/review-comet-pr` against it.
3. Triage findings: (a) real → fix; (b) false positive/known trade-off → record the
   justification in the unit's PR-description draft (reviewers will hit the same thought);
   (c) out-of-unit-scope → note for the owning unit.
4. Apply fixes as `fixup!` commits, then fold:
   `git rebase --autosquash --onto <prev-unit-tip> <prev-unit-tip> pr/delta-<unit>`.
5. Re-run the verification matrix (§5) for the unit, re-run `/review-comet-pr`.
6. Repeat until the skill returns **clean** (no actionable findings). Then rebase all later
   unit branches onto the new tip and move to the next unit.
7. Keep a per-unit log in `./.delta-split/review-log-<unit>.md` (untracked): findings, fix
   commit, won't-fix rationale. This becomes review-ammunition for the upstream PRs.

Additionally, before opening each upstream PR: run `/code-review` at medium effort on the unit
diff as a second lens (different failure modes than the comet-specific skill).

---

## 7. Phase 4 — Force-push and umbrella bookkeeping

Once **all** units pass §5 + §6:

1. `feat/delta-split` now is the full work product re-expressed as ~10 clean commit sets.
   Diff-audit it against the reference:
   `git diff ref/delta-complete feat/delta-split -- . ':!DELTA_PR_SPLIT_PLAN.md'` — expected
   differences ONLY: extraction-PR hunks (stripped), `perPartitionFilePaths` (deferred to A.8),
   `ScanImpl` constant location, stub-then-replaced files, 4.1.1 pin location. Anything else is
   a carving error.
2. **Force-push the reorganized stack to `contrib-delta-direct`** (the #4366 branch):
   `git push --force-with-lease origin feat/delta-split:contrib-delta-direct`.
   #4366 then shows the complete diff with commits ≈ review units.
3. Convert #4366 to **draft** and rewrite its description as the umbrella: the sequence table,
   dependency graph, per-unit checklist with PR links as they open, and the extraction-PR gate
   list. #4366 closes when the last unit merges.
4. `feat/delta-kernel-read` stays as the integration-proof branch (§3); `ref/delta-complete`
   plus the tag stay frozen.

---

## 8. Phase 5 — Opening and landing the upstream PRs

- **Open A.1 immediately**; open A.2 as soon as A.1 is approved (it can be opened earlier as a
  draft noting the dependency). From A.3a onward, open each PR when its dependencies are merged
  or clearly converging — at most ~3 sequence PRs open at once to respect reviewer bandwidth.
- Each upstream PR body: what this unit is, what it deliberately does NOT do yet (and which
  later unit does), how it's inert/gated, the verification evidence, and the won't-fix notes
  from the review log.
- After each upstream merge: rebase `feat/delta-split` onto `main`, re-cut the remaining
  `pr/delta-*` branches, re-run the §5 matrix on the next unit. (Fetch+rebase before any push —
  fork branches have diverged between sessions before.)
- Conflict policy with extraction PRs: whichever of {A.1 vs #4535}, {A.2 vs #4525} lands second
  takes the (small) conflict. If an extraction merges mid-sequence, fold its hunks back into the
  affected unit branches at the next rebase.
- **`%`-path fix — DEFERRED, do not open as a standalone PR** (decision 2026-06-10). Verified
  red/green on current `main`: the core `ParquetReadV1Suite` test passes **identically with and
  without** the production change (`object_store` 0.13.2's `Path::from_url_path` already round-trips
  `%`/spaces in local paths). It is a **no-op at the core read level** on current main — no failing
  test justifies a core bugfix PR. Its real necessity (if any) is in the Delta path, so the
  production change folds into **A.6a**, alongside the tests that would actually exercise it
  (`CometDeltaPercentFileNameReproSuite`, `CometDeltaSpecialCharFilenameSuite`). During A.6a carve:
  run those Delta suites WITHOUT the planner/parquet_support change; **only** keep the change if a
  suite goes red without it (real red-green), otherwise **drop it entirely**. Branch preserved:
  `fix/local-path-special-chars` (local, rebased onto main) and `origin/backup/fix-local-path-special-chars`
  (pre-rebase original). NB: that branch's native unit-test regression guard is a flawed micro-repro
  (`Url::from_file_path` round-trips cleanly, so it can't reproduce the bug) — fix or drop it if the
  change is kept.

Merge-order summary: A.1 → A.2 → {A.3a → A.3b ∥ A.4a} → A.4b → A.5 → A.7, with A.6a/A.6b last
(gated on all eight extractions), A.8 whenever #4536 merges.

---

## 9. Phase 6 — End-of-cycle

1. Confirm all eight extractions merged; rebase and land A.6a, then A.6b.
2. Land A.8 (FAILED_READ_FILE parity) with its red-green test.
3. Backport check: memory notes the contrib's broad `CreateArray` decline should be refined per
   #4533 once it merges — verify the contrib code that landed matches the refined version, or
   fold the refinement into A.6a's rebase.
4. Run the full Delta own-suite regression on `main` (gated build) for 3.5/4.0/4.1; record
   results on #4366; close #4366.
5. Delete `feat/delta-split` and the `pr/delta-*` branches; keep `ref/delta-complete` + tag
   permanently (or until a release contains the full feature).

---

## 10. Decision points to raise with maintainers (early, in A.2's description)

1. **Spark 4.1.1 pin**: delta-spark 4.1.0 needs `IgnoreCachedData` (removed in Spark 4.1.2).
   Options: (a) pin the default pom to 4.1.1 (affects all users — as the branch does today), or
   (b) **recommended**: keep the pom at 4.1.2 and apply `-Dspark.version=4.1.1` only in Delta CI
   workflows + document the constraint in `delta.md`. Decide in A.2; implement in A.6b/A.7.
2. **Proto placement**: all `Delta*` messages land in A.2 (wire format reviewed once, early).
   Alternative if reviewers object to dead proto: split `DeltaScanTask*` messages into A.3a.
3. **Landing target**: Option A (sequence straight to `main`, gate makes intermediates safe) vs
   Option C (same sequence into an apache-side `feature/delta-contrib` branch). Default: main.
4. **CI cost** of `delta_contrib_test.yml` / `delta_regression_test.yml` (trigger paths,
   matrix size, schedule vs per-PR) — pre-negotiate in A.2/A.6 descriptions.

---

## 11. Risk register

| Risk | Mitigation |
|---|---|
| Carving error: a stripped hunk was actually needed → unit doesn't compile/behave | Per-unit verification matrix (§5); final whole-stack diff audit (§7.1) |
| Silent duplicate hunks after rebases (has happened: #4525 gate) | Post-rebase `git diff` audit of shared files vs `ref/delta-complete` |
| Extraction PR review stalls past A.6 readiness | A.6a/A.6b are last and low-effort; if truly stuck, add temporary skip-entries to `dev/diffs` with TODO(#PR) markers — explicitly second-choice |
| `Cargo.lock` churn across A.2/A.3a/A.3b confuses reviewers | Regenerate lockfiles per unit; call out in PR body that lockfile is mechanical |
| Interim error-reporting regression (no FAILED_READ_FILE until A.8) | Documented in A.4b PR body; A.8 queued; affects error class only, not correctness |
| Reference branch drifts from what PRs deliver (post-review fixes) | Review-loop fixes happen on `feat/delta-split` units; after the cycle, `ref/delta-complete` is historical — the integration-proof branch (`feat/delta-kernel-read` + periodic main merges) is the living end-state check |
| Reviewer asks for a different split mid-cycle | Units are commit sets on one branch — re-cutting boundaries is a rebase, not a rebuild |
| macOS thread-cap exhaustion on full regression | Raise `kern.maxprocperuid`/`kern.maxproc` + `ulimit` each boot before the long run |
| Multi-session execution drift (context lost between sessions, half-carved unit resumed wrong) | §14 state log appended every session; resume by reading the log, then diff-auditing the in-progress unit vs `ref/delta-complete` before continuing |

---

## 12. Appendix — Complete file → PR manifest

Legend: `[carve]` = hand-carved shared file (strip extraction hunks); everything else is a
whole-file copy from `ref/delta-complete`.

### Not in the sequence (land via the eight extraction PRs)
| File | Owner |
|---|---|
| `native/jni-bridge/src/errors.rs`, `native/common/src/error.rs` | #4536 |
| `spark/.../SparkErrorConverter.scala`, `Shim SparkErrorConverter` ×3, `SparkErrorConverterSuite` | #4536 |
| `spark/.../CometExecIterator.scala`, `CometExecRDD.scala`, `CometNativeScanExec.scala` (their branch hunks), `CometExecSuite` additions | #4536 (verify residuals during carve) |
| `operators.scala` O(1)-registry hunks, `PlanDataInjectorSuite.scala` | #4535 |
| `spark/.../serde/arrays.scala`, `CometArrayExpressionSuite` additions | #4533 |
| `NativeUtil.scala`, `ArrowWriters.scala`, `comet/util/Utils.scala`, `UtilsSuite` | #4532 |
| `CometScanRule` scheme-gate hunks, `NativeBase.java` (+11), `native/core/src/lib.rs` (+25), `FakeHdfsSchemeFileSystem.java`, `CometScanSchemeFallbackSuite` | #4525 |
| `native/shuffle/src/spark_unsafe/{row.rs,unsafe_object.rs}`, `CometColumnarShuffleSuite` additions | #4524 |
| ~~`CometParquetPercentPathSuite.scala`, `pr_build_*.yml`~~ → **the `%`-path production change is DEFERRED into A.6a** (no-op on current main; see §8). Branch `fix/local-path-special-chars` kept for reference only. | %-path fix (deferred) |

### A.1 — Core SPI
- `spark/.../sql/comet/operators.scala` [carve]: `CometScanWithPlanData` trait (no
  `perPartitionFilePaths`), leaf/trait matching, contrib injector slot (vs main's registry)
- `spark/.../rules/CometPlanAdaptiveDynamicPruningFilters.scala` [carve]: trait DPP arm
- New focused test suite for slot + matching

### A.2 — Build gate + inert wiring
- `pom.xml` [carve: `delta.version` only — NOT the 4.1.1 pin], `spark/pom.xml`
- `native/Cargo.toml`, `native/core/Cargo.toml`, `native/Cargo.lock`
- `native/proto/src/proto/operator.proto` [carve: Delta messages]
- `native/core/src/execution/planner.rs` [carve], `planner/delta_scan.rs`,
  `planner/operator_registry.rs` [carve], `jni_api.rs` [carve]
- `spark/.../rules/DeltaIntegration.scala`
- `spark/.../rules/CometExecRule.scala` [carve: marker hook only]
- `spark/.../rules/CometScanRule.scala` [carve: delta delegation + metadata-col reorder, vs main]
- `.gitignore`, `dev/verify-contrib-delta-gate.sh`, minimal gate-check workflow
- Stubs: `contrib/delta/native/{Cargo.toml,Cargo.lock,src/lib.rs,src/error.rs,stub planner}`,
  `contrib/delta/src/main/scala/.../DeltaConf.scala`

### A.3a — Rust driver side
- `contrib/delta/native/src/{error,engine,predicate,scan,jni}.rs` (+Cargo/lock updates)

### A.3b — Rust executor side
- `contrib/delta/native/src/{dv_reader,kernel_scan,planner}.rs` (+lock updates)

### A.4a — Scala claim/decline
- `contrib/delta/.../delta/{DeltaReflection,DeltaScanMetadata,DeltaScanRule}.scala`
  (+`DeltaConf` if not in A.2), `.../sql/comet/CometDeltaScanMarker.scala`,
  `.../delta/RowTrackingAugmentedFileIndex.scala`
- Edit: move `ScanImpl` constant into `DeltaScanMetadata`
- Tests: `CometDeltaTestBase.scala` + marker/fallback/decline suite

### A.4b — Scala execution
- `contrib/delta/.../delta/{CometDeltaNativeScan,Native}.scala`,
  `.../sql/comet/{CometDeltaNativeScanExec,DeltaPlanDataInjector}.scala`
- Tests: `CometDeltaNativeSuite`, `CometDeltaColumnMappingSuite`, `CometDeltaFeaturesSuite`,
  `CometDeltaCoverageSuite`, `CometDeltaColumnMappingPhysicalNameReproSuite`

### A.5 — CDF
- `contrib/delta/.../sql/comet/CometDeltaCdfScanExec.scala`
- `CometExecRule.scala` [carve: CDF hook hunk]
- Tests: `CometDeltaCdcSuite`, `CometDeltaCdfReflectionReproSuite`

### A.6a — Test battery
- Remaining suites: `CometDeltaCredentialAuditSuite`, `CometDeltaDefaultRowCommitVersionReproSuite`,
  `CometDeltaDeleteWithDVReproSuite`, `CometDeltaDppReproSuite`, `CometDeltaEdgeCaseRegressionSuite`,
  `CometDeltaFilterPushdownAuditSuite`, `CometDeltaGeneratedColumnPartitionFilterReproSuite`,
  `CometDeltaMergeMetricsReproSuite`, `CometDeltaMetadataColumnAuditSuite`,
  `CometDeltaNestedArrayStructReproSuite`, `CometDeltaPartitionCoercionAuditSuite`,
  `CometDeltaPercentFileNameReproSuite`, `CometDeltaRegressionReproSuite`,
  `CometDeltaRowIdColumnCollisionReproSuite`, `CometDeltaRowTrackingMaterializedSuite`,
  `CometDeltaRowTrackingMergeReproSuite`, `CometDeltaScanConfAuditSuite`,
  `CometDeltaSchemaChangeReproSuite`, `CometDeltaSpecialCharFilenameSuite`,
  `CometDeltaStatsSkippingReproSuite`, `CometDeltaTimeTravelReproSuite`,
  `CometDeltaTypeRoundTripAuditSuite`, `sql/delta/CometDeltaCheckpointFilterReproSuite`
- `.github/workflows/delta_contrib_test.yml` (full), `dev/ci/check-suites.py`

### A.6b — Regression harness
- `contrib/delta/dev/diffs/{3.3.2,4.0.0,4.1.0}.diff`, `contrib/delta/dev/run-regression.sh`,
  `contrib/delta/dev/run-test.sh`, `.github/workflows/delta_regression_test.yml`
- (If §10.1 lands as CI-only pin: the `-Dspark.version=4.1.1` lines live in these workflows)

### A.7 — Docs
- `contrib/delta/docs/{README,01-overview,02-planning,03-native-execution,04-design-decisions,
  05-build-and-deploy,06-fallback-and-ops,07-spark35-feasibility,08-known-limitations,
  10-iceberg-style-kernel-read,11-kernel-read-coherence-audit,12-elimination-evaluation}.md`
- `docs/source/user-guide/latest/{delta.md,datasources.md,index.rst}`

### A.8 — Follow-up (post-#4536)
- `operators.scala`, `CometExecIterator.scala` threading; `CometScanWithPlanData.
  perPartitionFilePaths`; `CometDeltaNativeScanExec` impl; corrupt-file red-green test

---

## 13. Execution discipline (model choice + session continuity)

**The plan is the safety system, not the executor.** Correctness comes from the §5 verification
matrix, the gate script, the §6 review loop, and the §7.1 whole-stack diff audit — not from the
judgment of whoever (or whatever model) performs the steps. The plan is written to be executed
by Opus 4.8 (which built most of the underlying branch; see commit trailers) or any comparable
model. Model choice changes the *rate of small mistakes the guardrails must catch*, and the
cost/speed profile — not whether the plan can be executed.

Where the judgment density actually is:
1. **Phase 1 hand-carving** of the eight shared files (hunk attribution: extraction PR vs Delta
   sequence) and the decoupling rework. Highest risk of subtle error; backstopped by per-unit
   builds and the Phase 4 diff audit.
2. **Phase 3 review triage** — classifying `/review-comet-pr` findings (real fix vs documented
   won't-fix vs out-of-unit-scope) and keeping autosquash fixes inside unit boundaries.

Guidance:
- If mixing models, spend the strongest one on the Phase 1 carving sessions and the first one
  or two Phase 3 triage rounds; once the pattern is established, drop back. Phases 0, 2, 4–6
  are mechanical — Opus fast mode is the better economic choice for long build/regression
  babysitting.
- **One-unit-at-a-time (hard rule).** Complete the unit's carve → run its §5 verification →
  commit, *before* starting the next unit's carve. Never batch multiple carves between
  verifications — batching is how attribution errors compound past the point where the failing
  build identifies the culprit.
- **Session continuity (hard rule).** This plan spans many sessions; reliability across session
  boundaries depends on the §14 state log, not on conversational memory. At the end of every
  working session, append a dated entry. At the start of every session, read the log first; if
  a unit was in progress, diff-audit its current state against `ref/delta-complete` before
  writing anything new.

---

## 14. Execution state log

Append-only. Newest entry at the top. Entry template:

```
### YYYY-MM-DD (session N — model used)
- Phase/unit in progress:
- Last fully verified boundary (unit + §5 checks passed):
- Commits made this session (branch + short shas):
- Open carve questions / hunk-attribution uncertainties:
- Pending decisions or upstream events (PR merges, review feedback):
- Next action:
```

### 2026-06-21 (session 5f — Opus 4.8) — stack 2.12 fix; PR#2 fully fixed (gate + 2 DV/CDF bugs); A.4b carved + reviewed clean (END-TO-END NATIVE READS)
- **Stack 2.12 fix:** the A.2-review `scanHandler` cache (`val handler = serdeCls.flatMap{...}`)
  didn't compile on Scala 2.12 (Spark 3.4) — existential `CometOperatorSerde[_]` needs an explicit
  type annotation 2.13 infers. This is core (DeltaIntegration), so it broke EVERY stacked fork PR's
  3.4 builds. Fixed (`val handler: Option[CometOperatorSerde[_]] = ...`), amended into A.2, cascade-
  rebased A.3a/A.3b/A.4a, force-pushed all. New heads after rebase. (Memory: build -Pspark-3.4 too.)
- **PR #2 (monolith) fully fixed:** (a) build-gate = the pipefail/SIGPIPE gate-script bug I fixed in
  A.2, never backported -> backported (`69dfb05ff`); (b) DELETE-on-DV crash = empty Delta scan gave 0
  partitions while outputPartitioning floors to 1 -> `taskGroups = Seq(Seq.empty)` (`b803010e0`,
  CometDeltaNativeScanExec = A.4b file); (c) CDF orderBy Nx row dup = CometDeltaCdfScanExec wasn't
  `CometScanWithPlanData` so the parent block's findAllPlanData skipped its sub-ranges -> implements
  the trait (`47ac11144`, CometDeltaCdfScanExec = A.5 file). Both Delta fixes flow forward into A.4b/A.5.
  (Spark-4.0-exec 403 was transient infra.) See [[project_delta_empty_scan_and_cdf_dup]].
- **A.4b carved** onto `pr/delta-A4a-scala-claim` → branch `pr/delta-A4b-scala-exec` @ **`8d14cdb5e`**,
  fork review draft **#8**. 11 files +3879/-59. Serde + native exec — END-TO-END native Delta reads.
  Carved: serde dropped ScanImpl + convertCdf (A.5); exec stripped perPartitionFilePaths (A.8 interim
  error semantics), kept the taskGroups fix + CometScanWithPlanData. Test base re-gained native helpers;
  CometDeltaMarkerSuite rewritten (serde present -> claim engages native, not a leftover marker).
- **§5 (all green):** gated JVM test-compile; **60 contrib tests** across 6 suites; spotless/scalastyle;
  check-suites; gate-verify (default 0 Delta symbols). A.4b touches ONLY contrib/delta/src.
- **Review (manual + /code-review high, 15 agents, 3 refuted / 2 reported):** fixed orphaned imports
  (UTF8String/DateTimeUtils left after convertCdf removal -> breaks -Pstrict-warnings) + dead planData*
  aliases (0 callers, pre-existing monolith dead code).
- **PR #5 rust-test re-run:** BLOCKED — its CI run is stuck queued on shared fork runners; gh refuses
  to re-run a failed job while the run is in progress. Re-run the moment it completes. (Default-build
  job, unaffected by A.3a's excluded contrib crate -> transient.)
- **Next action:** carve **A.5** (CDF): re-add convertCdf to the serde, CometDeltaCdfScanExec (already
  carries the Nx-dup fix from PR#2 work), the CometExecRule CDF hook hunk (deferred from A.2), and
  DeltaIntegration's CDF methods (deferred from A.4a). Tests: CometDeltaCdcSuite (has the orderBy
  red-green), CometDeltaCdfReflectionReproSuite.

### 2026-06-21 (session 5e — Opus 4.8) — A.4a carved + reviewed clean; JVM claim/decline layer
- **A.4a carved** onto `pr/delta-A3b-rust-executor` → branch `pr/delta-A4a-scala-claim` @ **`ae3ec8d24`**,
  fork review draft **#7**. 8 files +2514. Survey agent mapped it. Verbatim main files (DeltaReflection,
  DeltaScanRule, RowTrackingAugmentedFileIndex, CometDeltaScanMarker, DeltaScanMetadata) + the ONE
  required edit (ScanImpl moved off CometDeltaNativeScan (4b) into `object DeltaScanMetadata`;
  DeltaScanRule:306 re-pointed). Tests: trimmed CometDeltaTestBase (3 serde/exec helpers → 4b) + NEW
  CometDeltaMarkerSuite. check-suites.py: exempt contrib suites.
- **Inert by design:** DeltaScanRule plants CometDeltaScanMarker; no serde → CometExecRule.scanHandler
  None → marker stays + executes vanilla. Delta reads run on vanilla Spark. No core/A.2 edits (A.2's
  reflective wiring already reaches DeltaScanRule$ + the marker).
- **§5 (all green):** gated JVM test-compile, **CometDeltaMarkerSuite 3/3** (marker planted = red on
  A.2 / green here; fallback result-correct; input_file_name declines, no marker), check-suites,
  spotless+scalastyle, gate-verify (default still 0 Delta symbols). Ran under spark-4.0/delta-4.0.0
  (avoids the local 4.1.1 pin quirk); copied contrib libcomet into the test classpath.
- **Carve invariants:** no deferred-class compile refs; A.4a touches ONLY contrib/delta/src +
  check-suites.py.
- **Review (manual + /code-review high, 25 agents, 6 refuted / 3 reported — ALL in the hand-authored
  TEST code; verbatim modules clean):** fixed a CONFIRMED coverage gap (test 2 only compared rows,
  never asserted the marker was planted → a claim regression would ship green; now asserts marker +
  correctness), aligned the marker matcher to the production FQN (getName, not getSimpleName), removed
  a dead+buggy non-AQE assertNativePlanContains helper.
- **Next action:** carve **A.4b** (serde + exec — the red-to-green native-read unit): CometDeltaNativeScan
  serde, Native.scala JNI decls, CometDeltaNativeScanExec, DeltaPlanDataInjector. Re-adds the native-read
  test helpers (assertDeltaNativeMatches etc.) to CometDeltaTestBase; re-points its serde at
  DeltaScanMetadata.ScanImpl (drop its own ScanImpl def). Needs A.3b + A.4a. Interim error semantics
  (until A.8): exec/iterator must NOT reference #4536's taskFilePaths overloads.

### 2026-06-21 (session 5d — Opus 4.8) — A.3b carved + reviewed clean; Rust side complete
- **A.3b carved** onto `pr/delta-A3a-rust-driver` → branch `pr/delta-A3b-rust-executor` @ **`38e2312c6`**,
  fork review draft **#6**. 7 files +2530/-62. Completes the contrib native crate: verbatim executor
  modules (planner.rs stub→real 561, kernel_scan.rs 1553, dv_reader.rs 388), lib.rs re-adds the
  dv_reader/kernel_scan decls + DeltaScan/DeltaScanCommon proto re-exports (trimmed in 3a), Cargo.toml
  re-adds executor deps. Kept A.3a improvements (crate version 0.18.0 not the monolith's stale 0.17.0;
  clarified planner doc-link). Both lockfiles regenerated.
- **§5 (all green):** gated native build, **89 in-crate unit tests** (54 driver + 35 executor), default
  native build unchanged, clippy ×2 feature states, gate-verify script (contrib libcomet +13 MB),
  cargo fmt. (Disk filled mid-run from the delta_kernel tree ×2 — cleaned the contrib standalone target
  after the 89 tests passed; the gated build alone proves the later dep cleanup.)
- **Carve invariants:** stub planner FULLY replaced (no NotImplemented left); A.3b touches ONLY
  contrib/native + native lockfile (no core, no gate script, no JVM); unchanged shim links the real
  4-arg planner.
- **Review (manual + /code-review high, 29 agents, 1 refuted / 5 reported — all Cargo.toml, no
  correctness bugs):** removed FOUR dead deps the monolith carried (verified unused by grep AND by the
  gated rebuild compiling without them): `roaring` (also killed a duplicate 0.10.12 vs 0.11.4),
  `datafusion-datasource`, direct `parquet` dep, datafusion `parquet` feature — all parquet I/O goes
  through delta_kernel; the exec is DeltaKernelScanExec. Fixed a stale `dv_filter`→`dv_reader` comment.
  These dead deps also exist in the monolith (byte-identical modules) → cleanup should flow back on
  reconciliation.
- **Rust side of the contrib is now COMPLETE** (A.3a driver + A.3b executor == the monolith's
  contrib/native modulo the version + doc-link + dep cleanup).
- **Next action:** carve **A.4a** (Scala claim/decline + reflection: DeltaReflection, DeltaScanMetadata,
  CometDeltaScanMarker, RowTrackingAugmentedFileIndex, DeltaScanRule) onto `pr/delta-A3b-rust-executor`.
  Needs only A.2 (parallel with A.3x), so it could also branch from A.2 — but stacking on A.3b keeps
  one linear chain. Required edit: move `ScanImpl` constant into DeltaScanMetadata (§4 A.4a).

### 2026-06-20 (session 5c — Opus 4.8) — PR#2 smoke fixed (version skew); A.3a carved + reviewed clean
- **PR #2 (monolith) smoke failures diagnosed + fixed:** the `Delta Lake Regression Tests /
  smoke/delta-*` jobs failed on the fresh head — NOT a code regression, a VERSION SKEW. The
  regression diffs hardcoded `val cometVersion = "0.17.0-SNAPSHOT"`; the main-sync bumped the repo to
  0.18.0-SNAPSHOT, so the smoke harness couldn't resolve `comet-spark-...-0.17.0-SNAPSHOT.pom`. Fixed
  on `feat/delta-kernel-read`: (1) bumped the 3 diffs to 0.18.0-SNAPSHOT (`48a84f08d`), then (2) made
  `run-regression.sh` auto-derive cometVersion from the pom + overwrite the injected line post-apply
  (`940fea62f`) so future bumps never re-break it (pipefail-safe extraction). This `run-regression.sh`
  improvement flows into A.6b when carved.
- **A.3a carved** onto `pr/delta-A2-buildgate` → branch `pr/delta-A3a-rust-driver` @ **`1a774d02d`**,
  fork review draft **#5**. 10 files +4374/-50. Survey agent mapped the carve. Verbatim driver
  modules (error/engine/predicate/scan/jni = 2622 lines), carved lib.rs (keeps planner stub, drops
  dv_reader/kernel_scan), TRIMMED real Cargo.toml (only driver deps; executor deps → A.3b). Gate
  cargo-tree assertion re-tightened to require delta_kernel (now real; contrib libcomet +11 MB).
- **§5 (all green):** gated native build, **54 in-crate unit tests**, default native build unchanged,
  clippy ×2 feature states, gate-verify script, cargo fmt.
- **Review (manual + /code-review high, 22 agents, 6 refuted / 6 survived) — 3 fixed, 2 documented:**
  fixed dead `crate::proto` re-export (DeltaScan/DeltaScanCommon → A.3b), unused datafusion `parquet`
  feature (→ A.3b), misleading `[plan_delta_scan]` doc-link. Documented (NOT fixed, verbatim monolith
  code, latent behind DV invariants): scan.rs:691/693 unguarded DV offset/sizeInBytes Arrow reads —
  candidate for a separate monolith-level hardening pass, not an A.3a divergence.
- **Carve invariants verified:** driver set has zero deferred-module refs; A.3a touches ONLY
  contrib/native + native lockfile + gate script; stub planner.rs byte-unchanged.
- **Next action:** carve **A.3b** (executor side: dv_reader, kernel_scan, real planner — replaces the
  stub) onto `pr/delta-A3a-rust-driver`. It re-adds the deferred Cargo deps (parquet, roaring,
  datafusion-datasource, futures, chrono*, comet-common, tokio) + the proto re-exports trimmed in 3a.

### 2026-06-20 (session 5b — Opus 4.8) — A.1 opened upstream (#4700); A.2 carved + reviewed clean
- **A.1:** opened upstream as **#4700** (with AI-disclosure footer; new standing rule: every PR body
  ends with it). Apache CI is `action_required` (awaiting a maintainer to approve workflow runs for a
  non-committer PR — NOT a failure). Fork review-draft #3 closed as superseded.
- **Loose end fixed:** fork PR #2 (monolith, `feat/delta-kernel-read`) was stranded on the pre-main-sync
  commit `f442361c3` (I'd pushed the merge to `contrib-delta-direct`/#4366 but not `feat/delta-kernel-read`).
  Pushed `9f29732a8` (FF) → #2 MERGEABLE again. Its stale `delta-contrib/*` CI failures were from the old
  commit; A.1/#4700 is unaffected (zero Delta surface).
- **A.2 carved** onto `pr/delta-A1-spi` → branch `pr/delta-A2-buildgate` @ **`f2ad00c29`**, fork review
  draft **#4** (base = A.1 branch, A.2-only diff). 20 files +4020/-14. An Explore agent produced the
  precise carve map (whole-checkout vs partial-carve vs stub vs copy); see `.delta-split/review-log-A2.md`.
  Proto `delta_scan = 118`. Stub contrib crate authored (real impl deferred). pom.xml carries
  `delta.version` only (NO 4.1.1 pin, §10.1). CometExecRule = marker hook only (CDF → A.5).
- **§5 (all green):** default + gated native build, clippy ×2 feature states, gate-verify script (all
  layers), gated + default JVM compile, spotless/scalastyle (both profiles), cargo fmt.
- **Review round (manual + /code-review high, 28 agents, 9 refuted / 6 survived) — 5 fixed, 2 accepted:**
  fixed a VACUOUS cargo-tree gate check (no anti-vacuous guard → false pass on cargo failure), a
  pipefail+`grep -q` SIGPIPE misfire in the gate script (→ here-strings), dead CDF block in
  DeltaIntegration (→ stripped to A.5, see A.5 note), scanHandler per-call MODULE$ re-resolve
  (→ @volatile cache), raw Class.forName (→ ClassLoaders.loadClass). Accepted+documented: a Spark-3.4
  diagnostic-only fallback-reason string change (rare metadata-col + AQE overlap, matches monolith) and
  DeltaConf's unread-in-A.2 config entries (the contrib's config leaf).
- **Next action:** carve **A.3a** (Rust driver side: error/engine/predicate/scan/jni) onto
  `pr/delta-A2-buildgate`, OR wait for #4700 review. Open A.2 upstream once A.1 is approved/merged (§8).

### 2026-06-20 (session 5 — Opus 4.8) — maintainers OK'd the plan; #4366 re-synced; A.1 refreshed onto current main + review round 2 clean
- **Phase/unit:** A.1 (Core SPI) refreshed onto post-#4535 main, re-reviewed clean. Maintainers
  comfortable with the split plan — execution greenlit. A.1 still fork-local (PR #3); upstream open
  pending the user's go.
- **Upstream events:** dependent PRs **#4524, #4533, #4535, #4536 all merged** to `apache/main`
  (now `9c69f30b0`). #4366 went CONFLICTING; resolved (see below).
- **#4366 main-sync** (branch `contrib-delta-direct` / `feat/delta-kernel-read`, merge `9f29732a8`):
  3 textual + 1 semantic conflict resolved. unsafe_object.rs → main's corrected comment;
  CometNativeScanExec → keep `override def perPartitionFilePaths` (trait declares it in the
  monolith); CometArrayExpressionSuite → main's #4533 tests. **Semantic:** auto-merge DUPLICATED
  `injectorsByKind` (delta's reflective `injectors` val + #4535's O(1) lookup) — removed the dup.
  Contrib-enabled `test-compile` BUILD SUCCESS. Pushed FF; #4366 → MERGEABLE. (Confirmed
  merged `arrays.scala` carries ONLY #4533's refined CreateArray decline — the old
  "contrib over-declines, backport #4533" note is now RESOLVED.)
- **A.1 refresh:** clean `git rebase upstream/main` of `aa0fd12c5`. The reflective-`injectors`-val
  hunk replayed on top of #4535's untouched `injectorsByKind` with **no duplication** — result ==
  the monolith's reconciled state. Diff shape unchanged (6 files). Fork `main` FF'd to `9c69f30b0`
  (clean PR base).
- **§5 (all green):** default `test-compile` SUCCESS; `CometScanWithPlanDataSuite` 2/2;
  `CometJoinSuite` 28/28 (fresh default debug libcomet copied to `spark/target/classes/.../darwin/
  aarch64`); spotless+scalastyle 0 errors; `check-suites.py` (py3.13!) exit 0. `CometLeafExec`
  verified to have EXACTLY the 3 removed subclasses ⇒ `case _: CometLeafExec` is a provable strict
  superset.
- **Review round 2:** `/review-comet-pr` (manual) + `/code-review high` (31-agent workflow: 8
  refuted, 6 survived). 2 fixes folded into A.1 commit: (a) reflective catch ladder now warns on
  `NoSuchField`/`IllegalAccess` (misbuild diagnosable; only `ClassNotFound` stays silent); (b)
  dropped the test's passthrough assertion that duplicated `PlanDataInjectorSuite` (+ removed unused
  `Operator` import). 4 PLAUSIBLE correctness findings are **inert on default builds AND on the real
  Delta scan** (verified vs monolith: `DeltaPlanDataInjector.opStructCase=DELTA_SCAN` distinct;
  `CometDeltaNativeScanExec extends CometLeafExec`) → documented as A.4b forward-notes in
  `.delta-split/review-log-A1.md`.
- **Commits:** A.1 == `feat/delta-split` == `pr/delta-A1-spi` @ **`fc48c0cfe`** (force-pushed).
  #4366 head `9f29732a8`. Local+fork `main` @ `9c69f30b0`.
- **A.1 OPENED UPSTREAM:** apache/datafusion-comet **#4700** (ready-for-review, not draft; head
  `schenksj:pr/delta-A1-spi` @ `fc48c0cfe` → apache `main`; 6 files +185/-16; MERGEABLE). Title:
  `feat: core SPI for contrib leaf scans (CometScanWithPlanData) [Delta contrib split, part 1]`.
  Body leads with the breakup framing + roadmap, links #4366/#4535/#4536/#3510. Umbrella #4366
  commented (issuecomment-4760525806) linking #4700. Fork review-draft #3 CLOSED as superseded.
- **Next action:** carve **A.2** (build gate + inert wiring) onto `feat/delta-split` — proto
  `delta_scan = 118`, NO 4.1.1 pin in default pom (§10.1), stub contrib crate, gate-verify script
  + CI job. Watch #4700 review.

### 2026-06-13 (session 4 — Opus 4.8) — Phase 1 Unit A.1 carved + fork draft PR + review loop
- **Phase/unit:** A.1 (Core SPI) carved, verified, fork draft PR open. First split unit done locally.
- **Branches:** `feat/delta-split` (from `upstream/main` 0031c60d6) == `pr/delta-A1-spi` @ **`aa0fd12c5`**.
  Fork `main` fast-forwarded to upstream `0031c60d6` (clean PR base). Local `main` also synced.
- **Fork draft PR: schenksj/datafusion-comet#3** (base `main`), title `feat: core SPI for contrib
  leaf scans (CometScanWithPlanData)`. 6 files, +192/-16.
- **A.1 carve (4 prod/test + 2 CI files):**
  - `operators.scala`: reflective `DeltaPlanDataInjector$` slot against main's O(N) loop (import
    `Logging`, `extends Logging`); `foreachUntilCometInput` → `case _: CometLeafExec`;
    `findAllPlanData` → `case s: CometScanWithPlanData`; new `trait CometScanWithPlanData` WITHOUT
    `perPartitionFilePaths`.
  - `CometNativeScanExec.scala`: `with CometScanWithPlanData` ONLY (the perPartitionFilePaths/triple
    hunks are A.8, excluded).
  - `CometPlanAdaptiveDynamicPruningFilters.scala`: trait DPP in-place arm.
  - NEW `CometScanWithPlanDataSuite` (trait defaults + reflective-slot absence; disjoint from
    #4535's `PlanDataInjectorSuite`).
  - `pr_build_{linux,macos}.yml`: register the new suite (preflight requires it — A.1's own hunk).
- **Excluded hunks confirmed absent in A.1:** `perPartitionFilePaths`, `opStructCase`,
  `injectorsByKind` (those are A.8 / #4535).
- **§5 verification (all green):** new suite 2/2; `CometJoinSuite` smoke 28/28 (native scan fusion);
  spotless + scalastyle BUILD SUCCESS; `check-suites.py` exit 0; A.1 has zero native changes.
  Had to **build a clean default-features `libcomet`** first — the staged June-10 contrib lib caused
  spurious `NoClassDefFound CometBatchIterator` join failures (stale-dylib gotcha). Disk freed to ~25G.
- **Review loop:** `/review-comet-pr` (adapted — structural PR, not expressions) + `/code-review`
  high (3 finders). No correctness bugs. Fixes folded: inline-FQN `Logging`→import; PR title→`feat:`.
  3 forward-notes documented as won't-fix in A.1 (all match the monolith): `hasScanInput` not
  generalized (metrics-only, A.4b/A.8); DPP case-4 guard asymmetry (no contrib scan in A.1); single-
  slot reflective lookup (altitude). See `.delta-split/review-log-A1.md`.
- **CI:** PR MERGEABLE. Fork Preflight/Analyze still queued (shared runners) at session end — local
  equivalents all pass, so expected green. Verify when it lands.
- **Carve-attribution learning for §12:** `CometNativeScanExec.scala`'s trait-extension hunk is A.1
  (only its perPartitionFilePaths hunks are #4536/A.8); `pr_build_*.yml` get a 1-line A.1 hunk
  (suite registration) regardless of the file's other-unit ownership.
- **Next action:** confirm #3 CI green; then either open A.1 **upstream** (apache) or proceed to carve
  **A.2** (build gate) onto `feat/delta-split`. A.2 proto must use `delta_scan = 118` (see session 3).

### 2026-06-13 (session 3 — Opus 4.8) — upstream sync #2 (real code conflicts) to clear #4366
- apache/main advanced +30 to `0031c60d6`; #4366 went CONFLICTING. Merged `upstream/main` into
  `feat/delta-kernel-read` (merge `f442361c3`, not rebase). 6 conflicts resolved + verified:
  - **`operator.proto`: upstream took field 117 for `BroadcastNestedLoopJoin`** (collided with
    Delta's `delta_scan`). Kept BNLJ=117, **moved `DeltaScan` to field 118**. ⇒ **A.2's proto carve
    must use `delta_scan = 118`.** Updated docs 01-overview/02-planning to 118.
  - `operator_registry.rs` + `jni_api.rs`: kept both new match arms (BNLJ + DeltaScan).
  - **`operators.scala`: upstream refactored the CometExecRDD construction** into
    `buildNativeContext(): NativeExecContext` + `executeColumnarWithContext(ctx)`. Re-threaded Delta's
    `perPartitionFilePaths` through it: added a field to the `NativeExecContext` case class (default
    `Array.empty`), compute it in `buildNativeContext`, pass `ctx.perPartitionFilePaths` to the RDD
    ctor. ⇒ **A.8 (perPartitionFilePaths threading) must target this new ctx structure, not the old
    inline body.** A.1's `CometScanWithPlanData` trait + `executeColumnarWithContext`/`buildNativeContext`
    matching are unaffected in shape.
  - `CometArrayExpressionSuite.scala`: kept both tests (the nullability-divergent CreateArray-decline
    test is #4533's; the ansi GetArrayItem-on-null-split is upstream's).
  - `Cargo.lock`: took upstream's lock; `cargo check` re-added Delta deps; consistent under `--locked`.
  - Non-conflict scare: rust-analyzer flashed E0107 on `planner.rs` `init_datasource_exec` mid-merge —
    a transient cascade from the conflicted proto enum; def=call=16 args, false alarm.
- Verified: native `cargo check` (default) green; spark `test-compile` (JDK-17 flags) BUILD SUCCESS.
  NOT verified: gated `--features contrib-delta` native build (merge didn't touch gated code; renumber
  is variant-name-safe) and full regression — proper §3 re-verify still pending.
- Pushed FF to `origin/feat` + `contrib-delta-direct`; #4366 back to MERGEABLE.
- **Process note:** `git add -A` accidentally staged `DELTA_PR_SPLIT_PLAN.md` into the merge commit;
  caught and removed via `git rm --cached` + `--amend`. Never `git add -A` with this file untracked —
  use explicit paths or it leaks into #4366.

### 2026-06-10 (session 2b — Opus 4.8) — upstream sync to clear #4366 conflict
- #4366 showed a `.gitignore` conflict (apache/main advanced +8 to `523ffb6c9`; feat 107 ahead / 8 behind).
  Merged `upstream/main` into `feat/delta-kernel-read` (merge `937b97760`, NOT rebase — per §3). Only
  `.gitignore` conflicted; all else auto-merged (Cargo.lock/toml, pom.xml, CometScanRule.scala, pr_build_*.yml,
  CometExpressionSuite.scala).
- Resolved `.gitignore` by **dropping leaked local-only junk** (`pr-4366-body*.md`, `# Claude Code local
  runtime state`, `.claude/scheduled_tasks.lock`, `.claude/*.lock`) and keeping upstream's `docs/superpowers/`.
  → carve A.2's `.gitignore` (gate entries) from THIS cleaned version, not the old one.
- Pushed FF to `origin/feat/delta-kernel-read` and to `contrib-delta-direct`. #4366 now `MERGEABLE`
  (state BLOCKED only because it's a draft). **Not** build-verified post-merge — a full integration
  re-verify (build + targeted suites) per §3 is the proper follow-up before trusting end-state green.

### 2026-06-10 (session 2 — Opus 4.8) — %-path fix investigation
- Asked to open the `%`-path fix as the 8th extraction PR. Rebased `fix/local-path-special-chars`
  onto `main` (clean, patch-id identical). Corrected its native unit test (the regression guard was
  a flawed micro-repro — `Url::from_file_path` round-trips `%`/spaces cleanly on object_store 0.13.2,
  so it never reproduced the bug). Built debug `libcomet`, staged into `spark/target/classes/.../darwin/aarch64`.
- Ran `ParquetReadV1Suite` **green** (53/53, my test passes, native operator handled it). Then proved
  **red** by reverting only the two production call sites (planner.rs + parquet_support cached branch)
  and rebuilding: test **still passed 53/53** → the fix is a **no-op on current main**. object_store
  0.13.2's `from_url_path` already handles `%`/spaces; the original branch base had the same 0.13.2.
- **Decision (user): DEFER** the `%`-path change to the phase that ships the tests needing it = **A.6a**
  (`CometDeltaPercentFileNameReproSuite` / `CometDeltaSpecialCharFilenameSuite`). Did NOT open a PR.
  Restored the branch to committed state. Updated §8 + §12 accordingly.
- Gotchas hit this session: **disk filled to 0** mid-build (release build of datafusion OOM'd the disk);
  user freed space by `rm -rf native/target/release`. JVM test needs `-Djava.version=17
  -Dmaven.compiler.source/target=17` (else `java.lang.Record not found` on the spark-4.x shim). Debug
  lib is fine for JVM correctness tests; copy it to `spark/target/classes/org/apache/comet/darwin/aarch64/libcomet.dylib`.
- Repo state left: on `feat/delta-kernel-read`; staged classpath lib is currently the throwaway RED
  (pre-fix) debug build — harmless (gitignored), rebuild before any real test run.

### 2026-06-10 (session 1 — Opus 4.8)
- Phase/unit in progress: **Phase 0 complete** (preserve work product + establish #4366 as the tracking PR). Phase 1 carving NOT started.
- Last fully verified boundary: n/a (no split units cut yet). Monolith = `feat/delta-kernel-read` @ `fae6869ba` = fork PR #2 (CI-validated end-to-end).
- Actions this session:
  - Confirmed monolith identity: "PR2" = fork PR schenksj#2, head `feat/delta-kernel-read`; it is a clean superset of the old `contrib-delta-direct` head (95 ahead, 0 behind → fast-forward). The `contrib-delta-pr2` branch is unrelated May exploration (ContribOperatorPlanner/ArcSwap lineage), NOT the work product.
  - Froze reference: branch `ref/delta-complete` + annotated tag `delta-complete-2026-06-10` @ `fae6869ba`, pushed to origin (fork). Never rebase.
  - Backed up "everything just in case": snapshot of pre-monolith #4366 head `8f8b7d445` as branch `backup/contrib-delta-direct-4366-pre-monolith` + tag `contrib-delta-direct-pre-monolith-2026-06-10`; pushed ALL local-only branches to `origin backup/*` (contrib-delta-{clean,onmain,port,direct-local,pre-rebase}, delta-integration-base, fix-{cast-binary-string-utf8-parity,local-path-special-chars}, worktree-agent-*). Verified 0 local branches remain unsaved.
  - Force-pushed (clean FF) `feat/delta-kernel-read` → `origin:contrib-delta-direct`; #4366 now shows the complete monolith @ `fae6869ba`.
  - Reframed #4366 as the umbrella/tracking PR (kept draft): new title `[Tracking] …`, body now leads with the split sequence table + dependency graph + extraction-PR gate list, with the full technical writeup preserved in a collapsed `<details>` reference block.
- Open carve questions: none yet (carving not begun).
- Pending decisions / upstream events: extraction PRs #4524/#4525/#4532/#4533/#4535/#4536/#4588 all OPEN (ready). %-path fix (`fix/local-path-special-chars`) still UNOPENED — plan §8 says open it now. A.2 §10.1 (4.1.1 pin) decision still outstanding.
- Durability note: **this plan file (`DELTA_PR_SPLIT_PLAN.md`) is untracked — lives only in the working tree.** It was deliberately NOT committed to `feat`/`contrib-delta-direct` (would leak into #4366). If durability is wanted, push it to a dedicated process branch on the fork.
- Next action: begin Phase 1 / Unit A.1 carve (branch `feat/delta-split` from `main`), OR open the %-path extraction PR first.

*(execution started — Phase 0 done)*
