# Unit A.1 — Core SPI for contrib leaf scans — review log

Branch: `pr/delta-A1-spi` (cumulative tip == `feat/delta-split` while A.1 is the front unit)
Base: `main` (upstream/main `0031c60d6`, mirrored to `schenksj:main`)

## Files (carved from `feat/delta-kernel-read` @ f442361c3 against current main)
- `operators.scala` [carve]: reflective `DeltaPlanDataInjector$` slot **against main's O(N) loop**
  (NOT #4535's `opStructCase`/`injectorsByKind`/O(1) rewrite); `foreachUntilCometInput` →
  `case _: CometLeafExec`; `findAllPlanData` → `case s: CometScanWithPlanData`; new
  `trait CometScanWithPlanData` **without** `perPartitionFilePaths` (A.8).
- `CometNativeScanExec.scala` [carve]: add `with CometScanWithPlanData` ONLY — A.1-necessary so the
  new generic `findAllPlanData` arm still catches native scans. Its `perPartitionFilePaths` /
  `serializedPartitionData`-triple hunks are A.8, excluded.
- `CometPlanAdaptiveDynamicPruningFilters.scala` [carve]: the `case p: CometScanWithPlanData if
  p.dynamicPruningFilters.exists(hasCometSAB)` in-place DPP arm.
- `CometScanWithPlanDataSuite.scala` [new test]: trait-contract defaults + reflective-slot graceful
  absence. Deliberately disjoint from #4535's `PlanDataInjectorSuite`.

## §5 verification (2026-06-13)
- Default native build: A.1 has zero native changes → unchanged. (Built a clean default lib to test
  against; the stale June-10 contrib lib caused spurious `NoClassDefFound CometBatchIterator` join
  failures — stale-dylib gotcha, not A.1.)
- New suite `CometScanWithPlanDataSuite`: 2/2 pass.
- Smoke `CometJoinSuite` (native scan fusion / DPP path): 28/28 pass.
- spotless:check + scalastyle:check: BUILD SUCCESS (after one spotless:apply on the new test).

## Carve attribution notes (for the PR description / future units)
- `CometNativeScanExec.scala` is listed under #4536 in plan §12, but the trait-extension hunk is
  A.1's; only the `perPartitionFilePaths` threading there is #4536/A.8. Split accordingly.
- Whichever of A.1 / #4535 lands second takes a small mechanical conflict in `PlanDataInjector`
  (O(N) reflective registry vs O(1) `injectorsByKind`). Accepted per plan §4.

## /review-comet-pr findings (round 1, 2026-06-13)
Fork draft PR: schenksj/datafusion-comet#3 (base `main`). Skill is expression-oriented; A.1 is a
structural SPI change (no expressions/benchmarks/SQL-tests apply). Reviewed for correctness,
behavior-preservation, tests, CI.

Correctness verified (not assumed):
- `foreachUntilCometInput` leaf match is a **strict superset**: the 3 scans removed from the
  enumeration (`CometNativeScanExec`, `CometIcebergNativeScanExec`, `CometCsvNativeScanExec`) all
  `extends CometLeafExec`; the 2 kept (`CometScanExec`, `CometBatchScanExec`) do NOT — correct.
- `findAllPlanData`: `CometIcebergNativeScanExec` case precedes the trait case (Iceberg keeps its
  dedicated path); trait case catches only `CometNativeScanExec` in A.1 and still drives
  `ensureSubqueriesResolved` via the `CometLeafExec` sub-match. Equivalent.
- DPP: `nativeScan`/`iceberg` arms precede the trait arm; trait `dynamicPruningFilters` defaults
  `Nil`, so the trait DPP arm never fires for core scans.
- Reflective injector slot: `ClassNotFoundException → None → unchanged registry` in default builds.

Fixes applied (folded into the unit commit, amend):
1. `operators.scala` used an inline `extends org.apache.spark.internal.Logging`. Project convention
   (sibling files) is `import ... Logging` + `extends Logging` — switched to that.
2. PR title needed a conventional-commit prefix (`check-pr-title` CI failed) → retitled `feat: ...`.

Documented trade-off (won't-fix):
- The behavioral generalization (trait/leaf matching) is covered by existing native-scan suites
  (verified `CometJoinSuite` 28/28), not by a new unit test that builds a full `CometNativeExec`
  tree (brittle, heavy). The new unit suite covers the trait contract + reflective-slot absence.

Re-verified after fixes: suite 2/2, spotless+scalastyle green. A.1 commit `e071ec845`.

## /code-review (second lens, high effort, 2026-06-13)
3 finder angles on the corrected 273-line diff (NOT `main...HEAD` — local `main` was stale at
4c1cf1bbf, inflating it to 24k lines; synced local `main` to `upstream/main`).
- Line-by-line correctness finder: **no bugs**. Confirmed the superset/ordering analysis above.
- Cross-file finder, 3 findings:
  1. `hasScanInput = sparkPlans.exists(_.isInstanceOf[CometNativeScanExec])` (operators.scala
     `buildNativeContext`) was NOT generalized to the trait. **Decision: leave as-is** — no-op in
     A.1 (only `CometNativeScanExec` impls the trait), matches the green monolith, and generalizing
     touches a metrics path whose Delta-side wiring isn't in A.1 (would diverge from `ref/delta-
     complete` on an untested path). Forward-note for A.4b/A.8. Metrics-only, not correctness.
  2. DPP rule case-4 fallback guard excludes `CometNativeScanExec`/`CometIcebergNativeScanExec` but
     not the generic trait. Forward-looking only (no contrib scan in A.1); matches monolith (which
     also only added case 3). Real contrib scans expose filters via the trait → caught by case 3.
     Won't-fix in A.1.
  3. Single-slot reflective `DeltaPlanDataInjector$` lookup doesn't scale past one contrib
     (altitude). Acknowledged trade-off; matches plan §4 (rewrite against main's registry); a
     list-driven loader is a possible future refactor. Won't-fix.
- All three are documented won't-fix / forward-notes, not A.1 defects.

## CI / preflight fixes (2026-06-13)
- `check-pr-title` failed → retitled PR to `feat: ...` (conventional commit).
- `Preflight` (`dev/ci/check-suites.py`) failed: new `CometScanWithPlanDataSuite` not registered →
  added it to `pr_build_linux.yml` + `pr_build_macos.yml`. Ran `check-suites.py` locally (py3.13):
  exit 0. Folded into A.1.
- A.1 commit now `aa0fd12c5` (6 files).

---

## 2026-06-20 — Refresh onto current main (#4535 landed) + review round 2

**Context:** dependent PRs #4524/#4533/#4535/#4536 merged to `apache/main` (now `9c69f30b0`).
A.1 was cut against the pre-#4535 O(N) loop; #4535 landed first, so A.1 needed re-basing onto
the O(1) `injectorsByKind` registry.

**Refresh:** clean `git rebase upstream/main` of `aa0fd12c5`. The single operators.scala hunk
(swap the `injectors` Seq for the reflective `builtin ++ deltaOpt` block) replayed cleanly on
top of #4535's untouched `injectorsByKind` — NO duplication (the trap the monolith merge hit).
Result == the monolith's reconciled state. Diff unchanged in shape: 6 files. Fork `main`
fast-forwarded to `9c69f30b0`; PR #3 base re-pointed.

**§5 re-verified (all green):** default `test-compile` BUILD SUCCESS; `CometScanWithPlanDataSuite`
2/2; `CometJoinSuite` 28/28 (native-scan fusion, fresh default debug libcomet copied into
`spark/target/classes/.../darwin/aarch64` to dodge the stale-dylib gotcha); spotless + scalastyle
0 errors; `check-suites.py` (py3.13) exit 0.

**Review round 2 = `/review-comet-pr` (manual) + `/code-review high` (31-agent workflow, 8 refuted,
6 survived).** Verified `CometLeafExec` has EXACTLY 3 subclasses (the 3 removed from the
enumeration) ⇒ `case _: CometLeafExec` is a provable strict superset; `CometScanExec`/
`CometBatchScanExec` are not leaf execs and stay in the 2nd case.

Two fixes folded into the A.1 commit (`fc48c0cfe`):
- **[fix] catch ladder (finding 6):** `NoSuchFieldException|IllegalAccessException` no longer
  silently → `None`; they now hit the `logWarning` arm. Only `ClassNotFoundException` (the
  expected default-build miss) stays quiet, so a *misbuilt* contrib jar (class present, MODULE$
  drift) is diagnosable instead of silently disabling Delta injection.
- **[fix] test dedup (finding 5, CONFIRMED):** dropped the `injectPlanData(root,…)==root`
  passthrough from `CometScanWithPlanDataSuite` — it duplicated `PlanDataInjectorSuite` (which
  covers the post-A.1 registry). Suite now asserts only A.1's unique contract (slot absence);
  removed the now-unused `Operator` import.

**Forward-notes carried to A.4b (PLAUSIBLE but provably inert on default builds AND on the real
Delta scan in the green monolith — NOT A.1 bugs):**
1. `injectorsByKind` is last-write-wins on duplicate `opStructCase`. Inert: real
   `DeltaPlanDataInjector.opStructCase = DELTA_SCAN`, distinct from ICEBERG/NATIVE_SCAN. (This is
   #4535's map, not A.1 code.)
2. DPP catch-all (`CometPlanAdaptiveDynamicPruningFilters`) excludes `CometNativeScanExec`/
   `CometIcebergNativeScanExec` by name, not the trait. Inert: monolith's Delta DPP surfaces the
   SAB through arm 3, so arm 4 never mishandles it. A.4b should swap the name-list for a
   `!_.isInstanceOf[CometScanWithPlanData]` guard.
3. Generic `findAllPlanData` arm has no `nonEmpty` guard (the Iceberg arm does). Inert: matches
   the prior unconditional `case nativeScan: CometNativeScanExec`; real Delta data is populated
   at collection.
4. `ensureSubqueriesResolved` only fires for `CometScanWithPlanData` that are also `CometLeafExec`.
   Inert: real `CometDeltaNativeScanExec extends CometLeafExec` (line 73). The trait's scaladoc
   already states implementers override it from `CometLeafExec`.

**State:** PR #3 force-updated to `fc48c0cfe`; MERGEABLE. Review loop clean — ready to open upstream
on the user's go. Next carve target: A.2 (build gate) onto `feat/delta-split`.
