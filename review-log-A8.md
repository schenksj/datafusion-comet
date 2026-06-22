# Unit A.8 — FAILED_READ_FILE provenance (perPartitionFilePaths) — review log

Branch: `pr/delta-A8-failed-read-file` (based on `pr/delta-A7-docs`). Commit `e703c0f98` (amended). Fork PR #13. **FINAL unit of the split.**

## Scope (code unit: core + contrib + red-green test)
A.8 was gated on #4536, which has now MERGED to main (CannotReadFile typed error + CometExecRDD's
perPartitionFilePaths param are on HEAD). So A.8 is the REMAINING delta: the trait member + the
CometNativeExec threading + the contrib override + the red-green test. NOT a verbatim monolith copy —
applied only the perPartitionFilePaths hunks of operators.scala (the unrelated PlanDataInjector
reflection-refinement drift between split and monolith was left out of scope).

## Carve (4 files)
- **operators.scala [CORE]**: `perPartitionFilePaths` trait member (default Array.empty);
  NativeExecContext field; CometNativeExec collects per-partition paths from all CometScanWithPlanData
  leaves (early-empty when none) and threads into CometExecRDD.
- **CometNativeScanExec.scala [CORE]**: `override` keyword on its existing perPartitionFilePaths
  (REQUIRED coupling — the now-concrete trait member; matches monolith line 242). Found via compile
  error, fixed; checked all 3 implementers (CometNativeScanExec + CometDeltaNativeScanExec override;
  CometDeltaCdfScanExec correctly inherits the empty default — CDF has no per-file tasks).
- **CometDeltaNativeScanExec.scala**: override perPartitionFilePaths (parse DeltaScan task lists) +
  standalone doExecuteColumnar pass-through. Did NOT re-add the dead planData* aliases (removed in A.4b).
- **CometDeltaFailedReadFileSuite.scala (NEW)**: red-green guard.

## Key finding during carve: F6 already passes without A.8
The native side ALWAYS calls `map_file_read_error(e, &file.path)` (kernel_scan.rs ×5), so CannotReadFile
is path-bearing and the corrupted-file case (EdgeCase F6, on HEAD via A.6a) already surfaces
FAILED_READ_FILE WITH the path — it passed the A.6a battery (157/0). So an e2e "path-in-error" test
would be VACUOUS (passes without A.8). perPartitionFilePaths is the parity/fallback provenance
(SparkErrorConverter fills it only when the native path is empty). => the honest red-green is STRUCTURAL:
assert CometDeltaNativeScanExec.perPartitionFilePaths exposes the scan's files (empty trait default = RED;
override = GREEN).

## §5 verification (all green)
- **Red-green PROVEN:** RED (override removed → `Array() was empty — provenance not wired`), GREEN (1/0).
- Targeted regression: CometDeltaNativeSuite + CometDeltaCdcSuite + new suite = **23/0** (no regression
  from the shared CometNativeExec change).
- **Scala 2.12 compile of CORE (spark-3.4) AND contrib (spark-3.5/scala-2.12)** — the operators.scala
  trait/threading + CometNativeScanExec override + the contrib exec all compile on 2.12.
- spotless/scalastyle clean; gate-verify all pass (default libcomet 0 Delta symbols — trait member inert).

## Review round (/code-review high, 35 agents, 5 findings)
**Fixed 2 (genuinely A.8-introduced):**
- **#3 redundant proto parse (CONFIRMED):** standalone doExecuteColumnar parsed execPerPartitionBytes
  TWICE (once for encryptedFilePaths, once for perPartitionFilePaths). Reordered: compute
  perPartitionFilePaths once, derive `encryptedFilePaths = perPartitionFilePaths.flatten`.
- **#5 vacuous subsetOf (PLAUSIBLE):** the test's `onDiskDataFiles.subsetOf` would pass trivially if the
  listing were empty (e.g. a future partitioned fixture). Added an `onDiskDataFiles.nonEmpty` assert.
**Documented, not fixed (inherent to the mechanism / monolith design):**
- **#1 join misattribution (CONFIRMED):** a fused multi-scan partition's hint unions all scans' files.
  Inherent to per-partition (not per-file) attribution — SAME as CometNativeScanExec; the NO_HINT variant
  signals imprecision. Acceptable; noted in the PR body.
- **#2 DPP rebuild to extract paths (CONFIRMED):** the override calls perPartitionData (recomputed, not
  memoised). The recompute is REQUIRED for DPP correctness (pruning changes which files are read), so
  memoising would be wrong. Inherent cost; matches monolith.
- **#4 collection runs per native query (PLAUSIBLE):** bounded by the early-empty return; modest
  per-partition allocation. Matches monolith; the feature's cost.

**State:** A.8 review clean, pushed (#13). **The Delta contrib split is now COMPLETE end-to-end (A.1–A.8).**
Remaining (not split units): docs 10/11/12 archive decision (user); #4366 reconcile/close at end-of-cycle
(§9.4); flaky CI re-runs on #4/#5; backport already landed on the fork monolith (503c4194c).
