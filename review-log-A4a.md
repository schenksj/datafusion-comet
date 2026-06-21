# Unit A.4a — Scala claim/decline + reflection — review log

Branch: `pr/delta-A4a-scala-claim` (based on `pr/delta-A3b-rust-executor`).
Commit: `ae3ec8d24` (single commit; review fixes amended in).

## Carve (from `feat/delta-kernel-read`)
8 files, +2514. Survey agent mapped it.
- Verbatim main files: DeltaReflection (1252), DeltaScanRule (729), RowTrackingAugmentedFileIndex
  (113), CometDeltaScanMarker (81), DeltaScanMetadata (38).
- The ONE required edit: `CometDeltaNativeScan.ScanImpl` (a 4b class) → new
  `object DeltaScanMetadata { val ScanImpl = "native_delta_compat" }`; DeltaScanRule:306 re-pointed.
  (Only def + that one site existed in the monolith; 4b re-points its serde at it.)
- Tests: CometDeltaTestBase (trimmed of the 3 serde/exec-dependent native-read helpers →
  deferred to 4b; added marker-claim helpers) + NEW CometDeltaMarkerSuite (3 tests).
- check-suites.py: exempt contrib test suites (run under the dedicated Delta workflow, not pr_build).

## Carve invariants (verified)
- No A.4a file references a deferred class (CometDeltaNativeScan/Native/CometDeltaNativeScanExec/
  DeltaPlanDataInjector/CometDeltaCdfScanExec) at compile time. ScanImpl is the only ex-dependency.
- A.4a touches ONLY contrib/delta/src + dev/ci/check-suites.py. No core, no A.2 edits (the reflective
  wiring A.2 shipped already reaches DeltaScanRule$ + the marker).
- Inert by design: DeltaScanRule plants CometDeltaScanMarker; with no serde, CometExecRule.scanHandler
  returns None, the marker stays and executes vanilla. Delta reads run on vanilla Spark.

## §5 verification (all green)
gated JVM test-compile (-Pcontrib-delta); **CometDeltaMarkerSuite 3/3** (marker planted on a plain
read — red on the A.2 build, green here; fallback result-correct; input_file_name decline plants no
marker); check-suites (py3.13); spotless + scalastyle; gate-verify (default build still 0 Delta
symbols, only the DeltaIntegration bridge class). Ran under spark-4.0/delta-4.0.0 (avoids the local
spark-4.1.1 pin quirk); contrib libcomet copied into the test classpath.

## Review round (manual + /code-review high, 25-agent workflow: 6 refuted, 3 reported — all in the
hand-authored TEST code; the verbatim monolith modules were clean)
**Fixed (folded into the commit):**
- **test 2 coverage gap** (CONFIRMED): "marker fallback returns rows identical to vanilla" asserted
  only row-equality, never that a marker was planted — a claim-path REGRESSION would ship green
  (both sides degrade to vanilla). Strengthened: test 2 now `assertMarkerPlanted` on the
  filter+select shape AND checks result-correctness.
- **marker matcher divergence** (PLAUSIBLE): the helpers matched `getClass.getSimpleName` while
  production `DeltaIntegration.isDeltaScanMarker` matches the FQN (`getClass.getName`). Switched the
  helpers to FQN so the test tracks the exact production predicate (shared `MarkerClass` constant).
- **dead+buggy `assertNativePlanContains`** (PLAUSIBLE): used non-AQE `TreeNode.collect` (would miss
  execs inside the AdaptiveSparkPlanExec wrapper) and was unused in A.4a. Removed; 4b adds the
  native-read helpers it needs with the AQE-aware `collect`.

**Refuted (6):** assertResultsMatchVanilla "can't detect native-vs-vanilla" (true but by design in the
inert unit), input_file_name decline "needs full plan" (the caller passes it), normalizeRow/withDeltaTable
"duplicate QueryTest/inherited helpers" (style), marker-rename fragility (variant of the FQN finding).

**State:** A.4a review clean. Next: A.4b (serde + exec — the red-to-green native-read unit) re-adds the
native-read test helpers and re-points its serde at DeltaScanMetadata.ScanImpl.
