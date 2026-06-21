# Unit A.4b — Scala serde + native exec — review log

Branch: `pr/delta-A4b-scala-exec` (based on `pr/delta-A4a-scala-claim`). Commit `8d14cdb5e`.

## Carve (from `feat/delta-kernel-read`)
11 files, +3879/-59. The RED-TO-GREEN unit: a -Pcontrib-delta build now does end-to-end native
Delta reads (serde converts the part-4a marker -> CometDeltaNativeScanExec reading via kernel-rs).
Survey agent mapped it.
- Verbatim main: Native.scala, DeltaPlanDataInjector.scala.
- Carved: CometDeltaNativeScan (serde) — dropped `val ScanImpl` (lives in DeltaScanMetadata from
  4a) + the whole `convertCdf` method + its CDF imports (CometDeltaCdfScanExec/RowDataSourceScanExec
  -> A.5). CometDeltaNativeScanExec — stripped the perPartitionFilePaths/FAILED_READ_FILE plumbing
  (A.8 interim error semantics: read failures -> generic CometNativeException; CometExecRDD param
  defaults to empty), preserving the taskGroups empty-scan fix + CometScanWithPlanData mixin.
- Tests: 5 whole-copy native-read suites; CometDeltaTestBase re-gained the native helpers
  (assertDeltaNativeMatches/KernelReadEngaged/Fallback/NativePlanContains — the last with AQE-aware
  `collect`), kept 4a's assertNoMarker/markersIn. CometDeltaMarkerSuite REWRITTEN: with the serde
  present, a claimed scan engages CometDeltaNativeScanExec (not a leftover marker) — assertions moved
  from marker-presence to native engagement. (A latent cross-unit issue: 4a's marker suite asserted
  the inert marker-fallback, which 4b's serde makes non-inert; updated here, as the unit that owns
  the behaviour change.)

## Carve invariants (verified)
- A.4b touches ONLY contrib/delta/src — no core/A.2/check-suites. The reflective wiring (CometExecRule
  scanHandler -> CometDeltaNativeScan$, A.1 registry -> DeltaPlanDataInjector$, CometScanRule
  delegation) reaches the new classes the moment they land.
- No dangling refs to convertCdf/CometDeltaCdfScanExec/perPartitionFilePaths in code.

## §5 verification (all green)
gated JVM test-compile; **60 contrib tests** (CometDeltaNativeSuite 19, ColumnMapping 5, Features 8,
Coverage 24, ColumnMappingPhysicalNameRepro 1, Marker 3) — end-to-end native reads; spotless +
scalastyle; check-suites; gate-verify (default still 0 Delta symbols). Ran spark-4.0/delta-4.0.0,
contrib libcomet copied into the test classpath.

## Review round (manual + /code-review high, 15-agent workflow: 3 refuted, 2 reported)
**Fixed (folded into the commit):**
- **orphaned imports** (CONFIRMED): removing `convertCdf` left `UTF8String` + `DateTimeUtils` unused
  (their only consumer) -> breaks the `-Pstrict-warnings` profile. Removed both.
- **dead `planData*` aliases** (PLAUSIBLE): planDataSourceKey/CommonBytes/PerPartitionBytes on the exec
  had ZERO callers (leftover from a rejected PR1 SPI; core reads the CometScanWithPlanData trait
  accessors directly). Removed (pre-existing monolith dead code, cleaned since the exec was edited).
- stale trait doc naming the removed test helpers -> updated.

**Refuted (3):** the DeltaPlanDataInjector CDF splice branch "unreachable in 4b" (it's proto-only,
harmless, and A.5 activates it), the decline test re-planning 3x (test style), and a dup of the
planData* finding.

**State:** A.4b review clean. The Rust + JVM read path is complete (a gated build reads Delta natively).
Next: A.5 (CDF — re-add convertCdf, CometDeltaCdfScanExec [already has my Nx-dup fix], the CometExecRule
CDF hook, DeltaIntegration CDF methods).
