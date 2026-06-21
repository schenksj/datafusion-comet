# Unit A.5 — CDF (Change Data Feed) — review log

Branch: `pr/delta-A5-cdf` (based on `pr/delta-A4b-scala-exec`). Commit `bf67e8814` (amended).

## Carve (survey-mapped re-add of CDF pieces deferred across earlier units)
6 files, +595/-5. Touches 2 CORE files (the CDF hook/members were deferred from A.2/A.4a).
- NEW `CometDeltaCdfScanExec.scala` (verbatim from monolith) — carries my Nx-dup fix
  (CometScanWithPlanData). Re-memoised commonData/perPartitionData as @transient lazy val per review.
- Serde `convertCdf` re-added (verbatim) + imports (CometDeltaCdfScanExec, RowDataSourceScanExec).
  Did NOT re-add ScanImpl (lives in DeltaScanMetadata) or the orphaned UTF8String/DateTimeUtils
  imports (convertCdf doesn't use them — confirmed; my A.4b removal was correct).
- DeltaIntegration CDF members (isCdfRelation/transformCdf/convertCdfBinding) re-added BEFORE the
  cached scanHandler — the A.2 cached-scanHandler + ClassLoaders.loadClass 2.12 fixes are UNTOUCHED.
- CometExecRule CDF arm re-added after the marker hook (RowDataSourceScanExec via existing wildcard).
- Tests: CometDeltaCdcSuite, CometDeltaCdfReflectionReproSuite (verbatim).

## Carve invariants (verified)
- DeltaIntegration: cached scanHandler (scanHandlerCache + `val handler: Option[CometOperatorSerde[_]]`)
  + ClassLoaders.loadClass PRESERVED. Serde: ScanImpl/UTF8String/DateTimeUtils ABSENT.
- Wiring: serde `convertCdf(RowDataSourceScanExec)` matches DeltaIntegration's
  `getMethod("convertCdf", classOf[RowDataSourceScanExec])`.
- CDF members inert on default builds (reflective serde lookup absent -> None -> vanilla CDF read).

## §5 verification (all green)
gated JVM test-compile; **Scala-2.12 core compile** (spark-3.4 — DeltaIntegration/CometExecRule CDF
hunks are core, must compile on 2.12); **6 CDF tests** (CometDeltaCdcSuite 3 incl. the orderBy +
unix_timestamp red-green for the Nx-dup fix; CometDeltaCdfReflectionReproSuite 3); spotless/scalastyle;
check-suites; gate-verify (default still 0 Delta symbols, only DeltaIntegration).

## Review round (manual + /code-review high, 15 agents: 4 refuted, 1 reported)
**Fixed:** CometDeltaCdfScanExec commonData/perPartitionData were `def`s (rebuild protobufs 2-3x per
execution) vs the sibling scans' memoised lazy val -> made `@transient override lazy val` (CDF
subRanges are fixed, no DPP, so memoising is safe). My Nx-dup fix had introduced the defs.
**Refuted (4):** the convertCdfBinding reflective-cache "duplication" (it's the established 3-method
pattern; a generic helper is a bigger refactor, not warranted) and the dedicated CDF match arm
(correct design, parallel to the marker arm).

**State:** A.5 review clean. The Delta READ path is now COMPLETE (V1 scans + CDF, end-to-end native).
Remaining: A.6a (test battery), A.6b (regression harness), A.7 (docs), A.8 (perPartitionFilePaths /
FAILED_READ_FILE follow-up).
