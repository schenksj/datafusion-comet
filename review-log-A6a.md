# Unit A.6a — Test battery + CI workflow — review log

Branch: `pr/delta-A6a-test-battery` (based on `pr/delta-A5-cdf`). Commit `9a1ef06fc` (amended). Fork PR #10.

## Carve (test-only — no production or native code)
26 files, +3677/-98.
- **23 contrib-delta suites** copied **byte-identical** from `feat/delta-kernel-read` (verified via
  `git diff --no-index` per file): 22 under `org.apache.comet.contrib.delta` +
  `CometDeltaCheckpointFilterReproSuite` under `org.apache.spark.sql.delta`.
- **`.github/workflows/delta_contrib_test.yml`** added (the full contrib test workflow).
- **`dev/ci/check-suites.py`** refined to monolith final state (contrib exclusion hoisted ahead of
  class-name extraction). Byte-identical to monolith.
- **`.github/workflows/delta_build_gate.yml`** DELETED — the part-2 standalone gate is byte-identical
  to the `delta-build-gate` job inside the full workflow (its own docstring anticipated this). Converges
  to the monolith end-state (monolith has no standalone gate file).

## Carve invariants (verified)
- All 23 suites byte-identical to monolith. `check-suites.py` byte-identical to monolith.
- My superset `CometDeltaTestBase` (A.4b: monolith helpers + `assertNoMarker`/`markersIn` for the marker
  suite) already supports all 23 incoming suites — they only use the monolith helper subset. Confirmed
  the incoming suites reference no mine-only helper, and the marker suite's helpers stay.
- No A.8 dependency: the `FAILED_READ_FILE` references in the incoming suites are all COMMENTS;
  `CometDeltaEdgeCaseRegressionSuite` deliberately asserts version-stable wording, not the error-class.

## %-path red-green decision (§8) — DROPPED
Ran `CometDeltaPercentFileNameReproSuite` + `CometDeltaSpecialCharFilenameSuite` WITHOUT the
`object_store_path_from_url` production change (the change lives only in `fix/local-path-special-chars`,
NOT in the monolith): **7 succeeded, 0 failed, 1 version-gated cancel**. Green without the change →
confirmed no-op on current main (object_store round-trips percent-encoded local paths) → the change is
**dropped entirely**. A.6a carries no production change. Branch `fix/local-path-special-chars` retired.

## §5 verification (all green)
gated JVM test-compile (all 31 contrib suites compile under `-Pspark-4.1,contrib-delta`);
**full battery 157 succeeded / 0 failed / 1 cancel across 33 suites** (14m54s, Spark 4.1 + Delta 4.1.0,
`-Dspark.version=4.1.1`); spotless + scalastyle clean; `check-suites.py` exit 0 (contrib correctly
excluded); `dev/verify-contrib-delta-gate.sh` all checks pass (default libcomet 0 Delta symbols).
No Scala-2.12 core compile needed — A.6a has zero core/main Scala changes.

## Review round (manual + /code-review high, 35 agents: 20 findings → ~9 root causes, ALL on the new workflow)
No issue in any carved suite or in check-suites.py. All findings targeted `delta_contrib_test.yml`.
**Fixed (5):**
1. **Spark 4.1 cell CI-red (CONFIRMED):** `-Pspark-4.1` pulls Spark 4.1.2 (dropped `IgnoreCachedData`,
   breaks delta-spark 4.1.0). Wired `-Dspark.version=${{ matrix.spark-version.full }}` so each cell pins
   its exact patch (3.5.8 / 4.0.2 / 4.1.1). The 4.1.1 command is the one the local battery proved.
   Pom stays 4.1.2 for default users — CI-only pin, per the part-2 / §10.1 decision.
2. **Spark 3.5 mislabeled 2.13 (CONFIRMED):** the `-Pspark-3.5` profile fixes Scala 2.12.18, never 2.13.
   Corrected the label to `2.12` + comment. 3.5/2.12 is *valuable* — the only 2.12 cell, guarding the
   existential-type-inference class of bug that bit the core DeltaIntegration bridge. (Did NOT force
   `-Pscala-2.13`: it would wrongly downgrade 4.1's Scala patch .17→.16 and lose 2.12 coverage.)
3. **Cosmetic `full` version (CONFIRMED):** made `spark-version.full` load-bearing via the `-Dspark.version`
   wiring above; aligned 4.0 full 4.0.1→4.0.2 to the proven profile default. `delta-version` left as a
   display label — the actual delta-spark version is profile-pinned and gate-verified, so it's accurate.
4. **Uncached contrib target (CONFIRMED):** added `contrib/delta/native/target` to the cache restore+save
   paths so the standalone crate's `cargo test` is incremental (subsumes the double-compile / critical-path
   findings — the contrib crate is outside the `native/` workspace, so its target is a separate dir).
5. **Zero-match silent-green (CONFIRMED):** scalatest treats a zero-match `wildcardSuites` as success.
   Added a guard step asserting ≥25 per-suite surefire reports (real run produces 31; dry-run verified).
Plus stale "both cells / both version pairs" comments → "all three" (3 cells, not 2).
**Not fixed (by design):** asymmetric `wildcardSuites` selector (prefix on `org.apache.spark.sql.delta`
would sweep the real Delta library — already commented); `delta-version` display label (resolution is
correct + gate-verified).

**Divergence note:** A.6a's `delta_contrib_test.yml` intentionally diverges from the monolith (the review
found real bugs in the monolith's workflow). These five fixes should be **backported to #4366** like the
gate-script fix was.

**State:** A.6a review clean, pushed (#10), §5 green. Remaining: A.6b (regression harness), A.7 (docs),
A.8 (perPartitionFilePaths / FAILED_READ_FILE follow-up).
