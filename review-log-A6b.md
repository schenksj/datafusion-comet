# Unit A.6b — Delta own-suite regression harness — review log

Branch: `pr/delta-A6b-regression` (based on `pr/delta-A6a-test-battery`). Commit `f0dcb242a` (amended). Fork PR #11.

## Carve (test/CI tooling only — no production, native, or main code)
6 files, +2021. All carved from `feat/delta-kernel-read`:
- `contrib/delta/dev/diffs/{3.3.2,4.0.0,4.1.0}.diff` (byte-identical to monolith)
- `contrib/delta/dev/run-regression.sh`, `run-test.sh` (carved verbatim, then review-hardened — see below)
- `.github/workflows/delta_regression_test.yml` (carved, then review-hardened)

## Carve invariants (verified)
- All 3 diffs byte-identical to monolith AND **`git apply --check` clean against the real upstream
  Delta tags** v3.3.2 / v4.0.0 / v4.1.0 (shallow-cloned, checked, cleaned up).
- Scripts carry the pipefail-safe cometVersion auto-derive (`grep -oE | sed -n '1p'`); correct `100755`
  exec bits. bash `-n` + workflow YAML valid.
- run-test.sh provides the reviewer-anticipated single-suite local rerun path.

## §10.1 pin question — RESOLVED: no pin needed in A.6b
The regression runs Delta's OWN suite; Delta's build selects its own Spark via `CrossSparkVersions`
(Delta 4.1.0 → Spark 4.1.0). delta-spark 4.1.0 never runs on the pom's 4.1.2, so the
removed-`IgnoreCachedData` skew (which forced A.6a's `-Dspark.version=4.1.1`) cannot occur here. The diff
inherits `sparkVersion.value` (doesn't pin); grep confirms no spark.version anywhere in the harness. The
proven monolith harness has no pin — correct.

## §5 verification (all green)
3 diffs apply --check clean to real Delta tags; bash -n both scripts; workflow YAML parses; version-skew
auto-derive present; gate-verify/check-suites unaffected (no native/main/suite changes). E2E full/smoke
regression run DEFERRED — disk at 94% (release native build duplicates the ~5-11G delta_kernel tree per
the documented blowup constraint) + runtime (full 3.3.2 ~29h); it is the plan's gated heavyweight (§5),
not a per-carve gate, and the harness is byte-identical to the monolith where the sweeps were executed.

## Review round (manual + /code-review high, 44 agents, 10 findings — ALL on the scripts/workflow, none on the diffs)
**Fixed 7:**
1. **cometVersion pipefail-abort (CONFIRMED):** `grep -oE` no-match exits 1 → aborts under `set -e` BEFORE
   the documented fallback (my prior fix handled the `head -1` SIGPIPE but not the no-match case). Added
   `|| true` so the substitution yields empty and the WARNING+hardcoded-fallback runs.
2. **reused-workdir checkout hard-abort (CONFIRMED):** `git fetch ... || true` then unconditional
   `git checkout -f vX` aborts if the tag isn't in a reused DELTA_WORKDIR (switching versions / offline).
   Now re-clones gracefully on checkout failure.
3. **dead 2.4.0 case (CONFIRMED):** no 2.4.0.diff exists and SBT_MODULE=core was wrong — removed the case
   + the usage line.
4. **COMET_PUBLISH_DIR coupling (CONFIRMED):** the override can't work (diff hardcodes the repo path) —
   documented the coupling loudly (a guard comment; the default is unaffected).
5. **run-test.sh hardcoded personal JAVA_HOME (CONFIRMED):** `$HOME/jdks/jdk-17.0.18+8/...` replaced with
   the macOS `java_home -v 17` helper, falling back to PATH `java`.
6. **PR trigger over-broad (CONFIRMED):** the heavy workflow ran on every code PR — scoped the
   `pull_request` trigger to Delta-affecting paths (contrib, native, SPI/rule/serde, poms); push-to-main
   keeps the broad paths-ignore so the post-merge full sweep isn't under-triggered.
7. **misleading "matching smoke cell" comment (CONFIRMED):** GitHub `needs` is job-level — corrected the
   comment and made the smoke-on-PR / full-on-merge cost tradeoff explicit.
**Not fixed (documented):** wrong-token cometVersion (#3 PLAUSIBLE — the comment already states the
project version is the only `-SNAPSHOT` in the pom; #1's `|| true` makes even a future miss graceful);
git-apply integrity (#5 PLAUSIBLE — `git apply` is atomic and the #2 fix ensures the right tag);
delta-full PR-skip (#6 CONFIRMED but a deliberate cost tradeoff — a 29h full suite can't run per-PR;
clarified in the comment, flagged as a §10.4 maintainer decision).

**Divergence note:** the 3 edited files (both scripts + the regression workflow) now INTENTIONALLY diverge
from the monolith (the review found real bugs in the monolith's harness). The diffs stay byte-identical.
These 7 fixes are real monolith bugs → BACKPORT to #4366.

## Post-review fix (found running the 4.1 full regression locally — session 5n)
Running `run-regression.sh 4.1.0 full` on macOS surfaced a real harness bug CI (Linux bash 5) never
hit: `SBT_REPO_OVERRIDE=()` is empty on the 4.x cells, and **macOS bash 3.2 treats `"${empty[@]}"` as
an unbound variable under `set -u`** → the script aborted at `[4/4]` before running any tests
(`SBT_REPO_OVERRIDE[@]: unbound variable`). Fixed the 4 call sites to the portable
`${SBT_REPO_OVERRIDE[@]+"${SBT_REPO_OVERRIDE[@]}"}` alternation (verified under bash 3.2 + set -u).
Amended into A.6b (`c65b636e2`); A.7 (`59ff67caa`) + A.8 (`8e31afad6`) cascade-rebased; all force-pushed
(#11/#12/#13). Re-launched the regression — now past `[4/4]` into Delta's sbt test build. NOTE: the same
bug is in the fork-monolith backport (`run-regression.sh` @ 503c4194c) — fix there too.

**State:** A.6b review clean + harness bash-3.2 fix, pushed (#11). Remaining: A.7 (docs),
A.8 (perPartitionFilePaths / FAILED_READ_FILE follow-up).
