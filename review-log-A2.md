# Unit A.2 — Build gate + inert wiring — review log

Branch: `pr/delta-A2-buildgate` (based on `pr/delta-A1-spi`).
Commit: `f2ad00c29` (single commit; review fixes amended in).

## Carve (from `feat/delta-kernel-read` @ 9f29732a8 against A.1 base)
20 files, +4020/-14. Wholesale-A.2 checkouts (spark/pom.xml, native Cargo.toml ×2,
operator.proto [delta_scan=118], planner.rs, operator_registry.rs, jni_api.rs, CometScanRule.scala);
whole-file copies (delta_scan.rs shim, DeltaIntegration.scala, DeltaConf.scala, gate script);
authored STUB contrib crate (contrib/delta/native/{Cargo.toml,lib.rs,planner.rs} -> plan_delta_scan
returns DataFusionError::NotImplemented); partial carves (pom.xml = delta.version only, NOT the
4.1.1 pin; CometExecRule = marker hook only, CDF hook -> A.5); authored minimal delta_build_gate.yml.

## §5 verification (all green)
default + gated native build; clippy both feature states; gate-verify script (all layers);
gated + default JVM test-compile; spotless + scalastyle (both profiles); cargo fmt (native + contrib).

## Review round (manual /review-comet-pr + /code-review high, 28-agent workflow: 9 refuted, 6 survived)

**Found+fixed (folded into the commit):**
- **[gate script] cargo-tree check was vacuous** (CONFIRMED): a failing `cargo tree` -> empty output
  -> leak grep finds nothing -> false PASS (the Maven gate had an anti-vacuous guard, the cargo one
  didn't). Added a `datafusion-comet ` anchor assertion. Unit-tested: empty input now fails, real
  tree passes.
- **[gate script] pipefail + `grep -q` SIGPIPE** (found during verification): `echo "$BIG" | grep -q`
  under `set -o pipefail` returns 141 (grep exits early -> echo SIGPIPE), misfiring the `if !` guard
  on the 1842-line effective-pom. Converted the three guards to here-strings and replaced
  `head -1` (same early-close hazard) with `sed -n '1s//p'`.
- **[DeltaIntegration] dead CDF block** (CONFIRMED): `isCdfRelation`/`transformCdf`/`convertCdfBinding`
  (~65 lines) had no caller in A.2 (the CDF hook is A.5). Stripped to A.5 (plan §4 A.5 updated).
- **[DeltaIntegration] scanHandler re-resolved MODULE$ per call** (CONFIRMED): violated the repo's
  cache-reflection convention. Added a @volatile `scanHandlerCache` (memoised once per JVM).
- **[DeltaIntegration] raw Class.forName** (PLAUSIBLE): swapped to `ClassLoaders.loadClass`
  (thread-context-classloader-first), matching IcebergReflection's convention.

**Accepted / documented (not fixed — match the green monolith, default behavior unchanged):**
- **metadata-col fallback REASON string on Spark<3.5** (CONFIRMED, diagnostic-only): a V1 scan with
  BOTH a metadata column AND an AQE DPP filter now reports "AQE Dynamic Partition Pruning requires
  Spark 3.5+" instead of "Metadata column is not supported". Both still fall back to Spark (no
  behavior change); the string differs only in this rare Spark-3.4 overlap. Inherent to letting Delta
  claim metadata-col scans (the reorder is required); the metadata-col guard is re-applied inside
  transformV1Scan so non-overlap cases are identical. Matches the monolith.
- **DeltaConf's 4 entries are unread in A.2** (PLAUSIBLE): they're the contrib's config surface,
  consumed by later units. DeltaConf is the leaf the plan deliberately ships so the contrib
  add-source dir compiles; trimming would fragment the config object or need an artificial placeholder.

**Refuted (9):** style dups (parameterize the two near-identical reflection caches), `isAvailable`
no-caller (public util), `isDeltaScanMarker` getClass.getName per node (inherent to marker matching),
compiled-class gate "vacuous on compile failure" (the find-based check is fine), et al.

**State:** A.2 review clean. Ready to open upstream once A.1 (#4700) is approved/merged (per §8).
