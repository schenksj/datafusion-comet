#!/usr/bin/env bash
# Verify the `contrib-delta` build gate keeps Delta surface out of default builds.
#
# Three independent layers are checked:
#   1. Cargo: default `cargo build` doesn't compile `comet-contrib-delta` and
#      doesn't pull `delta_kernel` into the dependency tree.
#   2. Maven: default `mvn ... package` doesn't compile any
#      `org/apache/comet/contrib/` classes and doesn't pull `io.delta:*` deps.
#   3. Symbol/size: the resulting `libcomet.dylib` from the default build is
#      meaningfully smaller than the contrib-enabled build, and carries no
#      `comet_contrib_delta`/`delta_kernel`/etc. external symbols.
#
# Exit non-zero on the first failure. Designed to be wired into CI so a future
# change that leaks Delta into core gets caught immediately.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NATIVE_DIR="$ROOT/native"
SPARK_DIR="$ROOT/spark"

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
hdr()   { printf '\n\033[36m==> %s\033[0m\n' "$*"; }

# ---- Cargo gate -----------------------------------------------------------

hdr "Cargo: default build does not depend on comet-contrib-delta / delta_kernel"
cd "$NATIVE_DIR"
TREE_DEFAULT="$(cargo tree -p datafusion-comet --no-default-features 2>/dev/null)"
if echo "$TREE_DEFAULT" | grep -qE 'comet-contrib-delta|delta_kernel|delta-kernel'; then
  red "FAIL: default cargo tree contains Delta-related deps:"
  echo "$TREE_DEFAULT" | grep -E 'comet-contrib-delta|delta_kernel|delta-kernel'
  exit 1
fi
green "OK: cargo tree default is clean of contrib + kernel"

TREE_CONTRIB="$(cargo tree -p datafusion-comet --features contrib-delta 2>/dev/null)"
CONTRIB_HITS="$(printf '%s\n' "$TREE_CONTRIB" | grep -cE 'comet-contrib-delta|delta_kernel|delta-kernel' || true)"
if [[ "$CONTRIB_HITS" -lt 2 ]]; then
  red "FAIL: --features contrib-delta tree missing expected Delta-related entries (hits=$CONTRIB_HITS)"
  exit 1
fi
green "OK: cargo tree with contrib-delta correctly pulls comet-contrib-delta + delta_kernel"

# ---- Maven gate -----------------------------------------------------------

hdr "Maven: default profile excludes io.delta:* dependencies"
cd "$ROOT"
DEPS_DEFAULT="$(mvn -Pspark-4.1 -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -pl spark dependency:list 2>/dev/null || true)"
if echo "$DEPS_DEFAULT" | grep -qE 'io\.delta:'; then
  red "FAIL: default Maven build pulls io.delta dependencies:"
  echo "$DEPS_DEFAULT" | grep -E 'io\.delta:'
  exit 1
fi
green "OK: default Maven build has zero io.delta dependencies"

DEPS_CONTRIB="$(mvn -Pspark-4.1,contrib-delta -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -pl spark dependency:list 2>/dev/null || true)"
DELTA_DEP_HITS="$(printf '%s\n' "$DEPS_CONTRIB" | grep -cE 'io\.delta:delta-spark' || true)"
if [[ "$DELTA_DEP_HITS" -lt 1 ]]; then
  red "FAIL: -Pcontrib-delta profile missing io.delta:delta-spark"
  exit 1
fi
green "OK: -Pcontrib-delta correctly pulls io.delta:delta-spark"

# ---- Compiled-class gate --------------------------------------------------

hdr "Compiled classes: no contrib/delta classes in default build"
cd "$ROOT"
mvn -Pspark-4.1 -Djava.version=17 -Dmaven.compiler.source=17 -Dmaven.compiler.target=17 -pl spark -am test-compile -q -DskipTests=true >/dev/null 2>&1
LEAK_CLASSES="$(find spark/target/classes -path '*comet/contrib*' -name '*.class' 2>/dev/null)"
if [[ -n "$LEAK_CLASSES" ]]; then
  red "FAIL: default Maven build compiled contrib classes:"
  echo "$LEAK_CLASSES"
  exit 1
fi
DELTA_IMPL_LEAKS="$(find spark/target/classes \
  \( -name 'CometDeltaNativeScan*' \
     -o -name 'CometDeltaNativeScanExec*' \
     -o -name 'DeltaScanRule*' \
     -o -name 'DeltaReflection*' \) \
  2>/dev/null)"
if [[ -n "$DELTA_IMPL_LEAKS" ]]; then
  red "FAIL: default Maven build compiled Delta-implementation classes:"
  echo "$DELTA_IMPL_LEAKS"
  exit 1
fi
green "OK: only the always-present DeltaIntegration reflection bridge in default classes"

# ---- libcomet symbol/size gate -------------------------------------------

hdr "libcomet.dylib: default build is smaller and has no Delta external symbols"
cd "$NATIVE_DIR"
cargo clean -p comet-contrib-delta -p datafusion-comet >/dev/null 2>&1 || true
cargo build -j 4 -p datafusion-comet >/dev/null 2>&1
SIZE_DEFAULT="$(stat -f%z target/debug/libcomet.dylib 2>/dev/null \
  || stat -c%s target/debug/libcomet.dylib)"
if command -v nm >/dev/null 2>&1; then
  EXT_SYMS="$(nm -gU target/debug/libcomet.dylib 2>/dev/null \
    | grep -ciE 'comet_contrib_delta|delta_kernel|deltadvfilter|deltasynthetic' || true)"
else
  EXT_SYMS=0
fi
if [[ "$EXT_SYMS" -ne 0 ]]; then
  red "FAIL: default libcomet.dylib contains $EXT_SYMS Delta-related external symbols"
  exit 1
fi
green "OK: default libcomet.dylib has 0 Delta external symbols (size=$SIZE_DEFAULT bytes)"

cargo build -j 4 -p datafusion-comet --features contrib-delta >/dev/null 2>&1
SIZE_CONTRIB="$(stat -f%z target/debug/libcomet.dylib 2>/dev/null \
  || stat -c%s target/debug/libcomet.dylib)"
if [[ "$SIZE_CONTRIB" -le "$SIZE_DEFAULT" ]]; then
  red "FAIL: contrib-enabled libcomet (size=$SIZE_CONTRIB) is not larger than default (size=$SIZE_DEFAULT)"
  red "       (would indicate contrib was being linked into default build too)"
  exit 1
fi
DIFF_MB=$(( (SIZE_CONTRIB - SIZE_DEFAULT) / 1024 / 1024 ))
green "OK: contrib-enabled libcomet is ${DIFF_MB} MB larger than default (size=$SIZE_CONTRIB bytes)"

# ---- Summary --------------------------------------------------------------

hdr "All gate checks passed"
echo "  default cargo:  no comet-contrib-delta, no delta_kernel"
echo "  default mvn:    no io.delta:*, no contrib/delta classes"
echo "  default dylib:  ${DIFF_MB} MB smaller than contrib build, 0 Delta symbols"
echo
echo "Run with: dev/verify-contrib-delta-gate.sh"
