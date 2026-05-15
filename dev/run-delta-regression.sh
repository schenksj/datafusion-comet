#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Run Delta Lake's own test suite with Comet enabled as a regression check.
# Mirrors what .github/workflows/delta_regression_test.yml does in CI.
#
# This is the PR2 (contrib) variant: the install step bundles the Delta
# contrib via `-Pcontrib-delta` so the comet-spark JAR being installed
# carries DeltaScanRuleExtension/DeltaOperatorSerdeExtension and the
# matching JNI symbols (built into libcomet via `--features contrib-delta`
# on the native crate). Without `-Pcontrib-delta` the installed comet-spark
# JAR has no Delta wiring and Delta tests would just exercise vanilla Spark.
#
# Usage:
#   dev/run-delta-regression.sh [DELTA_VERSION] [TEST_FILTER]
#
# Examples:
#   dev/run-delta-regression.sh                             # smoke on default (4.1.0)
#   dev/run-delta-regression.sh 4.1.0                       # smoke on Delta 4.1.0
#   dev/run-delta-regression.sh 4.1.0 full                  # full Delta test suite
#   dev/run-delta-regression.sh 4.1.0 DeltaTimeTravelSuite  # one specific test class
#   DELTA_WORKDIR=/tmp/my-delta dev/run-delta-regression.sh # reuse a checkout

set -euo pipefail

DELTA_VERSION="${1:-4.1.0}"
TEST_FILTER="${2:-smoke}"

# Map Delta version -> Spark short version -> SBT module
case "$DELTA_VERSION" in
  2.4.0) SPARK_SHORT="3.4"; SBT_MODULE="core" ;;
  3.3.2) SPARK_SHORT="3.5"; SBT_MODULE="spark" ;;
  4.0.0) SPARK_SHORT="4.0"; SBT_MODULE="spark" ;;
  4.1.0) SPARK_SHORT="4.1"; SBT_MODULE="spark" ;;
  *)
    echo "Error: unsupported Delta version '$DELTA_VERSION'"
    echo "Supported: 2.4.0 (Spark 3.4), 3.3.2 (Spark 3.5), 4.0.0 (Spark 4.0), 4.1.0 (Spark 4.1)"
    exit 1
    ;;
esac

COMET_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIFF_FILE="$COMET_ROOT/dev/diffs/delta/${DELTA_VERSION}.diff"
DELTA_WORKDIR="${DELTA_WORKDIR:-${TMPDIR:-/tmp}/delta-regression-${DELTA_VERSION}}"

if [[ ! -f "$DIFF_FILE" ]]; then
  echo "Error: diff file not found: $DIFF_FILE"
  exit 1
fi

echo "=========================================="
echo "Delta regression run (contrib variant)"
echo "  Delta version : $DELTA_VERSION"
echo "  Spark profile : spark-$SPARK_SHORT"
echo "  SBT module    : $SBT_MODULE"
echo "  Test filter   : $TEST_FILTER"
echo "  Work dir      : $DELTA_WORKDIR"
echo "  Comet root    : $COMET_ROOT"
echo "=========================================="

# Step 1: build + install Comet to local Maven repo for the target Spark profile,
# with the Delta contrib bundled into comet-spark.jar.
#
# `FAST=1` skips plugin checks that aren't relevant during iteration:
#   - drop `-Prelease` (no source/javadoc/scaladoc jars, no GPG prep)
#   - skip spotless check (run `mvn spotless:apply` manually before commit)
#   - skip Apache RAT license header check
#   - skip javadoc / scaladoc generation
#   - skip source jar packaging
# Together these save ~60-120s per iteration. The canonical (no-FAST) invocation
# still runs the full lifecycle so CI parity is preserved.
echo
echo "[1/4] Building and installing Comet (spark-$SPARK_SHORT, contrib-delta)..."
cd "$COMET_ROOT"
# Spark 4.1 requires Java 17 (java.lang.Record). Comet's parent pom defaults
# java.version=11 — overriding here so the install works regardless of which JDK
# is on JAVA_HOME, as long as that JDK is ≥17.
JAVA_OVERRIDE=(
  -Djava.version=17
  -Dmaven.compiler.source=17
  -Dmaven.compiler.target=17
)
if [[ -n "${FAST:-}" ]]; then
  echo "  FAST=1: skipping spotless/RAT/javadoc/source-jar plugins"
  ./mvnw install -DskipTests -Pspark-"$SPARK_SHORT" -Pcontrib-delta \
    "${JAVA_OVERRIDE[@]}" \
    -Dspotless.check.skip=true \
    -Drat.skip=true \
    -Dmaven.javadoc.skip=true \
    -Dmaven.source.skip=true
else
  ./mvnw install -Prelease -DskipTests -Pspark-"$SPARK_SHORT" -Pcontrib-delta \
    "${JAVA_OVERRIDE[@]}"
fi

# Step 2: clone Delta (or reuse existing checkout).
#
# `git clean -fd` here is intentional and cheap (sub-second): it removes
# untracked files left from the previous diff apply but respects gitignore,
# so Delta's `target/` (and SBT's zinc cache inside it) is preserved.
echo
echo "[2/4] Cloning Delta $DELTA_VERSION..."
if [[ -d "$DELTA_WORKDIR/.git" ]]; then
  echo "  Reusing existing checkout at $DELTA_WORKDIR"
  cd "$DELTA_WORKDIR"
  git fetch --depth 1 origin "refs/tags/v$DELTA_VERSION:refs/tags/v$DELTA_VERSION" 2>/dev/null || true
  git checkout -f "v$DELTA_VERSION"
  git clean -fd
  rm -rf spark/spark-warehouse
else
  rm -rf "$DELTA_WORKDIR"
  git clone --depth 1 --branch "v$DELTA_VERSION" https://github.com/delta-io/delta.git "$DELTA_WORKDIR"
  cd "$DELTA_WORKDIR"
fi

# Step 3: apply the Comet diff.
echo
echo "[3/4] Applying diff $DIFF_FILE..."
git apply "$DIFF_FILE"

# Step 4: run tests.
echo
echo "[4/4] Running tests..."
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-localhost}"

# Delta 4.1.0 mandates Java 17; Comet itself builds fine on 17+. If the user
# is iterating with a newer JDK on Comet, point this at a JDK 17 install for
# SBT. Typical usage: `DELTA_JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
if [[ -n "${DELTA_JAVA_HOME:-}" ]]; then
  echo "  Using DELTA_JAVA_HOME=$DELTA_JAVA_HOME for SBT"
  export JAVA_HOME="$DELTA_JAVA_HOME"
  export PATH="$DELTA_JAVA_HOME/bin:$PATH"
fi

# Reset Gradle daemon + script cache. A daemon started with an older JDK
# sticks around and will be reused by Delta's `./gradlew` inside
# `icebergShaded/assembly`, and Gradle's compiled-build-script cache stores
# classfiles whose major version matches the JDK of the earlier run.
pkill -f 'GradleDaemon' 2>/dev/null || true
rm -rf ~/.gradle/caches/7.5.1/scripts ~/.gradle/caches/7.6.3/scripts 2>/dev/null || true

case "$TEST_FILTER" in
  smoke)
    build/sbt "$SBT_MODULE/testOnly org.apache.spark.sql.delta.CometSmokeTest"
    ;;
  full)
    build/sbt "$SBT_MODULE/test"
    ;;
  *)
    build/sbt "$SBT_MODULE/testOnly $TEST_FILTER"
    ;;
esac

echo
echo "Done."
