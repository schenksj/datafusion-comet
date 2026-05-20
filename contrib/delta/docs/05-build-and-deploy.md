# Build, packaging, and deployment

## The two switches

Two things must be enabled together to get Delta acceleration:

| Switch | What it controls |
|---|---|
| Maven: `-Pcontrib-delta` | Scala/Java contrib classes are compiled and packaged into `comet-spark` JAR. The Spark extension is registered. |
| Cargo: `--features contrib-delta` (on `native/core`) | The contrib Rust crate is linked into `libcomet`. The JNI symbol `Java_…_planDeltaScan` is exported. |

Mismatched switches produce a clear failure:

- JAR with contrib, dylib without → first Delta query: `UnsatisfiedLinkError: planDeltaScan`
- JAR without contrib, dylib with → contrib classes simply absent; `DeltaIntegration.transformV1IfDelta` returns `None`; all Delta queries go through Spark's reader

The Maven `verify` phase has no cross-language assertion; getting both
switches set is on the operator.

## Cargo manifest structure

```
native/
├── core/                          # Comet core native code (workspace member)
│   ├── Cargo.toml                 # arrow = "58", with feature contrib-delta = ["delta-contrib-impl"]
│   └── src/execution/planner/contrib_delta_scan.rs
└── proto/                         # Comet proto definitions
    └── Cargo.toml

contrib/delta/native/              # Standalone, NOT a workspace member
├── Cargo.toml                     # arrow = "57" (kernel-rs's pin)
└── src/
    ├── lib.rs
    ├── engine.rs
    ├── scan.rs
    ├── planner.rs
    ├── dv_filter.rs
    ├── synthetic_columns.rs
    └── jni.rs
```

The contrib crate is referenced from `native/core/Cargo.toml` as a path
dependency gated by the `contrib-delta` feature:

```toml
[features]
contrib-delta = ["delta-contrib-impl"]

[dependencies]
delta-contrib-impl = {
    package = "comet-delta-contrib",
    path = "../../contrib/delta/native",
    optional = true,
}
```

`native/core` and `contrib/delta/native` are NOT in the same workspace, so
Cargo resolves their dependencies independently. This is the only way to
keep arrow-57 (kernel-rs's pin) and arrow-58 (Comet core's pin) in the same
final binary without cross-contamination — they end up as distinct crate
graphs and the boundary between them is the Arrow C Data Interface
(stable across versions).

If you `cargo build` directly in `contrib/delta/native/`, you get a `.rlib`
that does nothing useful — there's no JNI entry compiled in until the
parent `native/core` crate enables the `contrib-delta` feature and pulls
this crate in. Always build from `native/core` (or via the `make`
targets / Maven invocations that do so).

## Maven profile

`spark/pom.xml` declares the `contrib-delta` profile:

```xml
<profile>
  <id>contrib-delta</id>
  <build>
    <resources>
      <resource><directory>${project.basedir}/../contrib/delta/src/main/resources</directory></resource>
    </resources>
    <plugins>
      <plugin>
        <artifactId>build-helper-maven-plugin</artifactId>
        <executions>
          <execution>
            <id>add-contrib-delta-sources</id>
            <goals><goal>add-source</goal></goals>
            <configuration>
              <sources>
                <source>${project.basedir}/../contrib/delta/src/main/scala</source>
              </sources>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
    <dependencies>
      <!-- delta-spark, kernel-rs binding (test scope), Hadoop S3A -->
    </dependencies>
  </build>
</profile>
```

The resources directory contains
`META-INF/services/org.apache.spark.sql.SparkSessionExtensionsProvider`
listing the contrib's Spark extension class. With the profile active,
that resource lands in the JAR and Spark auto-loads the extension when
`spark.sql.extensions` is set (or, in Delta-aware Spark sessions, when
Delta itself adds extensions and we register alongside).

## What the `comet-spark` JAR looks like

| File or class | Default build | `-Pcontrib-delta` build |
|---|---|---|
| `org.apache.comet.rules.CometScanRule` | yes | yes |
| `org.apache.comet.rules.DeltaIntegration` | yes (reflective bridge, returns None at runtime) | yes |
| `org.apache.comet.contrib.delta.DeltaScanRule` | absent | present |
| `org.apache.comet.contrib.delta.CometDeltaNativeScan` | absent | present |
| `org.apache.comet.contrib.delta.DeltaPlanDataInjector` | absent | present |
| `META-INF/services/...SparkSessionExtensionsProvider` | absent | present |

A `default` consumer is therefore entirely free of Delta classes. Running
the default JAR against a Delta workload simply means
`DeltaIntegration.transformV1IfDelta` returns `None` and Spark's
unaccelerated path runs.

## What `libcomet` looks like

The dylib produced by `cargo build --release -p comet --features contrib-delta`
contains:

- All of Comet core
- The contrib Rust code, statically linked
- `Java_org_apache_comet_Native_planDeltaScan` exported

Default build (no feature) omits the contrib code entirely; the dispatcher
in `native/core/src/execution/planner/mod.rs` has a `#[cfg(not(feature =
"contrib-delta"))]` arm that returns a clear error if a `DeltaScan` proto
somehow arrives:

```rust
#[cfg(not(feature = "contrib-delta"))]
OpStruct::DeltaScan(_) => Err(DataFusionError::Plan(
    "DeltaScan operator received but native build does not include contrib-delta feature".into(),
)),
```

In practice this can't fire because the JVM side wouldn't have produced a
`DeltaScan` proto without the contrib classpath, but defense-in-depth.

## How to build and ship

For a Comet binary that supports Delta:

```bash
# Build the native dylib with the contrib feature
make release  # or: cargo build -p comet --features contrib-delta --release

# Build and install the comet-spark JAR with the contrib profile
mvn -Pspark-4.1 -Pcontrib-delta -DskipTests install
```

The `make release` target reads `COMET_FEATURES` if set; for our case the
Maven invocation also has to pass the feature, which the `spark` POM
arranges via `comet.native.features=contrib-delta` when the profile is
active.

For default (no Delta) builds, omit both switches:

```bash
make release
mvn -Pspark-4.1 -DskipTests install
```

## CI matrix expectation

CI should exercise both build paths. Adding a `-Pcontrib-delta` matrix
entry to the existing Spark profile axis is sufficient — the regression
suite then runs against the Delta test diff (`dev/diffs/delta/4.1.0.diff`)
under that matrix entry.

## Local iteration tips

- **Iterate on Scala only**: `mvn -Pspark-4.1 -Pcontrib-delta -DskipTests
  -pl spark -am install` — skips the native build, reuses your existing
  dylib
- **Iterate on Rust only**: build native (`cargo build -p comet --features
  contrib-delta`), then `cp target/release/libcomet.dylib
  spark/target/...` if you want to skip the JAR repack — the contrib
  classes are still wired the same way

The regression script `contrib/delta/dev/run-regression.sh` handles all of
this from scratch but is slow (full install + sbt + JVM forks).
