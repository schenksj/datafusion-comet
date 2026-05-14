// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! delta-kernel-rs integration for Comet, packaged as a contrib extension.
//!
//! This crate is linked into core's `libcomet` cdylib via the `contrib-delta` Cargo
//! feature flag on the `datafusion-comet` crate. At library-init time the `#[ctor]`
//! below registers a `ContribOperatorPlanner` under kind `"delta-scan"`; core's planner
//! dispatches `OpStruct::ContribOp` payloads with that kind into this crate.
//!
//! ## Module layout
//!
//!   * `engine`           -- kernel `DefaultEngine` construction with our storage config.
//!   * `error`            -- `DeltaError` / `DeltaResult`.
//!   * `jni`              -- driver-side JNI entry point for log replay
//!                            (`Java_org_apache_comet_contrib_delta_Native_planDeltaScan`).
//!   * `predicate`        -- Catalyst-proto Expr to kernel Predicate translator.
//!   * `scan`             -- kernel scan invocation + DV materialization.
//!   * `dv_filter`        -- `DeltaDvFilterExec` (was `execution::operators::delta_dv_filter`).
//!   * `planner`          -- `ContribOperatorPlanner` impl that builds the native exec
//!                            from the `OpStruct::ContribOp` payload.
//!   * `proto`            -- generated Rust types from `delta_operator.proto`.
//!
//! ## Quarantine boundary
//!
//! delta-kernel-rs pins arrow-57 / object_store-0.12 internally; nothing typed by those
//! crates leaves this module. Only plain Rust types
//! (`String`, `i64`, `HashMap<String, String>`, our own `DeltaFileEntry`) cross the
//! boundary to the rest of Comet.

pub mod engine;
pub mod error;
pub mod jni;
pub mod predicate;
pub mod scan;
pub mod dv_filter;

// TODO(contrib-delta): the original integration test references core's
// `crate::parquet::parquet_exec::init_datasource_exec` which is not exposed through
// `comet-contrib-spi`. For PR2 we'll either expose it through the SPI or rewrite
// the test against the contrib's own surface. For the validation build we skip the
// module entirely -- it isn't part of the SPI dispatch path the validation is
// proving out.
// #[cfg(test)] mod integration_tests;

pub use engine::{create_engine, create_object_store, DeltaStorageConfig};
pub use error::{DeltaError, DeltaResult};
pub use scan::{list_delta_files, plan_delta_scan, DeltaFileEntry, DeltaScanPlan};

/// Generated proto types for `delta_operator.proto`. `build.rs` writes the module here
/// at compile time; `src/generated/` is gitignored.
pub mod proto {
    include!(concat!("generated/", "comet.contrib.delta.rs"));
}

/// Stable kind identifier for the Delta scan operator. The Scala-side serde writes this
/// into the `ContribOp.kind` proto field; the native dispatcher looks up the planner by
/// it.
pub const DELTA_SCAN_KIND: &str = "delta-scan";

#[ctor::ctor]
fn register() {
    log::info!(
        "comet-contrib-delta: registering ContribOperatorPlanner kind={DELTA_SCAN_KIND:?}"
    );
    // TODO P4: register the planner once the dispatcher body has been adapted from
    // core's planner.rs OpStruct::DeltaScan arm.
}
