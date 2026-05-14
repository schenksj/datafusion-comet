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

//! delta-kernel-rs integration for Comet, as a contrib extension.
//!
//! This crate is linked into core's `libcomet` cdylib via the `contrib-delta` Cargo
//! feature flag on the `datafusion-comet` crate. At library-init time the `#[ctor]`
//! below registers a `ContribOperatorPlanner` under kind `"delta-scan"`; core's planner
//! dispatches `OpStruct::ContribOp` payloads with that kind to it.
//!
//! P1 SKELETON ONLY -- subsequent commits port the actual Delta source files.

/// Generated proto types for the contrib's wire schema (DeltaScan, DeltaScanCommon, etc.).
/// `build.rs` writes the module file here at compile time; `src/generated/` is gitignored.
pub mod proto {
    include!(concat!("generated/", "comet.contrib.delta.rs"));
}

/// Stable kind identifier for the Delta scan operator. Scala-side serde writes this into
/// the `ContribOp.kind` proto field; the native dispatcher looks up the planner by it.
pub const DELTA_SCAN_KIND: &str = "delta-scan";

#[ctor::ctor]
fn register() {
    log::info!(
        "comet-contrib-delta: registering ContribOperatorPlanner kind={DELTA_SCAN_KIND:?}"
    );
    // TODO P4: register DeltaScanPlanner once the dispatcher body lands.
}
