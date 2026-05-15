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
//! feature flag on the `datafusion-comet` crate. The crate's `#[ctor]` registers a
//! `ContribOperatorPlanner` under kind `"delta-scan"` at library-init time; core's
//! planner dispatches `OpStruct::ContribOp` payloads with that kind into this crate.
//!
//! ## P1 scaffold
//!
//! This file currently registers a stub planner that returns
//! `ContribError::Plan("delta-scan: not yet implemented (P1 scaffold)")`. The native
//! source port (engine / scan / predicate / dv_filter / jni) lands in P2; the real
//! `ContribOperatorPlanner::plan` body lands in P3. The scaffold exists so the build
//! wiring (Cargo feature, Maven profile, proto generation, ctor registration) can be
//! verified end-to-end before any logic is ported.
//!
//! ## Quarantine boundary
//!
//! delta-kernel-rs pins arrow-57 / object_store-0.12 internally; nothing typed by
//! those crates leaves this module. Only plain Rust types (`String`, `i64`,
//! `HashMap<String, String>`, our own `DeltaFileEntry`) cross the boundary to the rest
//! of Comet.

use std::sync::Arc;

use comet_contrib_spi::{
    register_contrib_planner, ContribError, ContribOperatorPlanner, ContribPlannerContext,
};
use datafusion::physical_plan::ExecutionPlan;

/// Generated proto types for `delta_operator.proto`. `build.rs` writes the module here
/// at compile time; `src/generated/` is gitignored.
pub mod proto {
    include!(concat!("generated/", "comet.contrib.delta.rs"));
}

/// Stable kind identifier for the Delta scan operator. The Scala-side serde writes this
/// into the `ContribOp.kind` proto field; the native dispatcher looks up the planner by
/// it.
pub const DELTA_SCAN_KIND: &str = "delta-scan";

/// P1 stub. P3 replaces this with the real ~150-line dispatcher port that builds a
/// `DataSourceExec` over Comet's tuned ParquetSource, wraps in `DeltaDvFilterExec`
/// when DVs are present, and applies the rename projection for column-mapping tables.
struct DeltaScanPlanner;

impl ContribOperatorPlanner for DeltaScanPlanner {
    fn plan(
        &self,
        _ctx: &dyn ContribPlannerContext,
        _payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        Err(ContribError::Plan(format!(
            "{DELTA_SCAN_KIND}: not yet implemented (PR2 P1 scaffold). \
             Wire-up is complete -- planner dispatch reaches this code path -- but \
             the dispatcher body lands in P3."
        )))
    }
}

#[ctor::ctor]
fn register() {
    let _ = std::panic::catch_unwind(|| {
        register_contrib_planner(DELTA_SCAN_KIND, Arc::new(DeltaScanPlanner));
    })
    .map_err(|panic| {
        eprintln!("comet-contrib-delta: #[ctor] panicked: {panic:?}");
    });
}
