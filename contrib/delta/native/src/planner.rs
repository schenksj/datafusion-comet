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

//! `ContribOperatorPlanner` implementation for `kind = "delta-scan"`.
//!
//! ## Status: STUB (validation finding -- Gap #4)
//!
//! This module is currently a stub. The dispatcher body (in
//! `delta-kernel-phase-1`'s `planner.rs:1496-1721`) needs five core-side helpers
//! that the current `comet-contrib-spi` does not expose:
//!
//!   1. `init_datasource_exec(...)`           -- 16-arg builder that produces a
//!      `DataSourceExec` over Comet's tuned `ParquetSource`. Lives in
//!      `crate::parquet::parquet_exec::init_datasource_exec`.
//!   2. `prepare_object_store_with_configs`   -- registers an object store with the
//!      runtime env and returns an `ObjectStoreUrl`. Lives in
//!      `crate::execution::operators::scan` (or similar).
//!   3. `convert_spark_types_to_arrow_schema` -- turns a `Vec<SparkStructField>`
//!      proto into an `arrow::SchemaRef`. Lives in `crate::execution::serde`.
//!   4. A `create_expr` capability                -- builds a `PhysicalExpr` from a
//!      Catalyst `Expr` proto + a schema. Today this is a method on
//!      `PhysicalPlanner`, not free-standing.
//!   5. Access to a `SessionContext` reference for runtime-env mutation and
//!      session-config lookups.
//!
//! Decoding the `DeltaScan` proto, building DV-aware `FileGroup`s, applying the
//! column-mapping rewrites, and constructing `DeltaDvFilterExec` (and the rename
//! `ProjectionExec`) are all contrib-local concerns and stay in this file. They
//! just can't compile until the SPI grows the missing surfaces.
//!
//! See `PR1-delta-port-findings.md` Gap #4 for the full proposal: extend
//! `ContribOperatorPlanner::plan` to take a `&dyn ContribPlannerContext` and have
//! core implement that trait against the existing helpers. Once the SPI is
//! extended, the body below becomes a ~150-line straight port of the
//! `OpStruct::DeltaScan` arm.

use std::sync::Arc;

use comet_contrib_spi::{ContribError, ContribOperatorPlanner};
use datafusion::physical_plan::ExecutionPlan;
use prost::Message;

use crate::proto::DeltaScan;

/// Planner for `kind = "delta-scan"`. Stub until Gap #4 is closed.
pub struct DeltaScanPlanner;

impl ContribOperatorPlanner for DeltaScanPlanner {
    fn plan(
        &self,
        payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        // Decode the payload eagerly so a malformed proto fails loudly with a
        // BadPayload error instead of NotImplemented -- this much we CAN do
        // through the existing SPI.
        let scan = DeltaScan::decode(payload).map_err(|e| {
            ContribError::BadPayload(format!("failed to decode DeltaScan proto: {e}"))
        })?;

        Err(ContribError::Plan(format!(
            "delta-scan planner is stubbed pending SPI extension (Gap #4); \
             received DeltaScan with {} tasks, common.is_some()={}",
            scan.tasks.len(),
            scan.common.is_some(),
        )))
    }
}
