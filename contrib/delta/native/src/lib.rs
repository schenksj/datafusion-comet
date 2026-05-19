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

//! Delta Lake integration for Apache DataFusion Comet.
//!
//! Enabled in core via `--features contrib-delta`. Default builds carry zero
//! Delta surface; this crate is not linked unless the feature is on.
//!
//! This is the initial scaffolding commit. Subsequent commits port the working
//! implementation from the `contrib-delta-pr2` branch piece by piece:
//!   - delta-kernel-rs log replay
//!   - deletion vector filter exec
//!   - column-mapping translation
//!   - partition value parsing
//! All wired into core via the `OpStruct::DeltaScan` dispatcher arm in
//! `native/core/src/execution/planner.rs`.

// Re-export the typed Delta proto messages so contrib-internal code has a stable
// short alias regardless of which crate they ultimately live in. (Today they live
// in core's `datafusion_comet_proto::spark_operator`; if we later move them into a
// contrib-private proto crate, only this re-export changes.)
pub use datafusion_comet_proto::spark_operator::{
    DeltaColumnMapping, DeltaPartitionValue, DeltaScan, DeltaScanCommon, DeltaScanTask,
    DeltaScanTaskList,
};
