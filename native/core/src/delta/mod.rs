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

//! Native Delta Lake support via delta-kernel-rs.
//!
//! This module is intentionally quarantined: it imports `delta_kernel`,
//! `object_store_kernel` (object_store 0.12), and kernel's own `arrow` (57),
//! which live in a *separate* dep subtree from Comet's `arrow 58` /
//! `object_store 0.13`. Nothing typed by those crates leaves this module —
//! only plain Rust types (`String`, `i64`, `HashMap<String,String>`, our own
//! `DeltaFileEntry`, etc.) cross the boundary to the rest of Comet.
//!
//! The public API is deliberately small:
//!   - `DeltaStorageConfig`: credential struct (mirrors tantivy4java).
//!   - `list_delta_files`: log-replay driver that returns the active file
//!     list for a given snapshot version.
//!   - `DeltaFileEntry`: per-file metadata with partition values + DV flag +
//!     record-count stats.
//!
//! Later phases add expression translation, deletion-vector decoding, and
//! the `DeltaScanExec` physical operator; those live in sibling files.

pub mod engine;
pub mod error;
pub mod jni;
pub mod predicate;
pub mod scan;

#[cfg(test)]
mod integration_tests;

pub use engine::{create_engine, create_object_store, DeltaStorageConfig};
pub use error::{DeltaError, DeltaResult};
pub use scan::{list_delta_files, plan_delta_scan, DeltaFileEntry, DeltaScanPlan};
