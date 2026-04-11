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

//! Delta log replay: given a table URL, return the list of active parquet
//! files with partition values, record-count stats, and deletion-vector
//! flags.
//!
//! Ported from tantivy4java's `delta_reader/scan.rs`. The API is the
//! smallest possible surface that still proves end-to-end kernel
//! integration: `Snapshot::builder_for(url)` → `scan_builder().build()` →
//! `scan_metadata(&engine)` → `visit_scan_files(...)`.
//!
//! **Critical gotcha** preserved from the reference implementation: kernel
//! internally does `table_root.join("_delta_log/")`, and `Url::join` will
//! *replace* the last path segment if the base URL does not end in `/`. So
//! `normalize_url` always appends a trailing slash.

use std::collections::HashMap;
use url::Url;

use delta_kernel::snapshot::Snapshot;

use super::engine::{create_engine, DeltaStorageConfig};
use super::error::{DeltaError, DeltaResult};

/// Metadata for a single active parquet file in a Delta table.
///
/// Plain Rust types only — no arrow / parquet / object_store types. This is
/// the boundary at which kernel's isolated dep subtree meets the rest of
/// Comet.
#[derive(Debug, Clone)]
pub struct DeltaFileEntry {
    /// Parquet file path, relative to the table root.
    pub path: String,
    /// File size in bytes.
    pub size: i64,
    /// Last-modified time as epoch millis.
    pub modification_time: i64,
    /// Record count from log stats, if known.
    pub num_records: Option<u64>,
    /// Partition column → value mapping from the add action.
    pub partition_values: HashMap<String, String>,
    /// True if this file has an associated deletion vector.
    ///
    /// The actual DV bytes + offset are fetched lazily by the executor
    /// via a separate API once Phase 3 lands.
    pub has_deletion_vector: bool,
}

/// List every active parquet file in a Delta table at the given version.
///
/// Returns `(entries, actual_version)` where `actual_version` is the
/// snapshot version that was actually read — equal to `version` when
/// specified, or the latest version otherwise.
pub fn list_delta_files(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
) -> DeltaResult<(Vec<DeltaFileEntry>, u64)> {
    let url = normalize_url(url_str)?;
    let engine = create_engine(&url, config)?;

    let snapshot = {
        let mut builder = Snapshot::builder_for(url);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        builder.build(&engine)?
    };
    let actual_version = snapshot.version();

    let scan = snapshot.scan_builder().build()?;

    let mut entries: Vec<DeltaFileEntry> = Vec::new();
    let scan_metadata = scan.scan_metadata(&engine)?;

    for meta_result in scan_metadata {
        let meta = meta_result?;
        entries = meta.visit_scan_files(entries, |acc, scan_file| {
            let num_records = scan_file.stats.as_ref().map(|s| s.num_records);
            let has_dv = scan_file.dv_info.has_vector();
            acc.push(DeltaFileEntry {
                path: scan_file.path,
                size: scan_file.size,
                modification_time: scan_file.modification_time,
                num_records,
                partition_values: scan_file.partition_values,
                has_deletion_vector: has_dv,
            });
        })?;
    }

    Ok((entries, actual_version))
}

/// Normalize a table URL so kernel's `table_root.join("_delta_log/")`
/// appends rather than replaces. Bare paths become `file://` URLs.
pub(crate) fn normalize_url(url_str: &str) -> DeltaResult<Url> {
    if url_str.starts_with("s3://")
        || url_str.starts_with("s3a://")
        || url_str.starts_with("az://")
        || url_str.starts_with("azure://")
        || url_str.starts_with("abfs://")
        || url_str.starts_with("abfss://")
        || url_str.starts_with("file://")
    {
        let mut url = Url::parse(url_str).map_err(|e| DeltaError::InvalidUrl {
            url: url_str.to_string(),
            source: e,
        })?;
        ensure_trailing_slash(&mut url);
        Ok(url)
    } else {
        let abs_path =
            std::path::Path::new(url_str)
                .canonicalize()
                .map_err(|e| DeltaError::PathResolution {
                    path: url_str.to_string(),
                    source: e,
                })?;
        Url::from_directory_path(&abs_path).map_err(|_| DeltaError::PathToUrl {
            path: abs_path.display().to_string(),
        })
    }
}

fn ensure_trailing_slash(url: &mut Url) {
    let path = url.path().to_string();
    if !path.ends_with('/') {
        url.set_path(&format!("{path}/"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_trailing_slash() {
        let url = normalize_url("file:///tmp/my_table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {url}");
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        let url = normalize_url("file:///tmp/my_table/").unwrap();
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        let url = normalize_url("s3://bucket/path/to/table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {url}");
    }

    #[test]
    fn test_normalize_url_join_behavior() {
        // The critical invariant: joining `_delta_log/` onto a normalized
        // URL must *append*, not replace the last segment.
        let url = normalize_url("file:///tmp/my_table").unwrap();
        let log_url = url.join("_delta_log/").unwrap();
        assert_eq!(log_url.as_str(), "file:///tmp/my_table/_delta_log/");
    }

    #[test]
    fn test_list_delta_files_local() {
        // Hand-build a minimal Delta table in a tempdir: one protocol action,
        // one metadata action, one add action. No Parquet data needed —
        // we're exercising the log-replay path only.
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_delta");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        let commit0 = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}"#,
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":5000,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":50}"}}"#,
        ]
        .join("\n");
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();
        std::fs::write(table_dir.join("part-00000.parquet"), [0u8]).unwrap();

        let config = DeltaStorageConfig::default();
        let (entries, version) =
            list_delta_files(table_dir.to_str().unwrap(), &config, None).unwrap();

        assert_eq!(version, 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "part-00000.parquet");
        assert_eq!(entries[0].size, 5000);
        assert_eq!(entries[0].num_records, Some(50));
        assert!(!entries[0].has_deletion_vector);
    }
}
