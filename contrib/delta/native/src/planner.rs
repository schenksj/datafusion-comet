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
//! Builds a `DataSourceExec` over Comet's tuned `ParquetSource` (via the SPI's
//! `ContribPlannerContext::build_parquet_datasource_exec`), wraps it in a
//! `DeltaDvFilterExec` when any file carries a deletion vector, and applies a rename
//! `ProjectionExec` when column mapping is active.
//!
//! Ported almost verbatim from the `delta-kernel-phase-1` branch's
//! `OpStruct::DeltaScan` arm in `native/core/src/execution/planner.rs:1496-1721`. The
//! only substantive changes are call-site rewrites onto the SPI's `ctx` methods.

use std::collections::HashMap;
use std::sync::Arc;

use comet_contrib_spi::{
    ContribError, ContribOperatorPlanner, ContribPlannerContext, ParquetDatasourceParams,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRewriter,
};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;
use object_store::path::Path;
use prost::Message;
use url::Url;

use crate::dv_filter::DeltaDvFilterExec;
use crate::proto::{DeltaScan, DeltaScanTask};

/// Planner for `kind = "delta-scan"`. Registered against the contrib registry by the
/// crate's `#[ctor]` (see `lib.rs`).
pub struct DeltaScanPlanner;

impl ContribOperatorPlanner for DeltaScanPlanner {
    fn plan(
        &self,
        ctx: &dyn ContribPlannerContext,
        payload: &[u8],
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, ContribError> {
        let scan = DeltaScan::decode(payload).map_err(|e| {
            ContribError::BadPayload(format!("failed to decode DeltaScan proto: {e}"))
        })?;
        let common = scan.common.as_ref().ok_or_else(|| {
            ContribError::BadPayload(
                "DeltaScan proto missing 'common' field (Scala serialization error)".into(),
            )
        })?;

        let required_schema: SchemaRef = ctx.convert_spark_schema(&common.required_schema);
        let mut data_schema: SchemaRef = ctx.convert_spark_schema(&common.data_schema);
        let partition_schema: SchemaRef = ctx.convert_spark_schema(&common.partition_schema);

        // Column mapping: substitute physical names into data_schema so ParquetSource
        // projects by the names actually in the file. A rename projection on top maps
        // physical names back to the logical names upstream operators expect.
        let logical_to_physical: HashMap<String, String> = common
            .column_mappings
            .iter()
            .map(|cm| (cm.logical_name.clone(), cm.physical_name.clone()))
            .collect();
        let has_column_mapping = !logical_to_physical.is_empty();
        if has_column_mapping {
            let new_fields: Vec<_> = data_schema
                .fields()
                .iter()
                .map(|f| {
                    if let Some(physical) = logical_to_physical.get(f.name()) {
                        Arc::new(Field::new(
                            physical,
                            f.data_type().clone(),
                            f.is_nullable(),
                        ))
                    } else {
                        Arc::clone(f)
                    }
                })
                .collect();
            data_schema = Arc::new(Schema::new(new_fields));
        }
        let projection_vector: Vec<usize> = common
            .projection_vector
            .iter()
            .map(|offset| *offset as usize)
            .collect();

        // Empty-partition fast path.
        if scan.tasks.is_empty() {
            return Ok(Arc::new(EmptyExec::new(required_schema)));
        }

        let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ContribError> = common
            .data_filters
            .iter()
            .map(|expr| {
                let filter = ctx.build_physical_expr(expr, Arc::clone(&required_schema))?;
                if has_column_mapping {
                    let mut rewriter = ColumnMappingFilterRewriter {
                        logical_to_physical: &logical_to_physical,
                        data_schema: &data_schema,
                    };
                    filter
                        .rewrite(&mut rewriter)
                        .data()
                        .map_err(|e| ContribError::Plan(format!("ColumnMappingFilterRewriter: {e}")))
                } else {
                    Ok(filter)
                }
            })
            .collect();

        let object_store_options: HashMap<String, String> = common
            .object_store_options
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        // Build PartitionedFiles. Kernel has already resolved each file path to an
        // absolute URL on the driver, so we thread them straight through. Delta stores
        // TIMESTAMP partition values in the JVM default TZ; pass the session TZ so
        // partition-value parsing produces the correct instant.
        let files = build_delta_partitioned_files(
            &scan.tasks,
            partition_schema.as_ref(),
            common.session_timezone.as_str(),
        )?;

        // Split files by DV presence -- each DV'd file becomes its own FileGroup so the
        // DeltaDvFilterExec's per-partition mapping is 1:1 with one physical parquet
        // file. All non-DV files go in a single combined group.
        let mut file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
        let mut deleted_indexes_per_group: Vec<Vec<u64>> = Vec::new();
        let mut non_dv_files: Vec<PartitionedFile> = Vec::new();
        for (file, task) in files.into_iter().zip(scan.tasks.iter()) {
            if task.deleted_row_indexes.is_empty() {
                non_dv_files.push(file);
            } else {
                file_groups.push(vec![file]);
                deleted_indexes_per_group.push(task.deleted_row_indexes.clone());
            }
        }
        if !non_dv_files.is_empty() {
            file_groups.push(non_dv_files);
            deleted_indexes_per_group.push(Vec::new());
        }

        // Pick any one file to register the object store (they all share the same root).
        let one_file = scan
            .tasks
            .first()
            .map(|t| t.file_path.clone())
            .ok_or_else(|| {
                ContribError::Plan(
                    "DeltaScan has no tasks after split-mode injection".into(),
                )
            })?;
        let (object_store_url, _root_path) =
            ctx.prepare_object_store(one_file, &object_store_options)?;

        // ignore_missing_files: Delta-specific flag (true when user has set
        // `spark.sql.files.ignoreMissingFiles=true`). Not exposed through the SPI's
        // ParquetDatasourceParams today; deferred until a real-world need surfaces.
        let _ignore_missing_files = common.ignore_missing_files;

        let params = ParquetDatasourceParams::new(
            Arc::clone(&required_schema),
            object_store_url,
            file_groups,
        )
        .with_data_schema(data_schema)
        .with_partition_schema(partition_schema)
        .with_projection_vector(projection_vector)
        .with_data_filters(data_filters?)
        .with_session_timezone(common.session_timezone.clone())
        .with_case_sensitive(common.case_sensitive);
        let delta_exec = ctx.build_parquet_datasource_exec(params)?;

        // Wrap in a DV filter when any partition has a DV. Skip the wrapper otherwise
        // to avoid the per-batch pass-through cost in the common "no DVs" case.
        let final_exec: Arc<dyn ExecutionPlan> =
            if deleted_indexes_per_group.iter().any(|v| !v.is_empty()) {
                Arc::new(
                    DeltaDvFilterExec::new(delta_exec, deleted_indexes_per_group)
                        .map_err(|e| ContribError::Plan(format!("DeltaDvFilterExec: {e}")))?,
                )
            } else {
                delta_exec
            };

        // When column mapping is active, the scan's output schema carries PHYSICAL
        // column names. Upstream operators reference columns by LOGICAL name, so add a
        // ProjectionExec aliasing each physical column back to its logical name.
        let scan_out = final_exec.schema();
        let needs_rename = has_column_mapping
            && required_schema.fields().len() == scan_out.fields().len()
            && required_schema
                .fields()
                .iter()
                .zip(scan_out.fields().iter())
                .any(|(req, phys)| req.name() != phys.name());
        let with_rename: Arc<dyn ExecutionPlan> = if needs_rename {
            let phys_to_logical: HashMap<&str, &str> = scan_out
                .fields()
                .iter()
                .zip(required_schema.fields().iter())
                .map(|(phys, req)| (phys.name().as_str(), req.name().as_str()))
                .collect();
            let projections: Vec<(Arc<dyn PhysicalExpr>, String)> = scan_out
                .fields()
                .iter()
                .enumerate()
                .map(|(idx, phys_field)| {
                    let col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(phys_field.name(), idx));
                    let alias = phys_to_logical
                        .get(phys_field.name().as_str())
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| phys_field.name().clone());
                    (col, alias)
                })
                .collect();
            Arc::new(
                ProjectionExec::try_new(projections, final_exec)
                    .map_err(|e| ContribError::Plan(format!("rename ProjectionExec: {e}")))?,
            )
        } else {
            final_exec
        };

        Ok(with_rename)
    }
}

/// Rewrites Column references in a PhysicalExpr from logical names/indices (in
/// required_schema) to physical names/indices (in data_schema). Used when Delta column
/// mapping is active so pushed-down data filters match the DataSourceExec's physical
/// names.
struct ColumnMappingFilterRewriter<'a> {
    logical_to_physical: &'a HashMap<String, String>,
    data_schema: &'a SchemaRef,
}

impl TreeNodeRewriter for ColumnMappingFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Self::Node,
    ) -> datafusion::common::Result<Transformed<Self::Node>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Some(physical_name) = self.logical_to_physical.get(column.name()) {
                if let Some(idx) = self
                    .data_schema
                    .fields()
                    .iter()
                    .position(|f| f.name() == physical_name)
                {
                    return Ok(Transformed::yes(Arc::new(Column::new(physical_name, idx))));
                }
                log::warn!(
                    "Column mapping: physical name '{}' for logical '{}' not found in \
                     data_schema; filter may fail at execution time",
                    physical_name,
                    column.name()
                );
            }
            Ok(Transformed::no(node))
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Convert `DeltaScanTask`s into DataFusion `PartitionedFile`s. Delta's add.path is
/// already an absolute URL once kernel has resolved it on the driver.
fn build_delta_partitioned_files(
    tasks: &[DeltaScanTask],
    partition_schema: &Schema,
    session_tz: &str,
) -> Result<Vec<PartitionedFile>, ContribError> {
    let mut files = Vec::with_capacity(tasks.len());
    for task in tasks {
        let url = Url::parse(task.file_path.as_ref())
            .map_err(|e| ContribError::Plan(format!("Invalid Delta file URL: {e}")))?;
        let path = Path::from_url_path(url.path())
            .map_err(|e| ContribError::Plan(format!("from_url_path: {e}")))?;

        let mut partitioned_file = match (task.byte_range_start, task.byte_range_end) {
            (Some(start), Some(end)) => PartitionedFile::new_with_range(
                String::new(),
                task.file_size,
                start as i64,
                end as i64,
            ),
            _ => PartitionedFile::new(String::new(), task.file_size),
        };
        partitioned_file.object_meta.location = path;

        let mut partition_values: Vec<ScalarValue> =
            Vec::with_capacity(partition_schema.fields().len());
        for field in partition_schema.fields() {
            let proto_value = task
                .partition_values
                .iter()
                .find(|p| p.name == *field.name());
            let scalar = match proto_value.and_then(|p| p.value.clone()) {
                Some(s) => parse_delta_partition_scalar(&s, field.data_type(), session_tz)
                    .map_err(|e| {
                        ContribError::Plan(format!(
                            "Failed to parse Delta partition value for column '{}': {e}",
                            field.name()
                        ))
                    })?,
                None => ScalarValue::try_from(field.data_type()).map_err(|e| {
                    ContribError::Plan(format!(
                        "Failed to build null partition value for column '{}': {e}",
                        field.name()
                    ))
                })?,
            };
            partition_values.push(scalar);
        }
        partitioned_file.partition_values = partition_values;
        files.push(partitioned_file);
    }
    Ok(files)
}

/// Parse a Delta partition value string into a `ScalarValue`. Honours session TZ for
/// TIMESTAMP columns. Delta writes TIMESTAMP partition values in the JVM default TZ
/// (`yyyy-MM-dd HH:mm:ss[.S]`); DataFusion's default parser interprets them as UTC which
/// would be off by the session offset.
fn parse_delta_partition_scalar(
    s: &str,
    dt: &DataType,
    session_tz: &str,
) -> Result<ScalarValue, String> {
    match dt {
        DataType::Timestamp(unit, tz_opt) => {
            use chrono::{DateTime, NaiveDateTime, TimeZone};
            use chrono_tz::Tz;
            if tz_opt.is_none() {
                let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .map_err(|e| format!("cannot parse TIMESTAMP_NTZ '{s}': {e}"))?;
                let micros = chrono::Utc.from_utc_datetime(&naive).timestamp_micros();
                return Ok(match unit {
                    datafusion::arrow::datatypes::TimeUnit::Microsecond => {
                        ScalarValue::TimestampMicrosecond(Some(micros), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Millisecond => {
                        ScalarValue::TimestampMillisecond(Some(micros / 1_000), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Nanosecond => {
                        ScalarValue::TimestampNanosecond(Some(micros.saturating_mul(1_000)), None)
                    }
                    datafusion::arrow::datatypes::TimeUnit::Second => {
                        ScalarValue::TimestampSecond(Some(micros / 1_000_000), None)
                    }
                });
            }
            let micros = if let Ok(dt_with_tz) = DateTime::parse_from_rfc3339(s) {
                dt_with_tz.timestamp_micros()
            } else if let Ok(dt_with_tz) =
                DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f %z")
                    .or_else(|_| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S %z"))
            {
                dt_with_tz.timestamp_micros()
            } else {
                let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .map_err(|e| format!("cannot parse timestamp '{s}': {e}"))?;
                use chrono::{FixedOffset, LocalResult};
                fn parse_fixed_offset(s: &str) -> Option<FixedOffset> {
                    let trimmed = s.trim();
                    let body = trimmed
                        .strip_prefix("GMT")
                        .or_else(|| trimmed.strip_prefix("UTC"))
                        .unwrap_or(trimmed);
                    if body.is_empty() || body.eq_ignore_ascii_case("Z") {
                        return Some(FixedOffset::east_opt(0).unwrap());
                    }
                    let (sign, rest) = match body.chars().next()? {
                        '+' => (1, &body[1..]),
                        '-' => (-1, &body[1..]),
                        _ => return None,
                    };
                    let secs = if rest.contains(':') {
                        let mut parts = rest.splitn(2, ':');
                        let h: i32 = parts.next()?.parse().ok()?;
                        let m: i32 = parts.next()?.parse().ok()?;
                        h * 3600 + m * 60
                    } else if rest.len() == 4 {
                        let h: i32 = rest[..2].parse().ok()?;
                        let m: i32 = rest[2..].parse().ok()?;
                        h * 3600 + m * 60
                    } else {
                        let h: i32 = rest.parse().ok()?;
                        h * 3600
                    };
                    FixedOffset::east_opt(sign * secs)
                }

                if let Ok(tz) = session_tz.parse::<Tz>() {
                    match tz.from_local_datetime(&naive) {
                        LocalResult::Single(dt) => dt.timestamp_micros(),
                        LocalResult::Ambiguous(earlier, _later) => earlier.timestamp_micros(),
                        LocalResult::None => {
                            chrono::Utc.from_utc_datetime(&naive).timestamp_micros()
                        }
                    }
                } else if let Some(off) = parse_fixed_offset(session_tz) {
                    match off.from_local_datetime(&naive) {
                        LocalResult::Single(dt) => dt.timestamp_micros(),
                        _ => chrono::Utc.from_utc_datetime(&naive).timestamp_micros(),
                    }
                } else {
                    return Err(format!("invalid session TZ '{session_tz}'"));
                }
            };
            match unit {
                datafusion::arrow::datatypes::TimeUnit::Microsecond => Ok(
                    ScalarValue::TimestampMicrosecond(Some(micros), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Millisecond => Ok(
                    ScalarValue::TimestampMillisecond(Some(micros / 1000), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Nanosecond => Ok(
                    ScalarValue::TimestampNanosecond(Some(micros * 1000), tz_opt.clone()),
                ),
                datafusion::arrow::datatypes::TimeUnit::Second => Ok(
                    ScalarValue::TimestampSecond(Some(micros / 1_000_000), tz_opt.clone()),
                ),
            }
        }
        _ => ScalarValue::try_from_string(s.to_string(), dt).map_err(|e| format!("{e}")),
    }
}
