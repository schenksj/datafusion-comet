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

//! `OpStruct::DeltaScan` planner body, feature-gated behind `contrib-delta`.
//!
//! This module mirrors the size and shape of the `OpStruct::IcebergScan` arm in
//! `super::planner` -- the arm itself stays tiny (just dispatches here), and the
//! Delta-specific algorithmic pieces (DV filter exec wrapping, column-mapping
//! rename projection, partition value parsing) live in the
//! [`comet_contrib_delta`] crate.

#![cfg(feature = "contrib-delta")]

use std::collections::HashMap;
use std::sync::Arc;

use comet_contrib_delta::planner::{
    build_delta_partitioned_files, ColumnMappingFilterRewriter,
};
use comet_contrib_delta::DeltaDvFilterExec;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_comet_proto::spark_operator::{DeltaScan, Operator};

use crate::execution::operators::ExecutionError;
use crate::execution::operators::ExecutionError::GeneralError;
use crate::execution::planner::convert_spark_types_to_arrow_schema;
use crate::execution::planner::PhysicalPlanner;
use crate::execution::planner::PlanCreationResult;
use crate::execution::spark_plan::SparkPlan;
use crate::parquet::parquet_exec::init_datasource_exec;
use crate::parquet::parquet_support::prepare_object_store_with_configs;

impl PhysicalPlanner {
    pub(crate) fn plan_delta_scan(
        &self,
        spark_plan: &Operator,
        scan: &DeltaScan,
    ) -> PlanCreationResult {
        let common = scan
            .common
            .as_ref()
            .ok_or_else(|| GeneralError("DeltaScan missing common data".into()))?;

        let required_schema: SchemaRef =
            convert_spark_types_to_arrow_schema(common.required_schema.as_slice());
        let mut data_schema: SchemaRef =
            convert_spark_types_to_arrow_schema(common.data_schema.as_slice());
        let partition_schema: SchemaRef =
            convert_spark_types_to_arrow_schema(common.partition_schema.as_slice());

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
            return Ok((
                vec![],
                vec![],
                Arc::new(SparkPlan::new(
                    spark_plan.plan_id,
                    Arc::new(EmptyExec::new(required_schema)),
                    vec![],
                )),
            ));
        }

        // Build pushed-down data filters, rewriting Column refs to physical names when
        // column mapping is active.
        let data_filters: Result<Vec<Arc<dyn PhysicalExpr>>, ExecutionError> = common
            .data_filters
            .iter()
            .map(|expr| {
                let filter = self
                    .create_expr(expr, Arc::clone(&required_schema))
                    .map_err(|e| GeneralError(format!("DeltaScan filter: {e}")))?;
                if has_column_mapping {
                    let mut rewriter = ColumnMappingFilterRewriter {
                        logical_to_physical: &logical_to_physical,
                        data_schema: &data_schema,
                    };
                    filter
                        .rewrite(&mut rewriter)
                        .data()
                        .map_err(|e| GeneralError(format!("ColumnMappingFilterRewriter: {e}")))
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
        )
        .map_err(GeneralError)?;

        // Split files by DV presence -- each DV'd file becomes its own FileGroup so the
        // DeltaDvFilterExec's per-partition mapping is 1:1 with one physical parquet
        // file. All non-DV files go in a single combined group.
        //
        // EXCEPT when ANY synthetic column is emitted: the per-partition row offset
        // counter in DeltaSyntheticColumnsExec doesn't reset across files within a
        // FileGroup, and every synthetic we emit depends on per-file row position
        // (row_index is per-file by definition; is_row_deleted uses a per-file DV;
        // row_id = baseRowId + physical_row_index is per-file; row_commit_version is
        // per-file constant). So when ANY emit is on, give each file its own group
        // regardless of DV presence so the per-file lookup is well-defined.
        // When metadata columns are requested they're per-file constants too, so
        // need_per_file_groups must include that case to keep partition-index =
        // file-index alignment in DeltaSyntheticColumnsExec.
        let need_per_file_groups = common.emit_row_index
            || common.emit_is_row_deleted
            || common.emit_row_id
            || common.emit_row_commit_version
            || !common.metadata_column_names.is_empty();
        let mut file_groups: Vec<Vec<PartitionedFile>> = Vec::new();
        let mut deleted_indexes_per_group: Vec<Vec<u64>> = Vec::new();
        let mut base_row_ids_per_group: Vec<Option<i64>> = Vec::new();
        let mut default_commit_versions_per_group: Vec<Option<i64>> = Vec::new();
        let mut task_metadata_per_group: Vec<
            comet_contrib_delta::synthetic_columns::TaskMetadata,
        > = Vec::new();
        let mut non_dv_files: Vec<PartitionedFile> = Vec::new();
        for (file, task) in files.into_iter().zip(scan.tasks.iter()) {
            if !task.deleted_row_indexes.is_empty() || need_per_file_groups {
                file_groups.push(vec![file]);
                deleted_indexes_per_group.push(task.deleted_row_indexes.clone());
                base_row_ids_per_group.push(task.base_row_id);
                default_commit_versions_per_group.push(task.default_row_commit_version);
                task_metadata_per_group.push(
                    comet_contrib_delta::synthetic_columns::TaskMetadata {
                        file_path: Some(task.file_path.clone()),
                        file_size: Some(task.file_size as i64),
                        byte_range_start: task.byte_range_start.map(|v| v as i64),
                        byte_range_end: task.byte_range_end.map(|v| v as i64),
                        modification_time_millis: task.modification_time,
                    },
                );
            } else {
                non_dv_files.push(file);
            }
        }
        if !non_dv_files.is_empty() {
            file_groups.push(non_dv_files);
            deleted_indexes_per_group.push(Vec::new());
            base_row_ids_per_group.push(None);
            default_commit_versions_per_group.push(None);
            task_metadata_per_group.push(
                comet_contrib_delta::synthetic_columns::TaskMetadata::default(),
            );
        }

        // Pick any one file to register the object store (they all share the same root).
        let one_file = scan
            .tasks
            .first()
            .map(|t| t.file_path.clone())
            .ok_or_else(|| {
                GeneralError("DeltaScan has no tasks after split-mode injection".into())
            })?;
        let url = url::Url::parse(&one_file)
            .map_err(|e| GeneralError(format!("DeltaScan invalid file URL: {e}")))?;
        let (object_store_url, _root_path) = prepare_object_store_with_configs(
            self.session_ctx().runtime_env(),
            url.to_string(),
            &object_store_options,
        )
        .map_err(|e| GeneralError(format!("prepare_object_store_with_configs: {e}")))?;

        let delta_exec = init_datasource_exec(
            Arc::clone(&required_schema),
            Some(data_schema),
            Some(partition_schema),
            object_store_url,
            file_groups,
            Some(projection_vector),
            Some(data_filters?),
            None, // default_values
            common.session_timezone.as_str(),
            common.case_sensitive,
            false, // return_null_struct_if_all_fields_missing
            self.session_ctx(),
            false, // encryption_enabled (Delta tables we natively support are unencrypted)
            common.use_field_id,
            false, // ignore_missing_field_id
            common.ignore_missing_files,
        )?;

        // Three mutually-exclusive wrap modes based on what the surrounding plan asks
        // for:
        //  - Delta synthetic columns requested (row_index and/or is_row_deleted): wrap
        //    with DeltaSyntheticColumnsExec which keeps all rows and APPENDS the
        //    columns. The outer Delta plan (typically UPDATE/DELETE/MERGE) decides
        //    what to do with the deletion flag.
        //  - DV present and no synthetics: wrap with DeltaDvFilterExec which DROPS
        //    deleted rows inline (standard read path).
        //  - Neither: pass through (avoids per-batch overhead).
        let need_synthetics = common.emit_row_index
            || common.emit_is_row_deleted
            || common.emit_row_id
            || common.emit_row_commit_version
            || !common.metadata_column_names.is_empty();

        // Column-mapping rename has to happen BEFORE synthetic emission so that the
        // synthetic exec sees logical column names in its input schema (matching what
        // its build_output_schema expects) and so that the (stripped) `required_schema`
        // we use here for the rename match isn't compared against a schema that already
        // has synthetics appended. Synthetic columns have FIXED names
        // (`__delta_internal_*`, `row_id`, `row_commit_version`) and aren't subject to
        // CM-name physical renames -- so it's correct to apply the rename to the
        // parquet output BEFORE the append.
        let delta_exec: Arc<dyn datafusion::physical_plan::ExecutionPlan> = delta_exec;
        let scan_out = delta_exec.schema();
        let needs_rename = has_column_mapping
            && required_schema.fields().len() == scan_out.fields().len()
            && required_schema
                .fields()
                .iter()
                .zip(scan_out.fields().iter())
                .any(|(req, phys)| req.name() != phys.name());
        let after_rename: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if needs_rename {
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
                ProjectionExec::try_new(projections, delta_exec)
                    .map_err(|e| GeneralError(format!("rename ProjectionExec: {e}")))?,
            )
        } else {
            delta_exec
        };

        // After CM-name rename: apply synthetic emission OR DV filter OR passthrough.
        let after_synthetics: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if need_synthetics
        {
            let row_index_alias = if common.row_index_column_alias.is_empty() {
                comet_contrib_delta::synthetic_columns::ROW_INDEX_COLUMN_NAME
            } else {
                common.row_index_column_alias.as_str()
            };
            Arc::new(
                comet_contrib_delta::synthetic_columns::DeltaSyntheticColumnsExec::new(
                    after_rename,
                    deleted_indexes_per_group,
                    base_row_ids_per_group,
                    default_commit_versions_per_group,
                    common.emit_row_index,
                    common.emit_is_row_deleted,
                    common.emit_row_id,
                    common.emit_row_commit_version,
                    row_index_alias,
                    common.metadata_column_names.clone(),
                    task_metadata_per_group,
                )
                .map_err(|e| GeneralError(format!("DeltaSyntheticColumnsExec: {e}")))?,
            )
        } else if deleted_indexes_per_group.iter().any(|v| !v.is_empty()) {
            Arc::new(
                DeltaDvFilterExec::new(after_rename, deleted_indexes_per_group)
                    .map_err(|e| GeneralError(format!("DeltaDvFilterExec: {e}")))?,
            )
        } else {
            after_rename
        };

        // If synthetic columns aren't a suffix of the user-visible required_schema,
        // `final_output_indices` is set and we project to reorder. Each entry is an
        // index into the wrapped exec's output schema (parquet columns first, then
        // appended synthetics in the canonical row_index/is_row_deleted/row_id/
        // row_commit_version order). Empty => already in the right order.
        let with_rename: Arc<dyn datafusion::physical_plan::ExecutionPlan> = if !common
            .final_output_indices
            .is_empty()
        {
            let wrapped_schema = after_synthetics.schema();
            let projections: Vec<(Arc<dyn PhysicalExpr>, String)> = common
                .final_output_indices
                .iter()
                .map(|idx| {
                    let i = *idx as usize;
                    let field = wrapped_schema.field(i);
                    let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(field.name(), i));
                    (col, field.name().clone())
                })
                .collect();
            Arc::new(
                ProjectionExec::try_new(projections, after_synthetics)
                    .map_err(|e| GeneralError(format!("final reorder ProjectionExec: {e}")))?,
            )
        } else {
            after_synthetics
        };

        Ok((
            vec![],
            vec![],
            Arc::new(SparkPlan::new(spark_plan.plan_id, with_rename, vec![])),
        ))
    }
}
