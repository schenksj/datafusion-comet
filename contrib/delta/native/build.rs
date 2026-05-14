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

//! Build script for the Delta contrib's proto.
//!
//! Two non-default behaviors:
//!   * The include path includes core's proto directory so `delta_operator.proto` can
//!     `import "operator.proto"` and `"expr.proto"` for the shared `SparkStructField`
//!     / `Expr` types.
//!   * `extern_path` tells prost-build to RESOLVE imported types against the upstream
//!     `datafusion-comet-proto` crate rather than generating duplicate Rust types here.
//!     Without this, the contrib would have its own copy of every `SparkStructField`
//!     instance and would not be type-compatible with core's expression-eval helpers.

use std::{fs, io::Result, path::Path};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=src/proto/");
    println!("cargo:rerun-if-changed=../../../native/proto/src/proto/");

    let out_dir = "src/generated";
    if !Path::new(out_dir).is_dir() {
        fs::create_dir(out_dir)?;
    }

    prost_build::Config::new()
        .out_dir(out_dir)
        // Resolve shared types to core's generated Rust path. Anything inside the
        // `spark.spark_operator` or `spark.spark_expression` proto packages becomes
        // a re-export of `datafusion_comet_proto::*` instead of a duplicate definition.
        .extern_path(
            ".spark.spark_operator",
            "::datafusion_comet_proto::spark_operator",
        )
        .extern_path(
            ".spark.spark_expression",
            "::datafusion_comet_proto::spark_expression",
        )
        .compile_protos(
            &["src/proto/delta_operator.proto"],
            // First entry is THIS contrib's proto dir; second is core's so the
            // `import` statements in delta_operator.proto resolve.
            &["src/proto", "../../../native/proto/src/proto"],
        )?;
    Ok(())
}
