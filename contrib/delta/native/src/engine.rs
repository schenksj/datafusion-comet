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

//! Construction of a delta-kernel-rs `DefaultEngine` backed by `object_store`.
//!
//! Ported from tantivy4java's `delta_reader/engine.rs` (Apache-2.0) with
//! minor changes: uses Comet's error type instead of `anyhow`, and uses the
//! renamed `object_store_kernel` (object_store 0.12) dependency that kernel
//! requires. Comet's main `object_store = "0.13"` tree is untouched.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use object_store_kernel::aws::AmazonS3Builder;
use object_store_kernel::azure::MicrosoftAzureBuilder;
use object_store_kernel::local::LocalFileSystem;
use object_store_kernel::ObjectStore;

use super::error::{DeltaError, DeltaResult};

/// Concrete engine type returned by [`get_or_create_engine`].
pub type DeltaEngine = DefaultEngine<TokioBackgroundExecutor>;

/// Storage credentials used to construct kernel's engine.
///
/// Mirrors tantivy4java's `DeltaStorageConfig`. Field-per-knob rather than a
/// generic map so we can validate at the boundary; the Scala side will
/// populate this from a Spark options map.
#[derive(Debug, Clone, Default, Hash, PartialEq, Eq)]
pub struct DeltaStorageConfig {
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub aws_force_path_style: bool,

    pub azure_account_name: Option<String>,
    pub azure_access_key: Option<String>,
    pub azure_bearer_token: Option<String>,
}

/// Build an `ObjectStore` for the given URL and credentials.
///
/// Supports `s3://` / `s3a://`, `az://` / `azure://` / `abfs://` / `abfss://`,
/// and `file://`. Any other scheme is rejected with
/// [`DeltaError::UnsupportedScheme`].
pub fn create_object_store(
    url: &Url,
    config: &DeltaStorageConfig,
) -> DeltaResult<Arc<dyn ObjectStore>> {
    let scheme = url.scheme();

    let store: Arc<dyn ObjectStore> = match scheme {
        "s3" | "s3a" => {
            let bucket = url.host_str().ok_or_else(|| DeltaError::MissingBucket {
                url: url.to_string(),
            })?;
            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            if let Some(ref key) = config.aws_access_key {
                builder = builder.with_access_key_id(key);
            }
            if let Some(ref secret) = config.aws_secret_key {
                builder = builder.with_secret_access_key(secret);
            }
            if let Some(ref token) = config.aws_session_token {
                builder = builder.with_token(token);
            }
            if let Some(ref region) = config.aws_region {
                builder = builder.with_region(region);
            }
            if let Some(ref endpoint) = config.aws_endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if config.aws_force_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            // Allow HTTP endpoints (MinIO, LocalStack, custom S3-compat)
            if config
                .aws_endpoint
                .as_ref()
                .is_some_and(|e| e.starts_with("http://"))
            {
                builder = builder.with_allow_http(true);
            }

            Arc::new(builder.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let container = url.host_str().ok_or_else(|| DeltaError::MissingBucket {
                url: url.to_string(),
            })?;
            let mut builder = MicrosoftAzureBuilder::new().with_container_name(container);

            if let Some(ref account) = config.azure_account_name {
                builder = builder.with_account(account);
            }
            if let Some(ref key) = config.azure_access_key {
                builder = builder.with_access_key(key);
            }
            if let Some(ref token) = config.azure_bearer_token {
                builder = builder.with_bearer_token_authorization(token);
            }

            Arc::new(builder.build()?)
        }
        "file" | "" => Arc::new(LocalFileSystem::new()),
        other => {
            return Err(DeltaError::UnsupportedScheme {
                scheme: other.to_string(),
                url: url.to_string(),
            });
        }
    };

    Ok(store)
}

/// Process-wide cache of constructed engines, keyed by (scheme, authority, config).
///
/// Each `DefaultEngine` owns a `TokioBackgroundExecutor` which spawns one std::thread
/// running a current_thread tokio runtime; the runtime's blocking pool (used by
/// kernel for parquet/object_store IO) holds spawned threads for `thread_keep_alive`
/// (~10s) after each spawn_blocking call. Constructing a fresh engine per JNI
/// `planDeltaScan` call therefore accumulates OS threads during regression runs that
/// hit kernel hundreds of times per minute, eventually tripping the per-process
/// thread cap (e.g. `pthread_create EAGAIN` aborts on macOS where `ulimit -u`
/// defaults to ~1300). Sharing one engine per (scheme, authority, config) bounds the
/// thread count by table-storage diversity instead of by request count.
///
/// `Arc<DeltaEngine>` is handed out so callers don't hold the mutex while using the
/// engine. We never evict — entries are cheap (one Arc per distinct storage target),
/// and dropping the cache at JVM teardown is acceptable.
type EngineKey = (String, String, DeltaStorageConfig);
fn engine_cache() -> &'static Mutex<HashMap<EngineKey, Arc<DeltaEngine>>> {
    static CACHE: OnceLock<Mutex<HashMap<EngineKey, Arc<DeltaEngine>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn engine_key(url: &Url, config: &DeltaStorageConfig) -> EngineKey {
    let scheme = url.scheme().to_string();
    // host+port form the storage target (e.g. S3 bucket, ABFS account); for file://
    // the authority is empty which collapses every local table to a single entry.
    let authority = match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => String::new(),
    };
    (scheme, authority, config.clone())
}

// Suppress dead_code: the standalone constructor stays useful for tests that want
// to exercise a fresh engine without polluting the cache.
#[allow(dead_code)]
pub fn create_engine(table_url: &Url, config: &DeltaStorageConfig) -> DeltaResult<DeltaEngine> {
    let store = create_object_store(table_url, config)?;
    Ok(DefaultEngine::new(store))
}

/// Return a shared `DeltaEngine` for the given URL+config, building one on first use.
pub fn get_or_create_engine(
    table_url: &Url,
    config: &DeltaStorageConfig,
) -> DeltaResult<Arc<DeltaEngine>> {
    let key = engine_key(table_url, config);
    // Mutex is held only across the (cheap) HashMap lookup and, on miss, the engine
    // construction. Multi-threaded JNI callers serialize here on first miss per key
    // but proceed lock-free on subsequent hits via the returned Arc clone.
    let mut cache = engine_cache().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(existing) = cache.get(&key) {
        return Ok(Arc::clone(existing));
    }
    let store = create_object_store(table_url, config)?;
    let engine = Arc::new(DefaultEngine::new(store));
    cache.insert(key, Arc::clone(&engine));
    Ok(engine)
}

