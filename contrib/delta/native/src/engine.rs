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
//! `object_store` 0.13 dependency that kernel
//! requires. Comet's main `object_store = "0.13"` tree is untouched.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use url::Url;

use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::DefaultEngine;
use object_store::aws::AmazonS3Builder;
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;

use super::error::{DeltaError, DeltaResult};

/// Concrete engine type returned by [`get_or_create_engine`].
pub type DeltaEngine = DefaultEngine<TokioBackgroundExecutor>;

/// Storage credentials used to construct kernel's engine, bridged from the Hadoop
/// configuration the JVM ships (see `jni::delta_storage_config_from_map`).
///
/// S3 mirrors core Comet's `objectstore::s3` static-key subset (`fs.s3a.*` incl. per-bucket
/// overrides); the Hadoop credential-provider classes core additionally emulates are a
/// documented residual (08-known-limitations.md A2e). Azure mirrors core's
/// `objectstore::azure::translate_hadoop_configs` mapping (account key, SAS, OAuth client
/// credentials, MSI, workload identity), resolved account/container-scoped on the JNI side.
/// GCS bridges the Hadoop service-account keyfile; anything not bridged falls back to the
/// builder's ambient resolution (env / ADC / instance metadata).
#[derive(Clone, Default, Hash, PartialEq, Eq)]
pub struct DeltaStorageConfig {
    pub aws_access_key: Option<String>,
    pub aws_secret_key: Option<String>,
    pub aws_session_token: Option<String>,
    pub aws_region: Option<String>,
    pub aws_endpoint: Option<String>,
    pub aws_force_path_style: bool,
    /// `fs.s3a.requester.pays.enabled` -- requester-pays buckets 403 without it.
    pub aws_requester_pays: bool,
    // Azure (abfs/abfss/wasb/wasbs/az). Bridged from `fs.azure.*` so the native reader uses the
    // SAME credentials Spark would, instead of falling back to ambient `AZURE_*` env on executors.
    pub azure_account_name: Option<String>,
    pub azure_account_key: Option<String>,
    pub azure_sas_token: Option<String>,
    // Azure OAuth2 client credentials / MSI / workload identity -- the same
    // `fs.azure.account.oauth2.*` surface core's `objectstore::azure` bridges.
    pub azure_client_id: Option<String>,
    pub azure_client_secret: Option<String>,
    pub azure_tenant_id: Option<String>,
    pub azure_msi_endpoint: Option<String>,
    pub azure_authority_host: Option<String>,
    pub azure_federated_token_file: Option<String>,
    // GCS (gs/gcs). Bridged from `fs.gs.*`.
    pub gcs_service_account_path: Option<String>,
    pub gcs_service_account_key: Option<String>,
}

impl DeltaStorageConfig {
    /// Typed `object_store` config pairs for the Azure store, derived from the bridged Hadoop
    /// creds. Empty when nothing was bridged -- the builder then runs on `from_env()` alone
    /// (ambient `AZURE_*` / workload identity), matching core's `objectstore::azure` fallback.
    /// (`object_store` reads the account/container from the abfss/wasb URL authority; the
    /// account name is supplied here when known, e.g. for the `az://` scheme.)
    pub fn azure_object_store_options(&self) -> Vec<(AzureConfigKey, String)> {
        let mut o = Vec::new();
        let mut push = |k: AzureConfigKey, v: &Option<String>| {
            if let Some(v) = v {
                o.push((k, v.clone()));
            }
        };
        push(AzureConfigKey::AccountName, &self.azure_account_name);
        push(AzureConfigKey::AccessKey, &self.azure_account_key);
        push(AzureConfigKey::SasKey, &self.azure_sas_token);
        push(AzureConfigKey::ClientId, &self.azure_client_id);
        push(AzureConfigKey::ClientSecret, &self.azure_client_secret);
        push(AzureConfigKey::AuthorityId, &self.azure_tenant_id);
        push(AzureConfigKey::MsiEndpoint, &self.azure_msi_endpoint);
        push(AzureConfigKey::AuthorityHost, &self.azure_authority_host);
        push(
            AzureConfigKey::FederatedTokenFile,
            &self.azure_federated_token_file,
        );
        o
    }

    /// `object_store`-style config key/value pairs for the GCS store. Empty = fall back to ambient.
    pub fn gcs_object_store_options(&self) -> Vec<(&'static str, String)> {
        let mut o = Vec::new();
        if let Some(v) = &self.gcs_service_account_path {
            o.push(("google_service_account", v.clone()));
        }
        if let Some(v) = &self.gcs_service_account_key {
            o.push(("google_service_account_key", v.clone()));
        }
        o
    }
}

// Hand-written `Debug` (NOT derived) so a stray `{:?}` -- a future debug log, an error wrapper --
// can never leak credential material. Secret fields render as `<set>` / `None`; non-secret fields
// (region, endpoint, account name) stay visible for diagnosability. Defense-in-depth: nothing on
// the read path Debug-prints this today.
impl std::fmt::Debug for DeltaStorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn redact(o: &Option<String>) -> &'static str {
            if o.is_some() {
                "<set>"
            } else {
                "None"
            }
        }
        f.debug_struct("DeltaStorageConfig")
            .field("aws_access_key", &redact(&self.aws_access_key))
            .field("aws_secret_key", &redact(&self.aws_secret_key))
            .field("aws_session_token", &redact(&self.aws_session_token))
            .field("aws_region", &self.aws_region)
            .field("aws_endpoint", &self.aws_endpoint)
            .field("aws_force_path_style", &self.aws_force_path_style)
            .field("aws_requester_pays", &self.aws_requester_pays)
            .field("azure_account_name", &self.azure_account_name)
            .field("azure_account_key", &redact(&self.azure_account_key))
            .field("azure_sas_token", &redact(&self.azure_sas_token))
            .field("azure_client_id", &self.azure_client_id)
            .field("azure_client_secret", &redact(&self.azure_client_secret))
            .field("azure_tenant_id", &self.azure_tenant_id)
            .field("azure_msi_endpoint", &self.azure_msi_endpoint)
            .field("azure_authority_host", &self.azure_authority_host)
            .field(
                "azure_federated_token_file",
                &self.azure_federated_token_file,
            )
            .field("gcs_service_account_path", &self.gcs_service_account_path)
            .field(
                "gcs_service_account_key",
                &redact(&self.gcs_service_account_key),
            )
            .finish()
    }
}

/// Build an `ObjectStore` for the given URL and credentials.
///
/// `s3://` / `s3a://` are built from the bridged `fs.s3a.*` config; `file://` is local.
/// Azure (`az` / `azure` / `abfs` / `abfss` / `wasb` / `wasbs`) starts from
/// `MicrosoftAzureBuilder::from_env()` (ambient `AZURE_*` / AKS workload identity, exactly
/// like core's `objectstore::azure::create_store`) and layers the bridged `fs.azure.*`
/// credentials on top. GCS (`gs` / `gcs`) goes through `object_store::parse_url[_opts]`
/// (which resolves credentials lazily from the ADC well-known path or the instance
/// metadata server -- parity with core, which has no GCS translation either; `gcs://`
/// is rewritten to `gs://` first because `ObjectStoreScheme::parse` only knows `gs`).
/// Any other scheme is rejected with [`DeltaError::UnsupportedScheme`].
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
            // `allow_http` unconditionally, mirroring core's `objectstore::s3::create_store`
            // (MinIO / LocalStack endpoints are `http://`; against real AWS the endpoint is
            // https anyway).
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_allow_http(true);

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
            if config.aws_requester_pays {
                builder =
                    builder.with_config(object_store::aws::AmazonS3ConfigKey::RequestPayer, "true");
            }
            // With neither an endpoint nor a region, object_store defaults to us-east-1 and
            // every request against a bucket in any other region fails with 301
            // PermanentRedirect (object_store does not follow region redirects). Mirror core:
            // resolve the bucket's real region via a cached HeadBucket probe.
            if config.aws_endpoint.is_none() && config.aws_region.is_none() {
                let region = resolve_bucket_region_blocking(bucket)?;
                builder = builder.with_region(region);
            }

            Arc::new(builder.build()?)
        }
        "az" | "azure" | "abfs" | "abfss" | "wasb" | "wasbs" => build_azure_store(url, config)?,
        "gs" | "gcs" => {
            // Build the GCS store from the service account Spark bridged from `fs.gs.*`; with no
            // bridged creds, fall back to `parse_url` (lazy ADC / instance-metadata resolution,
            // parity with core). `ObjectStoreScheme::parse` recognises only `gs`, so rewrite the
            // `gcs` alias first -- without this the arm can never build a store.
            let mut gs_url = url.clone();
            if scheme == "gcs" {
                gs_url.set_scheme("gs").map_err(|_| {
                    DeltaError::Internal(format!("cannot rewrite gcs:// scheme for {url}"))
                })?;
            }
            let opts = config.gcs_object_store_options();
            let (store, _path) = if opts.is_empty() {
                object_store::parse_url(&gs_url)?
            } else {
                object_store::parse_url_opts(&gs_url, opts)?
            };
            Arc::from(store)
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

/// Process-wide cache of resolved S3 bucket regions (a bucket's region is fixed at
/// creation, so no invalidation). Port of core's `objectstore::s3::region_cache`.
fn region_cache() -> &'static std::sync::RwLock<HashMap<String, String>> {
    static CACHE: OnceLock<std::sync::RwLock<HashMap<String, String>>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::RwLock::new(HashMap::new()))
}

/// Resolve an S3 bucket's region via the HeadBucket API (`x-amz-bucket-region` response
/// header), cached per bucket. Port of core's `objectstore::s3::resolve_bucket_region`
/// (itself adapted from the object_store crate as a workaround for
/// arrow-rs-object-store#479).
///
/// Runs the probe on a dedicated OS thread with a one-shot current-thread runtime rather
/// than `block_on` in place: engine construction can happen on a tokio worker (the
/// executor-side read path), where any in-context `block_on` panics. The thread cost is
/// paid once per bucket; subsequent lookups hit the cache.
fn resolve_bucket_region_blocking(bucket: &str) -> DeltaResult<String> {
    if let Some(region) = region_cache()
        .read()
        .ok()
        .and_then(|c| c.get(bucket).cloned())
    {
        return Ok(region);
    }
    let bucket_owned = bucket.to_string();
    let resolved = std::thread::spawn(move || -> Result<String, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("tokio runtime for region probe: {e}"))?;
        rt.block_on(async {
            let endpoint = format!("https://{bucket_owned}.s3.amazonaws.com");
            let response = reqwest::Client::new()
                .head(&endpoint)
                .send()
                .await
                .map_err(|e| format!("HeadBucket {endpoint}: {e}"))?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(format!("Bucket not found: {bucket_owned}"));
            }
            response
                .headers()
                .get("x-amz-bucket-region")
                .ok_or_else(|| format!("Missing region for bucket: {bucket_owned}"))?
                .to_str()
                .map(str::to_string)
                .map_err(|e| format!("x-amz-bucket-region for {bucket_owned}: {e}"))
        })
    })
    .join()
    .map_err(|_| DeltaError::Internal("S3 region resolution thread panicked".to_string()))?
    .map_err(|e| DeltaError::Internal(format!("failed to resolve S3 bucket region: {e}")))?;
    if let Ok(mut cache) = region_cache().write() {
        cache.insert(bucket.to_string(), resolved.clone());
    }
    Ok(resolved)
}

/// Build the Azure store: start from the ambient environment, then layer the bridged Hadoop
/// credentials, with the URL supplying account/container.
///
/// Mirrors core's `objectstore::azure::create_store`: `from_env()` first so AKS Workload
/// Identity (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_FEDERATED_TOKEN_FILE`) and
/// explicit `AZURE_STORAGE_*` variables are honoured with no further configuration, then the
/// translated Hadoop CREDENTIAL configs override. Note the account/container are the other
/// way round: `with_url` is applied inside `build()` AFTER all `with_config` calls, so the
/// URL-derived account/container override a bridged `AccountName` -- which is correct: the
/// table URL, not the config map, names the store. (Credential keys are never URL-derived,
/// so credentials always layer on top of env as described.) (`object_store::parse_url` would use
/// `MicrosoftAzureBuilder::new()`, which reads NO environment at all -- a store built that way
/// has no credentials whatsoever, so it is not a usable fallback.)
///
/// `wasb[s]://container@account.blob.core.windows.net/...` is handled by extracting the
/// account/container manually: neither `parse_url` nor the builder's `with_url` recognises
/// the wasb scheme.
fn build_azure_store(url: &Url, config: &DeltaStorageConfig) -> DeltaResult<Arc<dyn ObjectStore>> {
    let mut builder = MicrosoftAzureBuilder::from_env();
    match url.scheme() {
        "wasb" | "wasbs" => {
            let host = url.host_str().ok_or_else(|| DeltaError::MissingBucket {
                url: url.to_string(),
            })?;
            // wasb authority is `container@account.blob.core.windows.net`; the account is the
            // first host label, the container is the URL user-info.
            let account = host.split('.').next().unwrap_or(host);
            builder = builder.with_account(account);
            if !url.username().is_empty() {
                builder = builder.with_container_name(url.username());
            }
        }
        _ => builder = builder.with_url(url.to_string()),
    }
    for (key, value) in config.azure_object_store_options() {
        builder = builder.with_config(key, value);
    }
    Ok(Arc::new(builder.build()?))
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
/// **LRU-bounded**: the cache holds at most `MAX_CACHE_ENTRIES` engines. When full,
/// the least-recently-used entry is evicted and its `Arc<DeltaEngine>` drops --
/// `DefaultEngine`'s `TokioBackgroundExecutor` joins its OS thread on drop, so the
/// thread count stabilizes even when long-running drivers rotate credentials (e.g.
/// hourly STS / IRSA rotations on production). Without this bound, every rotation
/// produced a new cache entry (because `DeltaStorageConfig` is part of the key and
/// `aws_session_token` rotates) and leaked one tokio thread per rotation -- a
/// production-grade memory + thread leak over days.
///
/// `Arc<DeltaEngine>` is handed out so callers don't hold the mutex while using the
/// engine; concurrent in-flight scans against an evicted engine keep it alive until
/// they complete.
const MAX_CACHE_ENTRIES: usize = 32;

type EngineKey = (String, String, DeltaStorageConfig);

/// Cache state: maps key to (engine, monotonic last-use counter). The counter is the
/// LRU recency stamp; we bump it on every hit AND on every insert, and evict the
/// entry with the smallest counter when full.
struct EngineCacheState {
    map: HashMap<EngineKey, (Arc<DeltaEngine>, u64)>,
    counter: u64,
}

fn engine_cache() -> &'static Mutex<EngineCacheState> {
    static CACHE: OnceLock<Mutex<EngineCacheState>> = OnceLock::new();
    CACHE.get_or_init(|| {
        Mutex::new(EngineCacheState {
            map: HashMap::new(),
            counter: 0,
        })
    })
}

fn engine_key(url: &Url, config: &DeltaStorageConfig) -> EngineKey {
    let scheme = url.scheme().to_string();
    // userinfo+host+port form the storage target (S3 bucket; ABFS container@account -- the
    // container is part of the store's identity, so two containers on one account must NOT
    // share a cached engine); for file:// the authority is empty, which collapses every
    // local table to a single entry.
    let mut authority = match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{h}:{p}"),
        (Some(h), None) => h.to_string(),
        _ => String::new(),
    };
    if !url.username().is_empty() {
        authority = format!("{}@{authority}", url.username());
    }
    (scheme, authority, config.clone())
}

/// Return a shared `DeltaEngine` for the given URL+config, building one on first use.
///
/// LRU-bounded: when the cache is full, the least-recently-used entry is evicted.
/// In-flight users of an evicted engine keep it alive via their `Arc` clone until
/// they're done; only THEN does the evicted entry's `TokioBackgroundExecutor` join
/// its OS thread.
pub fn get_or_create_engine(
    table_url: &Url,
    config: &DeltaStorageConfig,
) -> DeltaResult<Arc<DeltaEngine>> {
    let key = engine_key(table_url, config);
    // Mutex is held only across the (cheap) HashMap lookup and, on miss, the engine
    // construction. Multi-threaded JNI callers serialize here on first miss per key
    // but proceed lock-free on subsequent hits via the returned Arc clone.
    let mut cache = engine_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.counter = cache.counter.wrapping_add(1);
    let stamp = cache.counter;
    if let Some(entry) = cache.map.get_mut(&key) {
        // Hit: bump the LRU stamp and return the existing Arc.
        entry.1 = stamp;
        return Ok(Arc::clone(&entry.0));
    }
    // Miss: build a fresh engine. If the cache is at capacity, evict the LRU entry
    // first so the bound is respected.
    if cache.map.len() >= MAX_CACHE_ENTRIES {
        if let Some(victim_key) = cache
            .map
            .iter()
            .min_by_key(|(_, (_, s))| *s)
            .map(|(k, _)| k.clone())
        {
            cache.map.remove(&victim_key);
        }
    }
    let store = create_object_store(table_url, config)?;
    let engine = Arc::new(DefaultEngine::builder(store).build());
    cache.map.insert(key, (Arc::clone(&engine), stamp));
    Ok(engine)
}

/// Test-only: clear the cache so tests don't see entries from prior tests.
#[cfg(test)]
pub(crate) fn _clear_cache_for_tests() {
    let mut cache = engine_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.map.clear();
    cache.counter = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn empty_config() -> DeltaStorageConfig {
        DeltaStorageConfig::default()
    }

    #[test]
    fn debug_redacts_credential_material() {
        // A stray `{:?}` on the storage config (e.g. a future debug log) must NOT leak the
        // secret key / session token / access key into logs. Non-secret fields stay visible
        // for diagnosability.
        let c = DeltaStorageConfig {
            aws_access_key: Some("AKIDEXAMPLE".to_string()),
            aws_secret_key: Some("SUPERSECRETKEYVALUE".to_string()),
            aws_session_token: Some("SESSIONTOKENVALUE".to_string()),
            aws_region: Some("us-west-2".to_string()),
            aws_endpoint: Some("https://s3.example".to_string()),
            aws_force_path_style: true,
            ..Default::default()
        };
        let s = format!("{c:?}");
        assert!(!s.contains("SUPERSECRETKEYVALUE"), "secret key leaked: {s}");
        assert!(
            !s.contains("SESSIONTOKENVALUE"),
            "session token leaked: {s}"
        );
        assert!(!s.contains("AKIDEXAMPLE"), "access key leaked: {s}");
        // Non-secret fields remain visible.
        assert!(s.contains("us-west-2"), "region should be visible: {s}");
        assert!(s.contains("s3.example"), "endpoint should be visible: {s}");
    }

    #[test]
    fn debug_redacts_azure_and_gcs_credential_material() {
        // Same guard as the AWS variant, for the Azure/GCS secret fields: account key,
        // SAS, OAuth client secret, and the GCS inline key must never survive a `{:?}`.
        // Identifiers (account/client/tenant ids, endpoints, file PATHS) stay visible.
        let c = DeltaStorageConfig {
            azure_account_name: Some("myacct".to_string()),
            azure_account_key: Some("AZKEYSECRETVALUE".to_string()),
            azure_sas_token: Some("SASSECRETVALUE".to_string()),
            azure_client_id: Some("client-id-visible".to_string()),
            azure_client_secret: Some("CLIENTSECRETVALUE".to_string()),
            azure_tenant_id: Some("tenant-visible".to_string()),
            azure_msi_endpoint: Some("http://169.254.169.254/msi".to_string()),
            azure_authority_host: Some("https://login.example".to_string()),
            azure_federated_token_file: Some("/var/run/secrets/token".to_string()),
            gcs_service_account_path: Some("/etc/gcs/key.json".to_string()),
            gcs_service_account_key: Some("GCSKEYSECRETVALUE".to_string()),
            ..Default::default()
        };
        let s = format!("{c:?}");
        assert!(!s.contains("AZKEYSECRETVALUE"), "azure key leaked: {s}");
        assert!(!s.contains("SASSECRETVALUE"), "azure SAS leaked: {s}");
        assert!(
            !s.contains("CLIENTSECRETVALUE"),
            "azure client secret leaked: {s}"
        );
        assert!(!s.contains("GCSKEYSECRETVALUE"), "gcs key leaked: {s}");
        // Non-secret identifiers remain visible for diagnosability.
        for visible in [
            "myacct",
            "client-id-visible",
            "tenant-visible",
            "169.254.169.254",
            "login.example",
            "/var/run/secrets/token",
            "/etc/gcs/key.json",
        ] {
            assert!(s.contains(visible), "{visible} should be visible: {s}");
        }
    }

    #[test]
    fn create_object_store_local_file() {
        let store = create_object_store(&url("file:///tmp/x"), &empty_config()).unwrap();
        // Just verify Arc construction succeeded; LocalFileSystem doesn't expose
        // anything we can usefully assert on without doing IO.
        assert!(format!("{store:?}").contains("LocalFileSystem"));
    }

    #[test]
    fn create_object_store_empty_scheme_is_local() {
        // The "file" | "" arm maps the empty-scheme case (URL like `relative/path`
        // wouldn't actually parse, but the arm exists for code paths that hand us
        // a Url with an empty scheme).
        let mut u = url("file:///x");
        u.set_scheme("").ok(); // best-effort; if it fails, the file:// arm still hits
        let store = create_object_store(&u, &empty_config()).unwrap();
        assert!(format!("{store:?}").contains("LocalFileSystem"));
    }

    #[test]
    fn create_object_store_s3_requires_bucket() {
        // `s3://` with empty host is rejected as MissingBucket.
        // url::Url::parse("s3:///x") gives host=None.
        let bad = url("s3:///just-a-path");
        let err = create_object_store(&bad, &empty_config()).unwrap_err();
        match err {
            DeltaError::MissingBucket { .. } => {}
            other => panic!("expected MissingBucket, got {other:?}"),
        }
    }

    #[test]
    fn create_object_store_s3_builds_with_full_creds() {
        let cfg = DeltaStorageConfig {
            aws_access_key: Some("AKIA…".into()),
            aws_secret_key: Some("secret".into()),
            aws_session_token: Some("token".into()),
            aws_region: Some("us-west-2".into()),
            aws_endpoint: Some("https://s3.example.com".into()),
            aws_force_path_style: true,
            ..Default::default()
        };
        let store = create_object_store(&url("s3://my-bucket/path"), &cfg).unwrap();
        assert!(format!("{store:?}").contains("AmazonS3") || format!("{store:?}").contains("S3"));
    }

    #[test]
    fn create_object_store_s3_http_endpoint_allows_http() {
        let cfg = DeltaStorageConfig {
            aws_access_key: Some("k".into()),
            aws_secret_key: Some("s".into()),
            aws_endpoint: Some("http://localhost:9000".into()),
            aws_force_path_style: true,
            ..Default::default()
        };
        // MinIO-style: endpoint starts with http:// → builder enables allow_http.
        // We can't introspect the builder's flag, but ensuring construction
        // succeeds covers the branch.
        create_object_store(&url("s3://minio-bucket"), &cfg).unwrap();
    }

    #[test]
    fn create_object_store_azure_ambient_fallback() {
        // With no bridged creds, the Azure store is still built -- from
        // `MicrosoftAzureBuilder::from_env()` + the URL, the same ambient path core's
        // `objectstore::azure::create_store` uses (workload identity / AZURE_* env
        // resolve lazily, so construction succeeds with no config).
        let u = url("abfss://container@myacct.dfs.core.windows.net/path");
        create_object_store(&u, &empty_config()).expect("azure store builds from env + url");
    }

    #[test]
    fn create_object_store_wasb_builds() {
        // wasb[s] is NOT recognised by object_store's parse_url / with_url; the account +
        // container are extracted manually from the authority. Regression guard: this arm
        // used to route through parse_url and failed at runtime for every wasb table.
        let u = url("wasbs://container@myacct.blob.core.windows.net/path");
        let store = create_object_store(&u, &empty_config()).expect("wasb store builds");
        let dbg = format!("{store:?}").to_lowercase();
        assert!(
            dbg.contains("azure") || dbg.contains("microsoft"),
            "got: {dbg}"
        );
    }

    #[test]
    fn create_object_store_gcs_alias_scheme_builds() {
        // `gcs://` must be rewritten to `gs://` before parse_url --
        // ObjectStoreScheme::parse only recognises `gs`, so without the rewrite this
        // arm could never build a store.
        create_object_store(&url("gcs://my-bucket/path"), &empty_config())
            .expect("gcs:// alias builds via scheme rewrite");
    }

    #[test]
    fn create_object_store_s3_requester_pays_builds() {
        let cfg = DeltaStorageConfig {
            aws_access_key: Some("k".into()),
            aws_secret_key: Some("s".into()),
            aws_region: Some("us-east-1".into()),
            aws_requester_pays: true,
            ..Default::default()
        };
        create_object_store(&url("s3://rp-bucket/p"), &cfg).expect("requester-pays store builds");
    }

    #[test]
    fn create_object_store_gcs_via_parse_url() {
        // GCS is likewise built through object_store::parse_url; ADC / GOOGLE_* resolve
        // lazily, so construction succeeds with no explicit config (was UnsupportedScheme
        // before this gained parity with core).
        create_object_store(&url("gs://my-bucket/path"), &empty_config())
            .expect("gcs store builds via parse_url");
    }

    #[test]
    fn create_object_store_azure_with_explicit_key() {
        // With bridged fs.azure.* creds, the builder applies the explicit account key on
        // top of the env baseline. Construction is lazy, so no network.
        let cfg = DeltaStorageConfig {
            azure_account_name: Some("myacct".to_string()),
            azure_account_key: Some("dGVzdGtleQ==".to_string()),
            ..Default::default()
        };
        let u = url("abfss://container@myacct.dfs.core.windows.net/path");
        let store = create_object_store(&u, &cfg).expect("azure store builds with explicit key");
        let dbg = format!("{store:?}").to_lowercase();
        assert!(
            dbg.contains("azure") || dbg.contains("microsoft"),
            "got: {dbg}"
        );
    }

    // Note: an engine-level GCS explicit-creds test is omitted because object_store's
    // `google_service_account` reads + parses the keyfile at build time, so it can't be
    // exercised with a fake path. The bridging is covered by jni::tests::gcs_creds_bridged_from_hadoop
    // (extraction -> gcs_object_store_options) and exercised end-to-end against real GCS.

    #[test]
    fn create_object_store_azure_oauth_client_creds_builds() {
        // OAuth2 client-credential fields reach the builder without erroring construction
        // (token exchange is lazy, so no network here).
        let cfg = DeltaStorageConfig {
            azure_client_id: Some("client".to_string()),
            azure_client_secret: Some("secret".to_string()),
            azure_tenant_id: Some("tenant".to_string()),
            ..Default::default()
        };
        let u = url("abfss://container@myacct.dfs.core.windows.net/path");
        create_object_store(&u, &cfg).expect("azure store builds with oauth client creds");
    }

    #[test]
    fn create_object_store_wasb_requires_authority() {
        // A wasb URL with no authority has no account to bind -- must error, not build a
        // store pointed at nothing.
        let bad = url("wasbs:///just/a/path");
        let err = create_object_store(&bad, &empty_config()).unwrap_err();
        match err {
            DeltaError::MissingBucket { .. } => {}
            other => panic!("expected MissingBucket, got {other:?}"),
        }
    }

    #[test]
    fn create_object_store_unsupported_scheme() {
        // A scheme outside the S3 / Azure / GCS / file arms is rejected before reaching
        // object_store. `ftp` is never a Delta storage backend.
        let err = create_object_store(&url("ftp://host/p"), &empty_config()).unwrap_err();
        match err {
            DeltaError::UnsupportedScheme { scheme, .. } => assert_eq!(scheme, "ftp"),
            other => panic!("expected UnsupportedScheme, got {other:?}"),
        }
    }

    #[test]
    fn engine_key_collapses_local_paths() {
        let cfg = empty_config();
        let a = engine_key(&url("file:///tmp/a"), &cfg);
        let b = engine_key(&url("file:///tmp/b/c/d"), &cfg);
        assert_eq!(a, b, "all local file:// URLs share one engine entry");
    }

    #[test]
    fn engine_key_distinguishes_s3_buckets() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://bucket-a/path"), &cfg);
        let b = engine_key(&url("s3://bucket-b/path"), &cfg);
        assert_ne!(a, b);
    }

    #[test]
    fn engine_key_distinguishes_azure_containers() {
        // The Azure store is CONTAINER-bound (`container@account` authority), so two
        // containers on one account must not share a cached engine.
        let cfg = empty_config();
        let a = engine_key(&url("abfss://data@acct.dfs.core.windows.net/t1"), &cfg);
        let b = engine_key(&url("abfss://logs@acct.dfs.core.windows.net/t2"), &cfg);
        assert_ne!(a, b, "different containers must not share a cached engine");
    }

    #[test]
    fn engine_key_includes_port() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://host:9000/p"), &cfg);
        let b = engine_key(&url("s3://host:9001/p"), &cfg);
        let c = engine_key(&url("s3://host/p"), &cfg);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn engine_key_distinguishes_credentials() {
        let cfg_a = DeltaStorageConfig {
            aws_access_key: Some("AKIA1".into()),
            ..Default::default()
        };
        let cfg_b = DeltaStorageConfig {
            aws_access_key: Some("AKIA2".into()),
            ..Default::default()
        };
        let a = engine_key(&url("s3://bucket/p"), &cfg_a);
        let b = engine_key(&url("s3://bucket/p"), &cfg_b);
        assert_ne!(a, b, "different credentials must NOT share a cached engine");
    }

    #[test]
    fn engine_key_path_does_not_affect_key() {
        let cfg = empty_config();
        let a = engine_key(&url("s3://bucket/path/a"), &cfg);
        let b = engine_key(&url("s3://bucket/path/b/c"), &cfg);
        assert_eq!(a, b, "paths within the same bucket share one engine");
    }

    #[test]
    fn get_or_create_engine_returns_same_arc_on_hit() {
        let cfg = empty_config();
        let u = url("file:///tmp/cache-test");
        let e1 = get_or_create_engine(&u, &cfg).unwrap();
        let e2 = get_or_create_engine(&u, &cfg).unwrap();
        assert!(
            Arc::ptr_eq(&e1, &e2),
            "second call must return the cached Arc, not a fresh engine"
        );
    }

    #[test]
    fn get_or_create_engine_distinct_keys_yield_distinct_engines() {
        let cfg = empty_config();
        let e_file = get_or_create_engine(&url("file:///tmp/distinct-a"), &cfg).unwrap();
        // s3:// would actually try to set up an AWS client; use a different file path
        // which collapses to the same key per `engine_key_collapses_local_paths`. So we
        // exercise a distinct-key case via a different cred config.
        let cfg_b = DeltaStorageConfig {
            aws_access_key: Some("dummy".into()),
            ..Default::default()
        };
        let e_creds = get_or_create_engine(&url("file:///tmp/distinct-a"), &cfg_b).unwrap();
        assert!(
            !Arc::ptr_eq(&e_file, &e_creds),
            "differing config keys must yield distinct engines"
        );
    }

    #[test]
    fn get_or_create_engine_evicts_lru_when_full() {
        _clear_cache_for_tests();
        // Build MAX_CACHE_ENTRIES distinct engines, each with a distinct credential
        // tuple so they all key uniquely against the same local URL.
        let urls_and_engines: Vec<(String, Arc<DeltaEngine>)> = (0..MAX_CACHE_ENTRIES)
            .map(|i| {
                let cfg = DeltaStorageConfig {
                    aws_access_key: Some(format!("key-{i}")),
                    ..Default::default()
                };
                let url_s = format!("file:///tmp/lru-{i}");
                let eng = get_or_create_engine(&url(&url_s), &cfg).unwrap();
                (format!("key-{i}"), eng)
            })
            .collect();

        // Cache is now exactly full.
        assert_eq!(
            engine_cache().lock().unwrap().map.len(),
            MAX_CACHE_ENTRIES,
            "cache should be at capacity after filling it"
        );

        // Touch entry 1 so it becomes most-recently-used. Entry 0 is now LRU.
        let cfg_1 = DeltaStorageConfig {
            aws_access_key: Some(urls_and_engines[1].0.clone()),
            ..Default::default()
        };
        let _hit_1 = get_or_create_engine(&url("file:///tmp/lru-1"), &cfg_1).unwrap();

        // Insert one more -- entry 0 (LRU after the touch) should be evicted.
        let cfg_new = DeltaStorageConfig {
            aws_access_key: Some("key-new".into()),
            ..Default::default()
        };
        let _new = get_or_create_engine(&url("file:///tmp/lru-new"), &cfg_new).unwrap();

        assert_eq!(
            engine_cache().lock().unwrap().map.len(),
            MAX_CACHE_ENTRIES,
            "cache size should stay at capacity after eviction"
        );

        // Hitting key-0 again should construct a fresh engine (not return the original Arc).
        let cfg_0 = DeltaStorageConfig {
            aws_access_key: Some(urls_and_engines[0].0.clone()),
            ..Default::default()
        };
        let fresh_0 = get_or_create_engine(&url("file:///tmp/lru-0"), &cfg_0).unwrap();
        assert!(
            !Arc::ptr_eq(&urls_and_engines[0].1, &fresh_0),
            "key-0 was LRU and should have been evicted -> fresh engine on re-insert"
        );
    }
}
