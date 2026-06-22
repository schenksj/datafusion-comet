# Object-storage credential propagation audit — Delta contrib (session 5n)

Path: Spark Hadoop conf → `DeltaScanCommon.object_store_options` (proto field 8) → native
`delta_storage_config_from_map` (`jni.rs`) → `create_object_store` (`engine.rs`) → kernel engine,
used at every read site. Audited via Explore agent + spot-verified the P0 finding by hand.

## Findings (prioritized)

### 🔴 P0 — CDF reads DROP object-store credentials (REAL bug, confirmed by hand)
`convertCdf` (`CometDeltaNativeScan.scala`) builds the `DeltaScanCommon` with `setTableRoot` but
**never populates `object_store_options`** (no `resolveStorageOptions`/`putObjectStoreOptions` — the
V1 path does, at :704/:1217). `CometDeltaCdfScanExec.commonData` = `nativeOp...getCommon`, so the empty
map flows to the executor → `read_all_cdf` (`kernel_scan.rs:536`) builds a credential-less S3 store →
`readChangeFeed` on a PRIVATE bucket fails / misauthenticates. Not caught because CometDeltaCdcSuite
runs on local fs (no creds). Undocumented (A2 sections don't mention CDF).
**Fix:** `convertCdf` must resolve + put storage options into the common block (reuse `resolveStorageOptions`);
add a red-green test asserting the CDF proto's `object_store_options` is non-empty for an S3 table. Belongs to A.5.

### 🔴 P1 — Azure/GCS rely on executor-side ambient env (documented "parity", but real gap)
Driver extracts `fs.azure.*`/`fs.gs.*` (`NativeConfig.extractObjectStoreOptions`), but the executor's
`delta_storage_config_from_map` (`jni.rs`) bridges **only `fs.s3a.*`**; Azure/GCS keys are discarded and
`create_object_store` builds those via `object_store::parse_url` with **ambient env only** (`engine.rs:106`).
So Azure/GCS reads need `AZURE_*`/`GOOGLE_*`/ADC on every EXECUTOR — `spark.hadoop.fs.azure.*` on the driver
is NOT honored. Documented as A2c "parity with core", but that framing understates the executor-env
requirement. **Fix (larger):** bridge `fs.azure.*`/`fs.gs.*` into typed Azure/GCS config with explicit creds,
OR document the executor-env requirement prominently. Design-level — scope with user.

### ⚠️ P2 — Guard-suite gaps
`CometDeltaCredentialAuditSuite` + Rust `jni`/`engine` tests cover the pieces in isolation (S3 extraction,
per-bucket, non-leakage `azure_and_gcs_keys_do_not_leak_into_s3_config`, cache-key-by-cred) but: (a) NO
driver→proto e2e assertion that `convert()` actually populates `object_store_options` for S3; (b) CDF
path uncovered (where P0 hides). Add both.

### ⚠️ P3 — Secret-hygiene defense-in-depth
`DeltaStorageConfig` derives `Debug` with no redaction; no secret is logged today, but a future
`{config:?}` would dump `aws_secret_key`/`aws_session_token`. Add a redacting `Debug` impl.

### ℹ️ P4 — A2e residual (already documented)
`fs.s3a.aws.credentials.provider` provider classes (AssumedRole/WebIdentity) still not bridged — covered by
IMDS/ECS/env fallback only. Known/documented.

## What's CORRECT (✅)
- S3 static creds incl. **session token**, endpoint, region, path-style, and **per-bucket**
  `fs.s3a.bucket.<bucket>.*` — extracted on the DRIVER and shipped in the proto (executors don't re-derive).
- No cross-scheme/cross-bucket leakage (S3 config struct only reads `fs.s3a.*`; guard tests assert it).
- DV reads + metadata/row-count reads use the same credentialed `storage_config` (A2d).
- Engine/store cache key includes the full credential tuple (no cred bleed across buckets/rotation).
- Encryption Hadoop-conf broadcast is an orthogonal channel; doesn't drop object-store creds.
- No secret values logged on the path.

## Status: AUDIT ONLY (no code changed). Fixes pending user direction + the running 4.1 regression
freeing the build (P0 needs a build+red-green test).
