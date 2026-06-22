# Unit A.7 — Docs (design docs + user guide) — review log

Branch: `pr/delta-A7-docs` (based on `pr/delta-A6b-regression`). Commit `01cd61f81` (amended). Fork PR #12.

## Carve (docs only — no code)
15 files, +2650. Carved from `feat/delta-kernel-read`:
- `contrib/delta/docs/*.md` (12 internal design/history docs) — verbatim.
- `docs/source/user-guide/latest/delta.md` (new user-facing guide).
- `datasources.md` / `index.rst` — additive (Delta guide link + toctree entry); diffs are exactly
  the monolith's additive blocks.

## Audit (this is the unit's real work — the plan flags doc drift)
Two passes: (1) a thorough Explore agent cross-checking every config/class/module/proto/path
reference and inter-doc link against landed HEAD; (2) /code-review high focused on user-facing
semantic accuracy. The docs were ~accurate (faithful monolith reconstruction); links/toctree/version
matrix/native module list/proto messages all verified. Fixes:

**Reference drift (audit agent):**
- `12-elimination-evaluation.md`: proto `kernel_read` (field 25) row said "kept; planner errors if
  false" — it's `reserved 25` in operator.proto and `planner.rs` no longer reads it → "removed".
- `05-build-and-deploy.md`: `cargo build -p comet` → `-p datafusion-comet` (the package name `-p`
  takes; the doc's other 3 occurrences were already correct).

**User-facing accuracy (/code-review high, CONFIRMED):**
- delta.md storage: added **Azure** (`abfs`/`abfss`/`wasb`) + **GCS** (`gs`) — both ship via
  `object_store::parse_url` (engine.rs:106); the line listed only local/HDFS/S3 (understated).
- delta.md credentials: the residual gap is explicit Hadoop credential-provider CLASSES
  (`fs.s3a.aws.credentials.provider`, AssumedRole/WebIdentity), not "per-bucket S3 chains" (08 A2b:
  per-bucket static keys are FIXED). Rewrote the limitation; also dropped the stale GCS mention
  (08 A2a: FIXED).
- delta.md INT96: added the narrow far-future (~year 2262) overflow caveat (08 A6: delta-kernel gap,
  silent wrong data — the only "wrong data" risk, so worth surfacing). INT96 stays in supported
  features (works for all practical ranges).
- delta.md Java 17: applies to ALL Spark 4.x (spark-4.0 profile sets java.version=17 too), not just
  4.1; noted Scala 2.12 is Spark-3.5-only.
- delta.md usage: clarified the comet-spark jar must be the from-source `-Pcontrib-delta` build (the
  published artifact has zero Delta surface — line 58).

**Refuted / skipped:**
- REFUTED #9 (Tuning table "duplicates auto-generated configs.md"): GenerateDocs has NO Delta
  reference and contrib configs aren't in the default build, so the table is NECESSARY (the configs
  are not auto-published). Kept.
- Skipped #6 (`spark.comet.enabled`/`exec.enabled` in the example) + #8 (extraClassPath): both default
  true and match iceberg.md's established convention for the dedicated datasource guides.
- Skipped the ambiguous `errors.rs`→`error.rs` cosmetic (doc 11): `native/jni-bridge/src/errors.rs`
  (plural) is a real file that also discusses cannotReadFilesError; line numbers already approximate
  in a historical audit doc — risk of a wrong "correction" outweighs benefit.

## Docs 10/11/12 archived (user decision, post-review)
Both review passes flagged docs 10/11/12 (migration plan / coherence audit / elimination evaluation) as
point-in-time development-history artifacts. **User chose: archive.** Moved them to
`contrib/delta/docs/archive/` via `git mv` (rename, history preserved); repointed the 4 incoming links
(01/03/04/README → `archive/`); added an "Archived design history" note to the README. 01–08 + README
remain the living doc set. All links verified resolving. Amended into A.7 (`51e71587b`); A.8 cascade-
rebased onto the new A.7 (`b0644a6ad`); both force-pushed (#12/#13).

## §5 verification
Inter-doc links resolve; user-guide links + index.rst toctree correct (delta.md wired in); the one
"broken" hit was a false positive (full https://github.com/... URL). No GenerateDocs conflict
(delta.md/datasources.md are hand-edited, like iceberg.md). Docs-only — no build/test/gate impact.

**State:** A.7 review clean, pushed (#12). Remaining: A.8 (perPartitionFilePaths / FAILED_READ_FILE
follow-up — gated on #4536, which has now merged to main). Also pending: #4366 reconcile/close at
end-of-cycle (§9.4); flaky CI re-runs on #4/#5.
