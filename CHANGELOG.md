# Changelog

All notable changes to the AnnData DuckDB Extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **WASM file access** (`wasm_mvp`, `wasm_eh`): files registered with duckdb-wasm
  (`registerFileBuffer`, URLs, OPFS) can now actually be queried. HDF5's default (POSIX) driver
  reads Emscripten MEMFS, which duckdb-wasm never populates - registered files live in its
  `WebFileSystem`. The new bridge (`src/wasm_file_image.cpp`) reads the whole file through
  `duckdb::FileSystem` and opens it as an in-memory HDF5 CORE file image
  (`spec/wasm-support-spec.md`, "Stepping stone: `H5FD_CORE`"). Whole-file-resident: bounded by
  wasm32 memory; the ranged-read VFD (spec Phase 3) supersedes this for large/remote files.
  Unregistered paths now produce an actionable error naming `registerFileBuffer`. The CI load
  smoke registers a real fixture and asserts `anndata_scan_obs`/`anndata_scan_var`/
  `anndata_scan_x` results plus the `ATTACH (TYPE ANNDATA)` path end to end.
- **In-browser demo** (`demo/browser/`): a terminal-style page that boots DuckDB-WASM from
  jsDelivr (pinned in lockstep with `duckdb_version`), loads the locally built extension, and
  queries `.h5ad` files dropped into the tab - entirely client-side. `python3 demo/browser/serve.py`
  after `make wasm_eh`.

### Fixed
- **WASM artifacts are now loadable** (`wasm_mvp`, `wasm_eh`). The wasm side module is produced by
  a `POST_BUILD emcc -sSIDE_MODULE=2` step that links only the archives named in
  `duckdb_extension_load`'s `LINKED_LIBS` — `target_link_libraries()` is a no-op there — so HDF5
  was never linked in and `LOAD anndata` died at `dlopen` with 63 unresolved `H5*` symbols
  ([#24](https://github.com/honicky/anndata-duckdb-extension/issues/24)). `CMakeLists.txt` now sets
  `DUCKDB_EXTENSION_ANNDATA_LINKED_LIBS` with all four required archives (`libhdf5.a`, `libsz.a`,
  `libaec.a`, `libz.a` — HDF5's static filter table references szip unconditionally, and
  `libhdf5.a` alone still leaves `inflate`/`compress2`/`SZ_*` unresolved). The artifact grows from
  446 KB (broken) to ~3 MB (loadable). The libhdf5-wasm FetchContent download is now pinned by
  `URL_HASH`.

### Added
- **CI gates for wasm** (`wasm-checks` job), closing the hole that let non-loadable artifacts ship
  green for eleven months: a link-level symbol contract check (`scripts/wasm_symbol_check.py` —
  pure-Python wasm parser proving `imports(side) − exports(side) − exports(main) − allowlist = ∅`
  against the pinned `@duckdb/duckdb-wasm` main module, plus size floor/ceiling and metadata
  checks) and a Node load smoke test (`test/wasm/run_node.mjs` — real `INSTALL` + `LOAD` +
  `anndata_version()` + function registration under duckdb-wasm). Version pins are documented in
  `test/wasm/README.md` and must move in lockstep with `duckdb_version`.
- The daily community-extensions descriptor monitor now also verifies the upstream descriptor
  keeps `excluded_platforms: "wasm_mvp;wasm_eh;wasm_threads"` while wasm file access is
  unimplemented, so the exclusion cannot silently disappear upstream.

### Changed
- `wasm_mvp`/`wasm_eh` are built and gated again (previously fully excluded); they remain
  **undistributed** — file access requires the `duckdb::FileSystem` VFD port
  (`spec/wasm-support-spec.md`, Phase 2+). `wasm_threads` remains excluded: extension-ci-tools
  builds extensions without `-fwasm-exceptions` against a COI main module built with it, and the
  prebuilt HDF5 archives are non-pthread.

## [0.14.6] - 2026-08-19

### Changed
- **WebAssembly builds are now excluded from CI and distribution** (`wasm_mvp`, `wasm_eh`,
  `wasm_threads`). The published wasm artifacts could never be loaded: HDF5 is not linked into the
  wasm side module, so `LOAD anndata` failed during `dlopen` with
  `bad export type for 'H5T_NATIVE_INT32_g': undefined`. The wasm link step uses
  `-sSIDE_MODULE=2`, which tolerates undefined symbols, so CI reported success while producing a
  non-loadable 446 KB artifact containing 63 unresolved `H5*` imports. Excluding the platform means
  users get a clear "not available" error instead of a cryptic load failure. See
  [#24](https://github.com/honicky/anndata-duckdb-extension/issues/24); proper wasm support is
  designed in `spec/wasm-support-spec.md`.

### Fixed
- **CI: unbreak the `Build against duckdb/main` job.** All three `main-distribution` arches (`linux_amd64`, `linux_amd64_musl`, `linux_arm64`) started failing to compile once `duckdb/main` landed the new `duckdb::Identifier` type (`duckdb/common/identifier.hpp`) and rolled it out across the string-keyed APIs. `Identifier` is implicitly constructible from a *string literal* but **explicitly** from a runtime `string`, so every call site that handed DuckDB a `string` stopped compiling. Three distinct breakages, 31 errors total:

  1. `ClientContext::TryGetCurrentSetting` now takes `const Identifier &key`, breaking `TryGetSettingString()` in `s3_credentials.hpp` (which forwards a runtime `string`). The literal-key call in the same header was already fine.
  2. `table_function_bind_t` now reports output column names as `vector<Identifier> &`, so all 30 `*Bind` signatures in `anndata_scanner.{hpp,cpp}` no longer matched the typedef — every one of the 21 `TableFunction` constructions was rejected.
  3. `AttachInfo`, `AttachOptions` and `AttachedDatabase` were only forward-declared at the point of use in `anndata_storage.cpp`. `duckdb/storage/storage_extension.hpp` is byte-identical between v1.5.5 and `duckdb/main`, but it no longer pulls those definitions in transitively — the extension had been relying on an incidental include edge.

  Fixed through the existing `src/include/duckdb_compat.hpp` shim rather than with `#ifdef`s at the call sites, so the same source compiles against both DuckDB versions and the fix can live on `main` instead of diverging `main-distribution`. Added `compat::BindColumnNames` (`vector<Identifier>` vs `vector<string>`), `compat::SettingKey()` and `compat::BindColumnNamesToStrings()`, all keyed off the existing `__has_include("duckdb/common/identifier.hpp")` probe. Bind bodies now append with `emplace_back()`, which direct-initializes and therefore reaches `Identifier`'s explicit constructor; `push_back()` only ever compiled for string literals, which is why the trap is asymmetric — a `push_back(some_string)` added on `main` builds fine against v1.5.5 and fails a day later against `duckdb/main`. The two missing attach headers (`duckdb/parser/parsed_data/attach_info.hpp`, `duckdb/main/attached_database.hpp`) are at the same paths with the same definitions on v1.5.5, so including them explicitly is safe on both.

  The shim also gained a `static_assert` tripwire that deduces the real column-name type out of DuckDB's own `table_function_bind_t` typedef and asserts it matches what the `__has_include` probe inferred. The probe keys on the *presence of a header*, which is a proxy rather than the fact itself; if DuckDB ever moves the name type without adding or removing `identifier.hpp`, the build now fails with one readable message naming the file to edit, instead of a cascade of signature mismatches at all 30 bind functions. Confirmed by forcing the skew locally: the assertion fires first, ahead of the type-mismatch errors.

  Verified locally with two full builds from the same tree: against `duckdb/main` (`f8e1c96a53`) and against the pinned stable `v1.5.5` (`d8cdaa33fd`). Both compile clean, and the extension suite passes on both with byte-identical per-file assertion counts across all 25 test files (20 cases; the 5 skips are the httpfs/S3-env-gated remote tests). `make format` and `make tidy-check` pass. No behavior change on the stable build — `compat::BindColumnNames` is exactly `vector<string>` there.

  Worth recording for next time: **`make format` does not currently check this extension's `src/` at all.** `extension-ci-tools` passes `T="--workdir $PWD --directories src test"` down to `make -C duckdb format-check`, but v1.5.5's recipe is `scripts/format.py --all --check` — `$(T)` is silently dropped, so format.py runs with DuckDB's own tree as its working directory. `duckdb/main`'s recipe appends `$(T)` and does honor it. The CI log for `duckdb-main-code-quality` shows the un-scoped form, so the gate is green today for both branches while 43 pre-existing violations sit in `src/` (42 in `anndata_scanner.cpp`, almost all the same missing space in `compat::SetChunkCardinality(output,count)`, plus one continuation indent in `anndata_storage.cpp`). Those are left alone here to keep this diff to the build fix, but they will surface the moment the scoping starts working. This change itself is format-neutral: measured against `origin/main` with the pinned clang-format 11.0.1, the violation count per touched file is unchanged.

- Documentation accuracy: `README.md` no longer claims thread-safe operation on *all* platforms
  (Windows is single-threaded) and now documents the WebAssembly gap; `RELEASE.md` no longer lists
  WebAssembly as a built and tested platform.

## [0.14.5] - 2026-08-12

### Changed
- Bumped target DuckDB version from v1.5.4 to v1.5.5 (latest stable patch). The `duckdb` submodule and all `duckdb_version:` / artifact-name / CLI-download-URL / extension-path / `git checkout` references in `.github/workflows/MainDistributionPipeline.yml` now point at v1.5.5; `UpcomingDuckdbPipeline.yml` needed no change because its `stable-build` job reads the version dynamically from `check-release`. The `extension-ci-tools` submodule moved to current `main`, and `ci_tools_version: main` / `uses: ...@main` stay as they are so upstream CI-tooling breakage keeps surfacing immediately.

  No source changes were required — v1.5.5 is a patch release (extension pin bumps, a concurrent `ALTER`/`INSERT` crash fix, a `range()` TIMESTAMP fix, C API destructor exposure) and broke none of the DuckDB internals this extension uses. Verified locally against v1.5.5: clean build, full extension suite green (647 assertions, 20 cases; the 5 skips are the httpfs/S3-env-gated remote tests), plus `make format` and `make tidy-check`.

## [0.14.4] - 2026-08-11

### Fixed
- **CI: unbreak the `linux_amd64_musl` and `linux_arm64` builds.** Both had been failing every day since ~2026-07-29. vcpkg's `hdf5` port enables its `szip` feature by default, which pulls in `libaec`, whose portfile at our pinned registry baseline (`62efe42f`, Sep 2025) downloads the source tarball from `gitlab.dkrz.de`. That host now returns HTTP 429 to GitHub Actions runner IP ranges, so the download failed all four vcpkg retries and aborted `vcpkg install`. Only musl/arm64 were affected because they get no hits from DuckDB's read-only vcpkg binary cache (different toolchain → different ABI hash) and therefore build `libaec` from source; `linux_amd64` restores it from cache and only ever showed as `cancelled` via matrix fail-fast.

  Fixed by adding a `libaec` **overlay port** at `vcpkg-overlay/ports/libaec/`, alongside the `hdf5` overlay already there — a verbatim copy of the upstream port after microsoft/vcpkg moved the source to the `Deutsches-Klimarechenzentrum/libaec` GitHub mirror (libaec 1.1.7). Overlay ports take precedence over the registry baseline, so this replaces the one broken port and leaves every other version alone.

  Worth recording for next time: port versions in this repo are **not** governed by `builtin-baseline` in `vcpkg.json`. The root `vcpkg-configuration.json` declares a `default-registry` whose `baseline` (`62efe42f`, Sep 2025 — where `libaec` is 1.1.3#1, exactly what CI resolved) takes precedence over the manifest's `builtin-baseline`. Adding `builtin-baseline` / `overrides` to `vcpkg.json` therefore changes nothing. That file is also the reason a `vcpkg-configuration` key cannot be added to `vcpkg.json` at all — vcpkg rejects the combination outright with "Ambiguous vcpkg configuration provided by both manifest and configuration file".
- **CI: silence the Node.js 20 deprecation warnings.** Bumped `actions/checkout` v4 → v5, `actions/github-script` v7 → v8, and `actions/download-artifact` v4 → v7 — the lowest majors that run natively on Node 24. `download-artifact` v5's breaking change only affects downloads by artifact ID; we download by name, so behavior is unchanged.
- **CI: stop the `community-ext-stale` check from firing permanently.** It compared the upstream descriptor's `ref` against `git rev-parse "<tag>^{commit}"`, but `auto-tag` creates *annotated* tags, and the published descriptor pins the **tag object** SHA. Those two SHAs never match, so the check reported `stale` on every run regardless of the actual state — issue #21 was a false positive the whole time (the descriptor was already correct at v0.14.3). It now accepts either SHA form, and the generated diff suggests the tag object SHA to match the convention already in use upstream.

## [0.14.3] - 2026-07-07

### Changed
- Bumped target DuckDB version from v1.5.2 to v1.5.4 (latest stable patch). v1.5.4 backports the upstream fix "Remove checked_array_iterator from fmt dep", which resolves the Windows build failure on the GitHub `windows-2025-vs2026` runner (MSVC 14.51 removed `stdext::checked_array_iterator`). v1.5.3 did not include this fix.
- Consolidated the `MonitorUpstream.yml` workflow into `UpcomingDuckdbPipeline.yml`. The single workflow now handles both upcoming-release tracking (sync + build/quality vs `duckdb/main` + `duckdb-main-broken`) and stable-upstream monitoring (`new-duckdb-release`, `ci-upstream-regression`, `community-ext-stale`). The monitoring jobs are gated to schedule/dispatch runs so they don't run on `main-distribution` pushes. Dropped the redundant `descriptor-sync-check` job (label `descriptor-out-of-sync`): it compared the descriptor's `ref` against `main`'s branch HEAD and expected a `ref_next` field, but the published descriptor pins `ref` to a release-tag SHA and has no `ref_next` — so it reported false positives. Descriptor freshness is now covered solely by the tag-based `community-ext-stale` check.

### Fixed
- X wide table now renames duplicate variable (gene) names with `_1`, `_2`, ... suffixes, matching the `var`/raw-X tables and the attach-time warning. Previously, querying `*.X` on a file with non-unique `var_names` (e.g. `feature_name` used as both var_id and var_name) failed with `Binder Error: table "anndata_scan_x" has duplicate column name`.

### Compatibility
- Extended `src/include/duckdb_compat.hpp` to cover the catalog API rework on `duckdb/main` (post-v1.5.x): `DefaultGenerator::CreateDefaultEntry`/`GetDefaultEntries` now take/return the new `Identifier` type instead of `string`, and `CreateViewInfo` replaced its public `schema`/`view_name` members with `SetSchema()`/`SetViewName()`. The shim is gated on `__has_include("duckdb/common/identifier.hpp")` and is a no-op on all released v1.5.x versions (verified: it compiles unchanged against v1.5.4), so it only affects the `main-distribution` build against `duckdb/main`. This unbreaks the Upcoming DuckDB Pipeline.

## [0.14.2] - 2026-05-07

### Added
- **Two-branch upcoming-release tracking**, matching the workflow described in DuckDB's [community-extensions developer guide](https://duckdb.org/community_extensions/development) (`ref` / `ref_next` in the descriptor):
  - `main` continues to track the stable DuckDB release (currently `v1.5.2`).
  - A new long-lived `main-distribution` branch tracks `duckdb/main`. It is auto-bootstrapped on first run.
  - `.github/workflows/UpcomingDuckdbPipeline.yml` (renamed in spirit; same filename) now: (a) auto-merges `main` → `main-distribution` daily, (b) builds + code-quality-checks `main-distribution` against `duckdb/main`, (c) on build failure opens a `duckdb-main-broken` tracking issue with `@claude` mention, (d) on merge conflict opens a `next-merge-conflict` issue with `@claude` mention, (e) on success checks the upstream `duckdb/community-extensions` descriptor and opens a `descriptor-out-of-sync` issue with a ready-to-paste diff if `ref` / `ref_next` are stale.
  - All three tracking issues auto-close once the underlying problem is resolved.

### Changed
- Bumped target DuckDB version from v1.5.0 to v1.5.2 (latest stable patch). The `duckdb` submodule and all `duckdb_version:` / artifact / URL references in `.github/workflows/MainDistributionPipeline.yml` now point at v1.5.2. The `extension-ci-tools` submodule and `ci_tools_version:` / `uses: ...@main` references stay on `main` so the stable build still surfaces breakages in upstream CI tooling immediately.

## [0.14.1] - 2026-03-13

### Fixed
- Fix deploy pipeline: v0.14.0 deploy failed because `linux_amd64_musl` artifact was missing (build added after tag was created)

## [0.14.0] - 2026-03-12

### Added
- **Multi-file wildcard query support** for all `anndata_scan_*` functions using glob patterns (e.g., `'data/*.h5ad'`, `'s3://bucket/*.h5ad'`)
- Schema harmonization with two modes: `intersection` (default, common columns only) and `union` (all columns, NULL for missing)
- Automatic `_file_name` column added to multi-file query results for source tracking
- Glob handler for expanding local and S3 file patterns
- Schema harmonizer with type coercion across files (numeric promotion, VARCHAR fallback)
- Projection pushdown support for multi-file X matrix and layer queries
- `schema_mode` named parameter for obs, var, X, layers, obsm, and varm scan functions
- File-scoped pair concatenation for obsp/varp multi-file queries
- Test data generator (`test/python/create_test_wildcard.py`) and comprehensive wildcard test suites for local and S3

### Fixed
- Windows SIGSEGV caused by ODR violation: renamed internal `ColumnInfo` to `AnndataColumnInfo` to avoid conflict with DuckDB's `duckdb::ColumnInfo`
- Windows path normalization for glob results (backslash to forward slash for HDF5 compatibility)
- Skip `anndata_union.test` on Windows where HDF5 is not built with thread-safety

## [0.13.5] - 2026-03-12

### Fixed
- S3 errors now show the real error message (e.g., "Access denied", "File not found") instead of misleading "not a valid AnnData file"
- Credentials set via `load_aws_credentials()` or `SET s3_access_key_id` now work with ATTACH and `anndata_scan_*()` functions
- Remote test files now use `INSTALL/LOAD httpfs` instead of `require httpfs` so they run in the unittest runner
- Updated info table row counts in S3 and HTTP tests to match current output (15 properties)

## [0.13.4] - 2026-02-05

### Fixed
- Duplicate variable name warning no longer prints twice during DESCRIBE operations

## [0.13.3] - 2026-01-31

### Fixed
- Fixed MinGW build failure by excluding OpenSSL and curl dependencies. Remote file access (HTTP/S3) is not available on MinGW builds, but local file access works normally.

## [0.13.2] - 2026-01-23

### Added
- Comprehensive remote file test coverage for HTTP and S3 access, testing all AnnData features (obs, var, X, obsm, varm, layers, obsp, varp, info) via both ATTACH and function interfaces
- New `test_comprehensive.h5ad` test file containing all AnnData features for thorough testing
- Python script `create_test_comprehensive.py` to generate the comprehensive test file

## [0.13.1] - 2026-01-23

### Changed
- Optimized obs/var table queries by caching categorical column categories. Previously, categories were re-read from HDF5 for every chunk (~2048 rows), causing significant slowdown on large tables. Now categories are read once and cached, dramatically improving query performance especially on remote files.

## [0.13.0] - 2026-01-22

### Added
- New `info` table in attached AnnData databases showing file metadata (dimensions, matrices, layers)
- Documentation for remote file access (HTTP/HTTPS and S3) in README

### Changed
- Optimized h5ad file attach for faster open times:
  - Lazy loading of categorical column values (loaded at query time instead of attach time)
  - Reuse of HDF5 reader during attach to avoid duplicate file opens
  - Smarter var column detection with smaller sample size and early termination

### Fixed
- Improved error messages for file not found and remote access failures
- Better error reporting when AnnData file validation fails

## [0.12.2] - 2026-01-21

### Fixed
- S3 URLs now automatically detect and handle region redirects. When a bucket is in a different region than the default (us-east-1), the extension captures the `x-amz-bucket-region` header and retries with the correct region.

## [0.12.1] - 2026-01-20

### Added
- 

### Changed
- 

### Fixed
-

## [0.12.0] - 2026-01-20

### Added
- S3 remote file access with AWS SigV4 authentication
- HTTP/HTTPS remote file access for publicly accessible .h5ad files
- LRU block cache (64MB, 1MB blocks) for improved remote file performance
- Custom HTTP VFD (Virtual File Driver) for HDF5 remote access

### Changed
- Replaced HDF5's built-in ROS3 VFD with custom HTTP VFD for all remote access
- Use OpenSSL directly for HMAC-SHA256 signing instead of HDF5 ROS3

## [0.11.5] - 2025-12-30

### Fixed
- Read obs/var counts from X matrix shape attribute when var/_index or obs/_index datasets don't exist
- Fixed bug where shape attribute was incorrectly read from file root instead of /X group for sparse matrices

## [0.11.4] - 2025-12-30

### Fixed
- Categorical columns with int16/int32 codes are now properly decoded (fixes NULL values for columns like `feature_name` with >127 categories)
- Integer and float categorical columns (e.g., `feature_length`) are now properly decoded in obs/var tables

## [0.11.3] - 2025-12-29

### Fixed
- Code formatting for CI compliance

## [0.11.2] - 2025-12-29

### Fixed
- Categorical var columns (e.g., `feature_name` stored as pandas categoricals) are now properly decoded when used for gene names

## [0.11.1] - 2025-12-29

### Fixed
- LZF compression code now builds on Windows MinGW by using portable 64-bit integer type

## [0.11.0] - 2025-12-29

### Added
- `VAR_NAME_COLUMN` and `VAR_ID_COLUMN` ATTACH options to specify which var columns contain gene names and IDs
- Auto-detection of gene name/ID columns using heuristics when options not specified
- Informational message printed to stderr when auto-detecting var columns

## [0.10.0] - 2025-12-29

### Added
- LZF compression filter support for reading h5ad files with LZF-compressed datasets (common in AnnData files created by h5py/scanpy)

### Fixed
- AnnData file validation now checks HDF5 content structure instead of requiring `.h5ad` file extension, allowing UUID-named files to be attached correctly

## [0.9.0] - 2025-12-18

### Changed
- **BREAKING**: Updated to DuckDB 1.4.x API compatibility
- Extension now uses `ExtensionLoader` instead of deprecated `DuckDB` parameter
- Removed dependency on deprecated `ExtensionUtil` class
- Updated storage extension callbacks to use `optional_ptr<StorageExtensionInfo>` and `AttachOptions`
- CI/CD pipeline now targets DuckDB v1.4.3

## [0.8.3] - 2025-12-17

### Fixed
- Layer tables now use `_index` column for gene names (consistent with X matrix) instead of falling back to generic `Gene_000` names

## [0.8.2] - 2025-12-16

### Added
- Projection pushdown for X matrix and layer scans - queries selecting specific genes now only read requested columns from HDF5
- `ReadMatrixColumns` method for column-selective matrix reading with HDF5 hyperslab selection
- Installation instructions in README for extension repository and local builds

### Changed
- X and layer table functions now capture DuckDB's column_ids for optimized reading
- Updated LICENSE to MIT with proper copyright attribution
- Expanded README with comprehensive usage examples and API documentation

### Fixed
- CSC sparse matrix format detection now correctly reads encoding-type attribute from HDF5 group
- String memory management: use `StringVector::AddString()` instead of `SetValue()` for proper string lifecycle
- NULL value handling: use validity masks with `SetInvalid()` instead of `EmptyString()`
- Made H5ReaderMultithreaded move-only to prevent accidental copies and resource issues
- Segfaults in subqueries accessing the same HDF5 file multiple times

## [0.8.1] - 2025-12-15

### Added
- 

### Changed
- 

### Fixed
-

## [0.8.0] - 2025-12-15

### Added
- ATTACH/DETACH semantics for mounting .h5ad files as virtual databases
- New syntax: `ATTACH 'file.h5ad' AS name (TYPE ANNDATA)`
- Dynamic table discovery from HDF5 structure (obs, var, X, obsm_*, varm_*, layers_*, obsp_*, varp_*, uns)
- `SHOW ALL TABLES` support for listing tables in attached AnnData databases
- Cross-database queries with multiple attached AnnData files

## [0.7.0] - 2025-09-29

### Changed
- Migrated from HDF5 C++ API to HDF5 C API to enable thread-safe builds
- Switched vcpkg HDF5 dependency from "cpp" to "threadsafe" feature
- Renamed H5ReaderNew to H5ReaderMultithreaded for clarity

### Fixed
- Fixed race condition in H5ReaderMultithreaded initialization using std::call_once
- Resolved concurrent file access issues enabling proper UNION query support

## [0.6.0] - 2025-09-25

### Added
- Concurrent file access protection to prevent crashes when multiple HDF5 files are accessed simultaneously
- Global tracking of active H5Reader instances to detect and prevent unsafe concurrent access
- Clear error messages explaining HDF5 C++ API thread-safety limitations

### Changed
- Test queries that access multiple H5 files now use temporary tables as a workaround for HDF5 limitations
- Updated test documentation to explain the HDF5 C++ API thread-safety constraints

### Fixed
- Resource leak in IsGroupPresent() that was causing "Group::~Group - H5Gclose failed" errors
- Crashes when attempting to open multiple HDF5 files concurrently (now fails gracefully with informative error)

## [0.5.0] - 2025-09-25

### Added
- Support for obsp (observation pairwise) matrices via `anndata_scan_obsp` table function
- Support for varp (variable pairwise) matrices via `anndata_scan_varp` table function
- H5Reader methods for reading sparse CSR matrices from obsp/varp data
- Test coverage for obsp/varp functionality

### Changed
- Extended AnnDataScanner to handle pairwise matrix data structures

### Fixed
- Struct inheritance issues in ObspScanFunction and VarpScanFunction classes

## [0.3.11] - 2025-09-22

### Fixed
- Remove ACL flags from S3 deployment to support modern S3 buckets
- Add custom deploy script that works without ACL requirements
- Update S3 setup documentation with bucket policy approach

## [0.3.10] - 2025-09-22

### Fixed
- Implement custom deploy job to handle S3 deployment from repositories outside duckdb org
- Deployment now properly accesses repository secrets

## [0.3.9] - 2025-09-21

### Fixed
- Corrected AWS secret names in documentation to match CI tool requirements (S3_DUCKDB_ORG_ prefix)

## [0.3.8] - 2025-09-21

### Added
- AWS S3 deployment support for direct installation via `INSTALL anndata FROM 's3://bucket'`
- Automatic deployment on version bumps when AWS credentials are configured

### Changed
- Simplified CI/CD workflow - deployment now integrated into main build pipeline
- Removed separate Deploy.yml workflow in favor of automated deployment

### Fixed
- Deployment artifact mismatch by using same workflow run for build and deploy

## [0.3.7] - 2025-09-20

### Changed
- Improved release workflow with repository-style distribution structure
- Simplified installation process for testers with direct URL installation
- Removed redundant standalone files from releases

### Fixed
- All 10 platform artifacts now included in releases (including musl, mingw, and wasm variants)
- Release assets properly structured for DuckDB's INSTALL FROM URL feature

## [0.3.6] - 2025-09-20

### Fixed
- Added proper permissions for GitHub Actions to create tags and releases
- Integrated release process into main build pipeline

## [0.3.5] - 2025-09-20

### Fixed
- Updated GitHub Actions from deprecated v3 to v4 versions

## [0.3.4] - 2025-09-20

### Added
- Automatic release workflow triggered by VERSION file changes
- Auto-tagging when version is bumped

### Changed
- Release process now fully automated - just bump version and push

### Fixed
- No manual tag creation needed anymore

## [0.3.3] - 2025-09-20

### Changed
- Optimized release workflow to reuse build artifacts from main branch
- Separated build and release workflows for efficiency

### Fixed
- Eliminated redundant builds when creating releases

## [0.3.2] - 2025-09-20

### Fixed
- Deploy job only runs on version tags to avoid duplicate deployments
- Prevent deploy conflicts when pushing to main before tagging

## [0.3.1] - 2025-09-20

### Added
- GitHub Release creation in CI/CD pipeline

### Changed
- Deploy workflow now creates GitHub releases for version tags
- Release notes automatically extracted from CHANGELOG.md

### Fixed
- Extension deployment works without AWS S3 credentials

## [0.3.0] - 2025-09-20

### Added
- Support for reading layer matrices (alternative expression matrices like raw counts, normalized data)
- Table functions for each layer: `anndata_scan_layers_<layer_name>()`
- Unified matrix reading implementation for both X and layers
- Batch reading optimization for layers (improved performance)
- Specification for unstructured data (uns) handling
- Release automation using DuckDB's CI/CD pipeline
- Version management system with single source of truth
- Documentation for extension signing process

### Changed
- Refactored matrix reading to use unified implementation for X and layers
- Version now managed from single VERSION file source of truth
- Improved sparse matrix handling with proper integer type support

### Fixed
- Integer sparse matrix data not being read correctly
- Code duplication between X and layer matrix reading (~400 lines reduced)

## [0.2.0] - 2025-09-20 [SKIPPED]
- Version tag already existed, skipped to 0.3.0

## [0.1.0] - 2024-01-18

### Added
- Initial release of AnnData DuckDB Extension
- Read support for obs (observations) metadata table
- Read support for var (variables/genes) metadata table  
- Read support for X (expression) matrix with both dense and sparse formats
- Read support for obsm matrices (dimensional reductions like PCA, UMAP)
- Read support for varm matrices (variable-level embeddings)
- Read support for layers (alternative expression matrices)
- Automatic detection and handling of categorical columns
- Support for CSR and CSC sparse matrix formats
- Batch reading optimization for improved performance
- Type-aware value conversion (FLOAT, DOUBLE, INTEGER, BIGINT)
- Table functions: `anndata_scan_obs`, `anndata_scan_var`, `anndata_scan_X`, `anndata_scan_obsm`, `anndata_scan_varm`, `anndata_scan_layers`
- Utility functions: `anndata_info`, `anndata_version`

### Technical Details
- Built with HDF5 C++ library for efficient H5AD file reading
- Compatible with DuckDB v1.3.2
- Cross-platform support (Linux, macOS, Windows, WebAssembly)