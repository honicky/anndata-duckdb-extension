# Changelog

All notable changes to the AnnData DuckDB Extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2026-02-28

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