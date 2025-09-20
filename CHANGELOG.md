# Changelog

All notable changes to the AnnData DuckDB Extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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