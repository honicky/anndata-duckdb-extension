# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a DuckDB extension for reading AnnData (.h5ad) files, which are the standard format for single-cell genomics data. The extension provides SQL access to annotated data matrices containing cells (observations) and genes (variables).

## Architecture

The extension maps AnnData components to DuckDB tables:
- `X` (expression matrix) → `main` table with (obs_id, var_id, var_name, value)
- `obs` (cell metadata) → `obs` table with observation metadata
- `var` (gene metadata) → Multiple tables: scalar columns in `var`, array columns in `var_<column>`
- `obsm`/`varm` (dimensional reductions) → Separate tables per matrix: `obsm_pca`, `obsm_umap`, etc.
- `layers` (alternative matrices) → Separate tables: `layers_raw`, `layers_normalized`, etc.
- `uns` (unstructured data) → Multiple approaches: `uns_scalar`, `uns_<dataframe>`, `uns_json`

## Key Design Decisions

1. **Read-only access**: Initial implementation focuses on reading AnnData files only
2. **Flexible gene/cell identifiers**: Support both IDs (e.g., Ensembl) and names (e.g., gene symbols) via configurable columns
3. **Variable-length array handling**: Arrays in var DataFrame decomposed into separate tables with (var_id, index, value) structure
4. **Sparse matrix optimization**: Automatic detection and efficient handling of sparse matrices
5. **Mirror SQLite extension**: Follow similar ATTACH/DETACH paradigm for familiarity

## SQL Interface

### Core Commands
```sql
-- Attach AnnData file
ATTACH 'data.h5ad' AS pbmc (TYPE ANNDATA);
ATTACH 'data.h5ad' AS pbmc (TYPE ANNDATA, VAR_NAME_COLUMN='gene_symbols', VAR_ID_COLUMN='ensembl_id');

-- Query data
SELECT * FROM pbmc.main WHERE var_name = 'CD3D';
SELECT * FROM pbmc.obs WHERE cell_type = 'T cell';
SELECT * FROM pbmc.obsm_umap;

-- Detach
DETACH pbmc;
```

### Configuration
```sql
SET anndata_chunk_size = 10000;
SET anndata_sparse_optimization = true;
SET anndata_cache_size = '1GB';
SET anndata_default_var_name_column = 'gene_symbols';
```

## Implementation Structure

Expected directory layout:
```
anndata_duckdb/
├── src/
│   ├── anndata_extension.cpp      # Main extension entry point
│   ├── anndata_attach.cpp          # ATTACH/DETACH implementation
│   ├── anndata_scanner.cpp         # Table function implementations
│   ├── anndata_schema.cpp          # Schema discovery and mapping
│   └── anndata_types.cpp           # Type conversion utilities
├── include/
│   └── anndata_extension.hpp       # Public API
└── CMakeLists.txt
```

## Development Notes

- The extension uses HDF5 library for reading .h5ad files
- Leverage DuckDB's columnar storage and query optimization
- Support zero-copy where possible to minimize memory overhead
- Implement lazy loading - data read on-demand from HDF5
- Push WHERE clause predicates to HDF5 reading when possible
- Handle sparse matrices efficiently using CSR/CSC formats
- use the documents in the `spec/` folder to guide the development process
- keep the specs in the `spec/implementation-plan.md` up to date as design decisions change
- use `uv` for all python package management. Prefer `uv add` over `uv pip install` in order to manage the package list with pyproject.toml

## Code Quality Checks Before Committing

**IMPORTANT**: Always run these checks before committing and pushing code to ensure CI/CD passes:

1. **Format Check**: 
   - Run `uv run make format` to check code formatting
   - If it reports changes needed, run `uv run make format-fix` to automatically fix formatting issues
   - The CI/CD uses DuckDB's formatting standards (tabs for indentation, specific spacing rules)

2. **Tidy Check**: 
   - Run `make tidy-check` to check for clang-tidy issues
   - Fix any reported issues before committing
   - Common issues include: missing explicit casts, using push_back instead of emplace_back, missing braces

3. **Build Verification**:
   - Ensure the extension builds successfully: `make`
   - For WASM builds, test locally if possible: `VCPKG_TOOLCHAIN_PATH="" make wasm_mvp`

Only commit and push after all checks pass locally. The CI/CD pipeline will fail if format or tidy checks don't pass.