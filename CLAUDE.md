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
- use `uv run python3` for all python actions

## Building from a Clean Environment

### Prerequisites

The build requires system-installed C/C++ libraries that are NOT managed by vcpkg in local dev (vcpkg.json is only used by CI):

```bash
apt-get install -y libhdf5-dev libcurl4-openssl-dev libssl-dev
```

You also need: `cmake`, `make`, `g++`, `git`, and `uv` (for Python tooling / running make targets).

### Submodule initialization

The repo has two git submodules (`duckdb/` and `extension-ci-tools/`) that must be populated before building. If these directories are empty:

```bash
git submodule init
git submodule update
```

### Building

The standard build command is:

```bash
uv run make          # builds the release target
```

This runs cmake configure + build via the Makefile in `extension-ci-tools/makefiles/duckdb_extension.Makefile`. The first build takes several minutes (it compiles all of DuckDB). Subsequent builds after source changes are incremental and fast.

For faster iteration when fixing compile errors, you can split configure and build:

```bash
# One-time configure:
rm -rf build/release
mkdir -p build/release
cmake -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_EXTENSION_CONFIGS="$(pwd)/extension_config.cmake" \
  -DCMAKE_BUILD_TYPE=Release \
  -S ./duckdb/ -B build/release

# Rebuild (run this after each source edit):
cmake --build build/release --parallel $(nproc)
```

This avoids re-running cmake configure on each iteration, which is the default behavior of `uv run make`.

### Build outputs

- `build/release/duckdb` — DuckDB CLI with the extension statically linked
- `build/release/test/unittest` — test runner binary
- `build/release/extension/anndata/anndata.duckdb_extension` — loadable extension

### Smoke test

```bash
./build/release/duckdb -c "SELECT anndata_version();"
```

### Running tests

```bash
# Run a single test file
build/release/test/unittest "test/sql/anndata_basic.test" --test-dir .

# Run all extension tests
build/release/test/unittest "test/*" --test-dir .
```

## Code Quality Checks Before Committing

**IMPORTANT**: Always run these checks before committing and pushing code to ensure CI/CD passes:

1. **Format Check**:
   - Run `uv run make format` to check code formatting
   - If it reports changes needed, run `uv run make format-fix` to automatically fix formatting issues
   - The CI/CD uses DuckDB's formatting standards (tabs for indentation, specific spacing rules)

2. **Tidy Check**:
   - Run `uv run make tidy-check` to check for clang-tidy issues
   - Fix any reported issues before committing
   - The tidy-check build uses a separate build directory (`build/tidy/`) and skips HDF5 (defines `DUCKDB_NO_HDF5`), so it doesn't need HDF5 installed
   - Common issues include: missing explicit casts, using push_back instead of emplace_back, missing braces

3. **Build Verification**:
   - Ensure the extension builds successfully: `uv run make`

Only commit and push after all checks pass locally. The CI/CD pipeline will fail if format or tidy checks don't pass.

## Version Management

**IMPORTANT**: Use the `scripts/bump_version.py` script to update the version number:

```bash
# Bump patch version (e.g., 0.1.0 -> 0.1.1)
uv run python scripts/bump_version.py patch

# Bump minor version (e.g., 0.1.0 -> 0.2.0)
uv run python scripts/bump_version.py minor

# Bump major version (e.g., 0.1.0 -> 1.0.0)
uv run python scripts/bump_version.py major

# Set a specific version
uv run python scripts/bump_version.py set 0.3.0
```

The script will automatically update the VERSION file. Remember to also:
1. Update the CHANGELOG.md with the new version and list of changes
2. Commit both VERSION and CHANGELOG.md together with your feature changes

- use uv run for all python actions

## Updating extension-ci-tools

The `extension-ci-tools` dependency is used in TWO places that must be updated together:

1. **Submodule** (for local builds): The `extension-ci-tools/` directory is a git submodule used by the local Makefile
2. **CI Workflow** (for CI builds): `.github/workflows/MainDistributionPipeline.yml` references extension-ci-tools via `uses:` and `ci_tools_version:`

**When updating extension-ci-tools:**

```bash
# 1. Update the submodule to latest
cd extension-ci-tools
git fetch origin
git checkout origin/main  # or a specific version tag like origin/v1.4.3
cd ..

# 2. Stage the submodule update
git add extension-ci-tools

# 3. Update the workflow file to match
# Edit .github/workflows/MainDistributionPipeline.yml:
#   - Change: uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@main
#   - Change: ci_tools_version: main
# (or use a specific version tag like @v1.4.3)

# 4. Commit both changes together
git add .github/workflows/MainDistributionPipeline.yml
git commit -m "chore: update extension-ci-tools to <version>"
```

**Why both?** The submodule provides Makefile targets for local development, while the CI workflow pulls extension-ci-tools directly from GitHub for builds.

## Upgrading DuckDB Version

When upgrading DuckDB to a new version (e.g., v1.4.4 → v1.5.0), ALL of these must be updated together:

### 1. Update submodules

```bash
# Update DuckDB submodule to the target version tag
cd duckdb && git fetch origin tag vX.Y.Z && git checkout vX.Y.Z && cd ..

# Update extension-ci-tools submodule to latest main
cd extension-ci-tools && git fetch origin && git checkout origin/main && cd ..

git add duckdb extension-ci-tools
```

### 2. Update CI workflow

In `.github/workflows/MainDistributionPipeline.yml`, update ALL occurrences of the old version to the new version. This includes:
- `duckdb_version:` in build and code-quality jobs
- Artifact names (e.g., `anndata-vX.Y.Z-extension-*`)
- DuckDB CLI download URL
- Extension directory paths (`~/.duckdb/extensions/vX.Y.Z/`)
- `git checkout vX.Y.Z` in the deploy job

### 3. Fix API breaking changes

DuckDB frequently changes internal C++ APIs between versions. Common breaking changes to watch for:
- **Storage extension registration** (changed in v1.5.0): Use `StorageExtension::Register(config, name, extension)` instead of `config.storage_extensions[name] = extension`. The factory function must return `shared_ptr<StorageExtension>` instead of `unique_ptr`.
- **Function signatures**: Check `attach_function_t`, `create_transaction_manager_t` typedefs in `duckdb/storage/storage_extension.hpp`
- **Catalog APIs**: `DuckCatalog`, `DefaultGenerator`, `CatalogTransaction` methods may change

### 4. Clean build, test, and run quality checks

A clean build is required after submodule changes (`rm -rf build/release`). Then follow the steps in "Building from a Clean Environment" and "Code Quality Checks Before Committing" above.