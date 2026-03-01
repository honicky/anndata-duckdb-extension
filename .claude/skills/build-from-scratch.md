# Build From Scratch

How to build and test the AnnData DuckDB extension from a fresh checkout.

## Prerequisites

- Python 3.x with `uv` package manager
- Git
- HDF5 library (installed via Homebrew on macOS: `brew install hdf5`)
- Docker (optional, for Linux cross-compilation)

## Step 1: Initialize Submodules

After cloning or entering a new worktree, submodules must be initialized:

```bash
git submodule update --init --recursive
```

This pulls in `duckdb/` and `extension-ci-tools/` which are required for building.

## Step 2: Build

### Local Build (macOS with HDF5 installed)

```bash
uv run make
```

This will:
- Use cmake to configure and build in `build/release/`
- Produce the extension at `build/release/extension/anndata/anndata.duckdb_extension`

### Docker Build (Linux)

```bash
mkdir -p ccache_dir
docker run --env-file=docker_env.txt -v `pwd`:/duckdb_build_dir -v `pwd`/ccache_dir:/ccache_dir duckdb/linux_amd64 make
```

## Step 3: Run Tests

### Local Tests

```bash
uv run make test_release
```

This runs the DuckDB sqllogictest runner against all `test/sql/*.test` files.
Tests requiring `httpfs` extension will be skipped.

### Docker Tests (Linux)

```bash
docker run --env-file=docker_env.txt -v `pwd`:/duckdb_build_dir -v `pwd`/ccache_dir:/ccache_dir duckdb/linux_amd64 make test_release
```

### Running a Single Test

```bash
# Run the unittest binary directly with a filter
./build/release/test/unittest "test/sql/wildcard_local.test"
```

## Step 4: Code Quality Checks (REQUIRED before committing)

```bash
# Check code formatting (reports issues)
uv run make format

# Auto-fix formatting issues
uv run make format-fix

# Check for clang-tidy issues
uv run make tidy-check
```

## Step 5: Run with DuckDB CLI

```bash
./build/release/duckdb -unsigned
```

Then in the DuckDB shell:
```sql
LOAD anndata;
SELECT * FROM anndata_info('test/data/test_small.h5ad');
```

## Common Issues

### "No such file: extension-ci-tools/makefiles/duckdb_extension.Makefile"

Submodules not initialized. Run: `git submodule update --init --recursive`

### "Could NOT find HDF5"

HDF5 not installed or not found. On macOS: `brew install hdf5`

### Test Data Generation

Test `.h5ad` files are tracked in git under `test/data/`. To regenerate:

```bash
uv run python test/python/create_test_wildcard.py    # wildcard test files
uv run python test/python/create_test_comprehensive.py  # comprehensive test file
```

## Clean Build

```bash
rm -rf build/
uv run make
```
