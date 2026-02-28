# Build From Scratch

This skill describes how to build the AnnData DuckDB extension from a fresh checkout.

## Prerequisites

- Python 3.x with `uv` package manager
- Docker (for production builds and testing)
- Git

## Quick Build (Development)

For local development builds, the extension requires HDF5 libraries. The easiest approach is to use Docker:

```bash
# Build using Docker (recommended)
docker run --env-file=docker_env.txt -v `pwd`:/duckdb_build_dir -v `pwd`/ccache_dir:/ccache_dir duckdb/linux_amd64 make
```

If HDF5 is installed locally:

```bash
# Build with vcpkg (if vcpkg is set up)
VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake uv run make

# Or if HDF5 is installed system-wide
uv run make
```

## Running Tests

```bash
# Run all tests using Docker
docker run --env-file=docker_env.txt -v `pwd`:/duckdb_build_dir -v `pwd`/ccache_dir:/ccache_dir duckdb/linux_amd64 make test_release
```

## Code Quality Checks (REQUIRED before committing)

Always run these checks before committing:

```bash
# Check code formatting
uv run make format

# If formatting changes are needed, fix them:
uv run make format-fix

# Check for clang-tidy issues
uv run make tidy-check
```

## Docker Environment File

The `docker_env.txt` file should contain environment variables for the build. If it doesn't exist, create it:

```bash
# Minimal docker_env.txt
echo "" > docker_env.txt
```

## Common Build Issues

### HDF5 Not Found

If you see "Could NOT find HDF5", use the Docker build method instead.

### Submodule Issues

If duckdb or extension-ci-tools are missing:

```bash
git submodule update --init --recursive
```

### ccache Directory

Create the ccache directory if it doesn't exist:

```bash
mkdir -p ccache_dir
```

## Version Management

Use the bump_version.py script:

```bash
# Bump patch version
uv run python scripts/bump_version.py patch

# Bump minor version
uv run python scripts/bump_version.py minor

# Set specific version
uv run python scripts/bump_version.py set 0.14.0
```

## Full Clean Build

```bash
# Clean everything
rm -rf build/

# Rebuild
docker run --env-file=docker_env.txt -v `pwd`:/duckdb_build_dir -v `pwd`/ccache_dir:/ccache_dir duckdb/linux_amd64 make
```
