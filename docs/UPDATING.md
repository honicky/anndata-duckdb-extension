# Extension Updating

This document describes procedures for updating the extension version and upgrading to new DuckDB releases.

## Version Bumping

Use the `scripts/bump_version.py` script to update the extension version:

```bash
# Bump patch version (e.g., 0.1.0 -> 0.1.1) - for bug fixes
uv run python scripts/bump_version.py patch

# Bump minor version (e.g., 0.1.0 -> 0.2.0) - for new features
uv run python scripts/bump_version.py minor

# Bump major version (e.g., 0.1.0 -> 1.0.0) - for breaking changes
uv run python scripts/bump_version.py major

# Set a specific version
uv run python scripts/bump_version.py set 0.3.0
```

### What the Script Does

1. Updates the `VERSION` file with the new version number
2. Adds a new section to `CHANGELOG.md` with the current date
3. Prints instructions for completing the version bump

### Complete Version Bump Procedure

1. **Bump the version:**
   ```bash
   uv run python scripts/bump_version.py patch  # or minor/major
   ```

2. **Update the CHANGELOG.md** with your changes:
   - Edit the newly added section in `CHANGELOG.md`
   - Fill in Added/Changed/Fixed sections with your changes

3. **Run code quality checks:**
   ```bash
   uv run make format
   uv run make tidy-check
   ```

4. **Run tests:**
   ```bash
   uv run make
   ./build/release/test/unittest --test-dir test "[sql]"
   ```

5. **Commit and push:**
   ```bash
   git add VERSION CHANGELOG.md <other-changed-files>
   git commit -m "feat: description of changes"
   git push origin main
   ```

6. **Create a release tag (optional):**
   ```bash
   git tag -a v0.x.x -m "Release version 0.x.x"
   git push origin v0.x.x
   ```

## Upgrading to New DuckDB Releases

When a new DuckDB version is released and you need to update the extension:

### 1. Bump Submodules

Update the `duckdb` submodule to the latest tagged release:

```bash
cd duckdb
git fetch origin
git checkout v1.x.x  # Replace with the new version tag
cd ..
git add duckdb
```

Update the `extension-ci-tools` submodule to the corresponding branch:

```bash
cd extension-ci-tools
git fetch origin
git checkout v1.x.x  # Branch matching the DuckDB version
cd ..
git add extension-ci-tools
```

### 2. Update CI Workflow Versions

Edit `.github/workflows/MainDistributionPipeline.yml`:

- Update `duckdb_version` input in `duckdb-stable-build` job
- Update `duckdb_version` input in `duckdb-stable-deploy` job
- Update the reusable workflow reference for `duckdb-stable-build`:
  ```yaml
  uses: duckdb/extension-ci-tools/.github/workflows/_extension_distribution.yml@v1.x.x
  ```

### 3. Test the Build

```bash
# Clean and rebuild
make clean
uv run make

# Run tests
./build/release/test/unittest --test-dir test "[sql]"
```

### 4. Fix API Changes

DuckDB extensions are built against the internal C++ API, which may change between versions.

If your build fails after upgrading:

1. Check DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
2. Review [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
3. Look at git history for relevant C++ header files

Common changes include:
- Function signature changes
- Renamed classes or methods
- New required parameters
- Deprecated API removal

### 5. Commit All Changes

```bash
git add duckdb extension-ci-tools .github/workflows/MainDistributionPipeline.yml
git commit -m "chore: upgrade to DuckDB v1.x.x"
git push origin main
```
