# Release Process

This document describes how to release the AnnData DuckDB Extension.

## Overview

The release process uses DuckDB's standard extension CI/CD pipeline. When you push a version tag, the extension is automatically built for all platforms and deployed.

## How to Release

### 1. Prepare the Release

```bash
# Bump version (choose patch, minor, or major)
uv run python3 scripts/bump_version.py minor

# Review changes
git diff

# Commit version bump
git add -A
git commit -m "chore: bump version to X.Y.Z"
```

### 2. Create and Push Tag

```bash
# Create annotated tag
VERSION=$(cat VERSION)
git tag -a "v${VERSION}" -m "Release version ${VERSION}"

# Push changes and tag
git push origin main
git push origin "v${VERSION}"
```

### 3. What Happens Next

When you push a tag starting with `v`, the GitHub Actions workflow will:

1. **Build** the extension for all platforms:
   - Linux (x64, ARM64)
   - macOS (Intel, Apple Silicon)  
   - Windows (x64)
   - WebAssembly

2. **Test** the extension on each platform

3. **Deploy** the built artifacts using DuckDB's deployment pipeline

4. **Create GitHub Release** (if configured)

## Platform Artifacts

The CI builds the following artifacts:

- `anndata.duckdb_extension.gz` (compressed)
- Platform-specific binaries for each OS/architecture

## Version Management

- Version is stored in `VERSION` file
- CMake reads version at build time
- No need to update version in multiple places

## Deployment Options

The current setup uses `deploy_latest` for tags and main branch pushes. This means:
- Tagged releases are deployed as versioned releases
- Main branch pushes update the "latest" version
- Users can install specific versions or latest

## Installation

After release, users can install the extension:

```sql
-- For unsigned extensions (current state)
LOAD 'path/to/anndata.duckdb_extension';

-- Future: When signed and in community repository
INSTALL anndata;
LOAD anndata;
```

## Troubleshooting

If the release fails:
1. Check GitHub Actions logs
2. Ensure VERSION file matches tag
3. Verify CHANGELOG.md has entry for version
4. Make sure all tests pass locally first

## Future Improvements

- [ ] Sign extensions for trusted distribution
- [ ] Submit to DuckDB Community Extensions
- [ ] Automated changelog generation
- [ ] Release notes in GitHub releases