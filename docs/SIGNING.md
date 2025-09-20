# Extension Signing Guide

## Overview

DuckDB extensions can be signed to ensure their integrity and authenticity. There are three types of extensions based on signing:

1. **Core Extensions** - Signed with DuckDB's core key (only for vetted extensions by DuckDB team)
2. **Community Extensions** - Signed with community key (open-source, built by DuckDB CI)
3. **Unsigned Extensions** - Require `allow_unsigned_extensions` setting to load

## Current Status

Currently, the AnnData extension is **unsigned**. Users must run DuckDB with:
```bash
duckdb -unsigned
```

## How to Get Your Extension Signed

### Option 1: Submit to Community Extensions (Recommended)

This is the best way to get your extension signed. DuckDB will:
- Build your extension for all platforms
- Sign it with the community key
- Distribute it from their repository
- Users can install with `INSTALL anndata FROM community;`

#### Steps to Submit:

1. **Ensure Extension Compatibility**
   - Extension must build with DuckDB's CI toolchain
   - Must be based on the extension-template
   - Must be open-source and on GitHub
   - Must pass all tests

2. **Create description.yml**
   ```yaml
   extension:
     name: anndata
     description: Read AnnData/H5AD files for single-cell genomics
     version: 0.1.0
     language: C++
     build: cmake
     license: MIT
     maintainers:
       - your-github-username
   
   repo:
     github: yourusername/anndata-duckdb-extension
     ref: main  # or specific commit/tag
   
   docs:
     hello_world: |
       SELECT * FROM anndata_scan_obs('data.h5ad') LIMIT 10;
     extended_description: |
       The AnnData extension provides SQL access to H5AD files, the standard format 
       for single-cell genomics data. It supports reading observations (cells), 
       variables (genes), expression matrices, and metadata from AnnData files.
   ```

3. **Submit Pull Request**
   - Fork https://github.com/duckdb/community-extensions
   - Create folder: `extensions/anndata/`
   - Add your `description.yml` file
   - Open PR with clear description

4. **Wait for Approval**
   - DuckDB team reviews metadata (not code)
   - CI builds and tests your extension
   - Once approved, extension is signed and published

### Option 2: Custom Signing (Advanced)

For private or commercial extensions, you can implement custom signing:

1. **Generate Key Pair**
   - Create your own RSA key pair
   - Keep private key secure

2. **Sign Extension**
   - Sign the binary hash with your private key
   - Embed signature in extension metadata

3. **Distribute Public Key**
   - Users need your public key to verify
   - Users must configure DuckDB to trust your key

**Note**: This requires modifying DuckDB's trust settings and is not recommended for public extensions.

## Benefits of Signing

1. **Trust** - Users know extension is authentic
2. **Integrity** - Detects if extension was tampered with
3. **Easy Installation** - No need for `-unsigned` flag
4. **Distribution** - Can be served over HTTP (not requiring HTTPS)

## Current Workaround

Until the extension is signed, users must:

```sql
-- Start DuckDB with unsigned extensions allowed
duckdb -unsigned

-- Load the extension
LOAD '/path/to/anndata.duckdb_extension';
```

## Timeline for Community Extension

1. **Preparation** (1-2 days)
   - Ensure all tests pass
   - Create description.yml
   - Test with DuckDB CI locally

2. **Submission** (1 day)
   - Fork and create PR
   - Respond to feedback

3. **Review** (3-7 days)
   - DuckDB team reviews
   - CI builds and tests

4. **Publication** (Automatic)
   - Once approved, immediately available
   - Users can install from community repository

## Security Considerations

- Community extensions execute third-party code
- DuckDB team doesn't review code, only ensures it's open-source
- Users should review extension source before installing
- Report security issues to extension maintainers

## Next Steps

To prepare for community extension submission:

1. Ensure all tests pass consistently
2. Add comprehensive documentation
3. Create stable release (not just main branch)
4. Write clear usage examples
5. Set up issue tracking in your repository