# ATTACH Semantics Specification

## Overview

This specification describes the implementation of ATTACH semantics for AnnData files, enabling users to mount `.h5ad` files as virtual databases similar to SQLite. This provides a more natural SQL syntax compared to table functions.

## Motivation

Currently, users must use table functions for every query:
```sql
SELECT * FROM anndata_scan_obs('file.h5ad') WHERE cell_type = 'T cell';
SELECT * FROM anndata_scan_var('file.h5ad');
```

With ATTACH semantics, the syntax becomes more natural:
```sql
ATTACH 'file.h5ad' AS pbmc (TYPE ANNDATA);
SELECT * FROM pbmc.obs WHERE cell_type = 'T cell';
SELECT * FROM pbmc.var;
DETACH pbmc;
```

This mirrors the familiar SQLite ATTACH syntax that DuckDB users already know.

## SQL Interface

### Basic Usage
```sql
-- Attach an AnnData file
ATTACH 'data.h5ad' AS pbmc (TYPE ANNDATA);

-- Query tables directly
SELECT * FROM pbmc.obs;
SELECT * FROM pbmc.var;
SELECT * FROM pbmc.X;
SELECT * FROM pbmc.obsm_pca;
SELECT * FROM pbmc.layers_raw;
SELECT * FROM pbmc.uns;

-- Detach when done
DETACH pbmc;
```

### Options Support
```sql
-- Specify gene identifier columns
ATTACH 'data.h5ad' AS pbmc (
    TYPE ANNDATA,
    VAR_NAME_COLUMN = 'gene_symbols',
    VAR_ID_COLUMN = 'ensembl_id'
);
```

### Cross-Database Queries
```sql
-- Attach multiple datasets
ATTACH 'healthy.h5ad' AS healthy (TYPE ANNDATA);
ATTACH 'disease.h5ad' AS disease (TYPE ANNDATA);

-- Query across datasets
SELECT 'healthy' AS source, * FROM healthy.obs
UNION ALL
SELECT 'disease' AS source, * FROM disease.obs;
```

## Architecture

### DuckDB Extension Points

DuckDB's StorageExtension interface requires implementing 4 components:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DuckDB Core                                   │
│                                                                      │
│  ATTACH 'file.h5ad' AS pbmc (TYPE ANNDATA)                          │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  AnndataStorageExtension                                      │    │
│  │  - Registered as "ANNDATA" type                              │    │
│  │  - attach() callback creates AnndataCatalog                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  AnndataCatalog                                               │    │
│  │  - Represents the attached database "pbmc"                    │    │
│  │  - Holds file path and H5Reader instance                      │    │
│  │  - Contains AnndataSchemaEntry                                │    │
│  └─────────────────────────────────────────────────────────────┘    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  AnndataSchemaEntry                                           │    │
│  │  - Virtual "main" schema                                      │    │
│  │  - Enumerates tables dynamically from HDF5 structure          │    │
│  │  - Contains AnndataTableEntry instances                       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  AnndataTableEntry                                            │    │
│  │  - Virtual table (obs, var, X, obsm_pca, etc.)               │    │
│  │  - GetScanFunction() delegates to existing table functions    │    │
│  └─────────────────────────────────────────────────────────────┘    │
│         │                                                            │
│         ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  Existing Table Functions                                      │    │
│  │  - AnndataScanner::ObsScan                                    │    │
│  │  - AnndataScanner::VarScan                                    │    │
│  │  - AnndataScanner::XScan                                      │    │
│  │  - etc.                                                        │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Details

#### 1. AnndataStorageExtension

Registers the "ANNDATA" storage type with DuckDB:

```cpp
class AnndataStorageExtension : public StorageExtension {
public:
    AnndataStorageExtension() {
        attach = AnndataAttach;
        create_transaction_manager = AnndataCreateTransactionManager;
    }
};

// Registration in extension Load():
config.storage_extensions["anndata"] = make_uniq<AnndataStorageExtension>();
```

#### 2. AnndataCatalog

Custom catalog representing an attached AnnData file:

- Inherits from `Catalog`
- Stores file path and shared H5Reader instance
- Creates single "main" schema containing all tables
- Implements required virtual methods:
  - `GetSchema()` - Returns the main schema
  - `ScanSchemas()` - Lists available schemas
  - `GetCatalogTransaction()` - Returns read-only transaction

#### 3. AnndataSchemaEntry

Virtual schema containing dynamically discovered tables:

- Inherits from `SchemaEntry` or `CatalogEntry`
- Scans HDF5 structure to enumerate tables
- Creates `AnndataTableEntry` for each discovered table
- Implements:
  - `GetTableFunction()` - Returns table by name
  - `Scan()` - Enumerates all tables

#### 4. AnndataTableEntry

Virtual table entry for each AnnData component:

- Inherits from `TableCatalogEntry`
- Stores table type enum and associated metadata
- Key method: `GetScanFunction()` returns delegate to existing table functions

#### 5. AnndataTransactionManager

Minimal read-only transaction manager:

- Returns `DuckTransactionManager` or simple custom implementation
- All transactions are read-only
- No commit/rollback needed (read-only data)

## Table Discovery

Tables are discovered dynamically by scanning the HDF5 structure:

### Always Present
- `obs` - Cell/observation metadata
- `var` - Gene/variable metadata

### Conditionally Present
- `X` - Expression matrix (if exists)
- `obsm_<name>` - One table per obsm matrix (e.g., `obsm_pca`, `obsm_umap`)
- `varm_<name>` - One table per varm matrix
- `layers_<name>` - One table per layer (e.g., `layers_raw`, `layers_normalized`)
- `obsp_<name>` - One table per obsp pairwise matrix
- `varp_<name>` - One table per varp pairwise matrix
- `uns` - Unstructured data (flattened view)

### Discovery Algorithm
```python
def discover_tables(h5ad_file):
    tables = ['obs', 'var']  # Always present

    if 'X' in h5ad_file:
        tables.append('X')

    for matrix_name in h5ad_file.get('obsm', {}).keys():
        tables.append(f'obsm_{matrix_name}')

    for matrix_name in h5ad_file.get('varm', {}).keys():
        tables.append(f'varm_{matrix_name}')

    for layer_name in h5ad_file.get('layers', {}).keys():
        tables.append(f'layers_{layer_name}')

    for matrix_name in h5ad_file.get('obsp', {}).keys():
        tables.append(f'obsp_{matrix_name}')

    for matrix_name in h5ad_file.get('varp', {}).keys():
        tables.append(f'varp_{matrix_name}')

    if 'uns' in h5ad_file:
        tables.append('uns')

    return tables
```

## Table Function Delegation

The key insight is that `AnndataTableEntry` delegates to existing table functions rather than reimplementing scanning logic:

```cpp
TableFunction AnndataTableEntry::GetScanFunction(ClientContext &context, ...) {
    switch (table_type) {
    case TableType::OBS:
        // Create table function that calls AnndataScanner::ObsScan
        return CreateDelegateFunction(file_path, "obs");

    case TableType::VAR:
        return CreateDelegateFunction(file_path, "var");

    case TableType::X:
        return CreateDelegateFunction(file_path, "X");

    case TableType::OBSM:
        // obsm_<name> tables - pass matrix name as parameter
        return CreateObsmFunction(file_path, matrix_name);

    case TableType::LAYERS:
        return CreateLayersFunction(file_path, layer_name);

    // ... similar for other types
    }
}
```

This approach:
1. Maximizes code reuse
2. Ensures consistency between table functions and ATTACH syntax
3. Automatically benefits from optimizations to underlying functions

## Design Decisions

### 1. Single Schema ("main")
All tables exist in a single "main" schema. This keeps the interface simple:
```sql
SELECT * FROM pbmc.obs;      -- Works
SELECT * FROM pbmc.main.obs; -- Also works
```

### 2. Read-Only Access
The extension is strictly read-only. Any attempt to modify data throws an error:
```sql
INSERT INTO pbmc.obs VALUES (...);
-- Error: AnnData databases are read-only

CREATE TABLE pbmc.custom (...);
-- Error: Cannot create tables in AnnData database
```

### 3. Shared H5Reader
A single H5Reader instance is shared across all tables in an attached database:
- Reduces file handle overhead
- Enables caching optimizations
- Ensures consistent view of data

### 4. Dynamic Table Names
Tables with dynamic names (obsm_*, layers_*, etc.) use underscore separation:
- `obsm_X_pca` (for obsm['X_pca'])
- `layers_raw_counts` (for layers['raw_counts'])

### 5. Backward Compatibility
Existing table functions continue to work unchanged:
```sql
-- Old syntax still works
SELECT * FROM anndata_scan_obs('file.h5ad');

-- New syntax available
ATTACH 'file.h5ad' AS ad (TYPE ANNDATA);
SELECT * FROM ad.obs;
```

## Files to Create

### New Source Files
```
src/
├── anndata_storage.cpp         # StorageExtension registration
├── anndata_catalog.cpp         # AnndataCatalog implementation
├── anndata_schema_entry.cpp    # AnndataSchemaEntry implementation
├── anndata_table_entry.cpp     # AnndataTableEntry implementation
├── anndata_transaction.cpp     # AnndataTransactionManager
└── include/
    ├── anndata_storage.hpp
    ├── anndata_catalog.hpp
    ├── anndata_schema_entry.hpp
    ├── anndata_table_entry.hpp
    └── anndata_transaction.hpp
```

### Modified Files
- `src/anndata_extension.cpp` - Register storage extension in `Load()`
- `CMakeLists.txt` - Add new source files to build

## Implementation Phases

### Phase 1: Basic Infrastructure
1. Create `AnndataTransactionManager` (minimal, read-only)
2. Create `AnndataStorageExtension` with attach callback
3. Create minimal `AnndataCatalog` skeleton
4. Register storage extension type
5. Verify basic ATTACH/DETACH works

### Phase 2: Schema and Table Enumeration
1. Implement table discovery from HDF5 structure
2. Create `AnndataSchemaEntry` with table listing
3. Create `AnndataTableEntry` wrapper
4. Verify table names are visible after ATTACH

### Phase 3: Table Function Delegation
1. Implement `GetScanFunction()` delegation
2. Test queries against each table type
3. Ensure column schemas match table functions

### Phase 4: Options Support
1. Parse ATTACH options (VAR_NAME_COLUMN, etc.)
2. Pass options to underlying table functions
3. Add documentation for options

## Testing Plan

### Unit Tests
```sql
-- test/sql/attach_basic.test
require anndata

# Basic ATTACH and DETACH
statement ok
ATTACH 'test/data/sample.h5ad' AS ad (TYPE ANNDATA);

statement ok
DETACH ad;

# Query obs table
statement ok
ATTACH 'test/data/sample.h5ad' AS ad (TYPE ANNDATA);

query I
SELECT COUNT(*) FROM ad.obs;
----
100

statement ok
DETACH ad;
```

### Integration Tests
1. Query each table type (obs, var, X, obsm_*, layers_*, uns)
2. Multiple simultaneous attachments
3. UNION queries across attached databases
4. Error handling (invalid file, missing components)
5. Options passing (VAR_NAME_COLUMN, etc.)

### Error Cases
```sql
-- Invalid file
statement error
ATTACH 'nonexistent.h5ad' AS ad (TYPE ANNDATA);
----
not a valid AnnData file

-- Write attempt
statement ok
ATTACH 'test/data/sample.h5ad' AS ad (TYPE ANNDATA);

statement error
INSERT INTO ad.obs VALUES ('cell1', 'T cell');
----
AnnData databases are read-only

statement ok
DETACH ad;
```

## Performance Considerations

### File Handle Management
- Single file handle per attachment
- Lazy table enumeration (don't scan HDF5 until needed)
- Cache schema information after first query

### Query Optimization
- Push predicates to underlying table functions
- Support parallel scanning where possible
- Reuse existing optimization in table functions

## Future Extensions

### Writing Support
Future versions could support writing:
```sql
-- Copy to new file
CREATE TABLE 'output.h5ad' AS SELECT * FROM pbmc.obs WHERE cell_type = 'T cell';
```

### Virtual Table Modifications
Allow adding computed columns:
```sql
ATTACH 'data.h5ad' AS ad (TYPE ANNDATA);
ALTER TABLE ad.obs ADD COLUMN is_tcell AS (cell_type = 'T cell');
```

### Schema Customization
Allow users to specify which tables to expose:
```sql
ATTACH 'data.h5ad' AS ad (TYPE ANNDATA, TABLES = ['obs', 'var', 'X']);
```
