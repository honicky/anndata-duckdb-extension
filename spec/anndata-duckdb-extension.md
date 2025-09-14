# AnnData DuckDB Extension Specification

## Overview

The AnnData DuckDB extension provides read-only access to AnnData (.h5ad) files within DuckDB, following a similar user experience to the SQLite extension. AnnData is the standard data format for single-cell genomics data, storing annotated data matrices along with observations (cells) and variables (genes) metadata.

## Design Goals

1. **Familiar Interface**: Mirror the SQLite extension's attach/detach paradigm
2. **Read-Only Access**: Initial implementation focuses on reading AnnData files
3. **Efficient Data Access**: Leverage DuckDB's columnar storage and query optimization
4. **Zero-Copy Where Possible**: Minimize memory overhead when reading from HDF5
5. **Schema Preservation**: Maintain AnnData's structure and data types

## Architecture

### Extension Components

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

### Data Model Mapping

AnnData structure maps to DuckDB tables as follows:

| AnnData Component | DuckDB Representation | Description |
|------------------|----------------------|-------------|
| `X` (main matrix) | `main` table | Primary expression matrix |
| `obs` (observations) | `obs` table | Cell metadata (one row per observation) |
| `var` (variables) | Multiple tables - see below | Gene metadata with variable-length arrays |
| `obsm` (obs matrices) | Multiple tables: `obsm_<key>` | Dimensional reductions with varying dimensions |
| `varm` (var matrices) | Multiple tables: `varm_<key>` | Variable-level matrices with varying dimensions |
| `layers` | Multiple tables: `layers_<key>` | Alternative expression matrices |
| `uns` (unstructured) | Multiple approaches - see below | Unstructured metadata with arbitrary nesting |

#### Handling var with Variable-Length Arrays

The `var` DataFrame often contains columns with variable-length arrays (e.g., from highly variable gene selection, gene scores across different conditions). These are decomposed into:

1. **Scalar columns** → `var` table (main gene metadata)
2. **Array columns** → Separate tables: `var_<column>` with (var_id, index, value) structure
3. **Complex/nested columns** → Stored as JSON in `var` table

Example decomposition:
```
var DataFrame with columns:
- gene_id (string) → var.gene_id
- gene_name (string) → var.gene_name
- n_cells (int) → var.n_cells
- highly_variable_rank (float[]) → var_highly_variable_rank table
- dispersions (float[]) → var_dispersions table
```

#### Handling Variable Dimensions in obsm/varm

Each matrix in `obsm`/`varm` becomes a separate table with its specific dimensions:

```
obsm['X_pca'] (n_obs × 50) → obsm_pca table with columns: obs_id, dim_0...dim_49
obsm['X_umap'] (n_obs × 2) → obsm_umap table with columns: obs_id, dim_0, dim_1
varm['PCs'] (n_vars × 50) → varm_pcs table with columns: var_id, dim_0...dim_49
varm['gene_loadings'] (n_vars × 10) → varm_gene_loadings table with columns: var_id, dim_0...dim_9
```

#### Handling Unstructured Data (uns)

The `uns` dictionary contains arbitrary nested data structures. This is handled through:

1. **Simple key-value pairs** → `uns_scalar` table for primitive values
2. **DataFrames/Tables** → Separate tables: `uns_<key>` for each DataFrame
3. **Arrays/Lists** → Separate tables: `uns_<key>` with (index, value) structure  
4. **Complex nested structures** → `uns_json` table with JSON storage

Example decomposition:
```
uns = {
    'pca': {'variance': [0.1, 0.05, ...], 'params': {...}},
    'neighbors': {'connectivities': sparse_matrix, 'params': {...}},
    'leiden': {'params': {'resolution': 1.0}},
    'rank_genes_groups': DataFrame
}

Becomes:
- uns_scalar: simple parameters (leiden.params.resolution)
- uns_pca_variance: array table
- uns_neighbors_connectivities: sparse matrix table
- uns_rank_genes_groups: DataFrame as table
- uns_json: complex nested params as JSON
```

## User Interface

### Attaching AnnData Files

```sql
-- Attach an AnnData file with alias
ATTACH 'path/to/data.h5ad' AS mydata (TYPE ANNDATA);

-- Attach with read-only mode (default)
ATTACH 'path/to/data.h5ad' AS mydata (TYPE ANNDATA, READONLY);

-- Attach with custom variable name column
ATTACH 'path/to/data.h5ad' AS mydata (
    TYPE ANNDATA, 
    VAR_NAME_COLUMN='gene_symbols'  -- Use gene_symbols instead of var_names
);

-- Attach with multiple configuration options
ATTACH 'path/to/data.h5ad' AS mydata (
    TYPE ANNDATA,
    VAR_NAME_COLUMN='gene_name',     -- Column to use for variable names
    OBS_NAME_COLUMN='cell_barcode',  -- Column to use for observation names
    VAR_ID_COLUMN='ensembl_id'       -- Column to use as primary var identifier
);

-- List available tables in attached AnnData
SHOW TABLES FROM mydata;
```

### Querying Data

```sql
-- Query main expression matrix
SELECT * FROM mydata.main LIMIT 10;

-- Join observation metadata with expression data
SELECT 
    o.cell_type,
    o.sample_id,
    m.*
FROM mydata.obs o
JOIN mydata.main m ON o.obs_id = m.obs_id
WHERE o.cell_type = 'T cell';

-- Access dimensional reduction data
SELECT * FROM mydata.obsm_pca LIMIT 100;
SELECT * FROM mydata.obsm_umap;

-- Query gene information
SELECT * FROM mydata.var 
WHERE gene_name LIKE 'CD%';

-- Access layers (alternative expression matrices)
SELECT * FROM mydata.layers_raw_counts;
SELECT * FROM mydata.layers_normalized;
```

### Detaching

```sql
-- Detach the AnnData file
DETACH mydata;
```

## Schema Details

### Main Expression Matrix (`main`)

| Column | Type | Description |
|--------|------|-------------|
| `obs_id` | VARCHAR | Observation (cell) identifier |
| `var_id` | VARCHAR | Variable (gene) identifier (configurable via VAR_ID_COLUMN) |
| `var_name` | VARCHAR | Human-readable variable name (configurable via VAR_NAME_COLUMN) |
| `value` | DOUBLE | Expression value |

For sparse matrices, only non-zero values are stored.

Note: Both `var_id` and `var_name` are included to allow querying by either identifier. The actual columns used are configurable at attach time.

### Observations Table (`obs`)

| Column | Type | Description |
|--------|------|-------------|
| `obs_id` | VARCHAR | Primary key, cell identifier |
| `<metadata_columns>` | VARIES | User-defined metadata columns |

Types are automatically inferred from the AnnData file.

### Variables Table (`var`)

The main `var` table contains only scalar columns:

| Column | Type | Description |
|--------|------|-------------|
| `var_id` | VARCHAR | Primary key, gene identifier |
| `<scalar_columns>` | VARIES | Scalar metadata columns (strings, numbers, booleans) |
| `<array_columns>` | JSON | Complex/nested data stored as JSON |

Variable-length array columns are stored in separate tables:

### Variable Array Tables (`var_<column_name>`)

For each array column in the original var DataFrame:

| Column | Type | Description |
|--------|------|-------------|
| `var_id` | VARCHAR | Foreign key to var table |
| `index` | INTEGER | Array index (0-based) |
| `value` | VARIES | Array element value |

Example: `var_highly_variable_rank` table for highly_variable_rank array column.

### Dimensional Reduction Tables (`obsm_*`, `varm_*`)

| Column | Type | Description |
|--------|------|-------------|
| `obs_id` or `var_id` | VARCHAR | Reference to observation or variable |
| `dim_0`, `dim_1`, ... | DOUBLE | Dimensional values |

### Configuration Options

#### Attach-Time Options

These options are specified when attaching an AnnData file:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `VAR_NAME_COLUMN` | VARCHAR | 'var_names' | Column from var to use as human-readable gene names |
| `VAR_ID_COLUMN` | VARCHAR | 'var_names' | Column from var to use as primary gene identifier |
| `OBS_NAME_COLUMN` | VARCHAR | 'obs_names' | Column from obs to use as cell names |
| `OBS_ID_COLUMN` | VARCHAR | 'obs_names' | Column from obs to use as primary cell identifier |
| `SPARSE_THRESHOLD` | DOUBLE | 0.1 | Density threshold for sparse optimization |
| `CACHE_SIZE` | VARCHAR | '100MB' | Memory cache size for this file |

#### Global Settings

```sql
-- Set chunk size for reading large matrices
SET anndata_chunk_size = 10000;

-- Enable/disable sparse matrix optimization
SET anndata_sparse_optimization = true;

-- Set memory limit for caching
SET anndata_cache_size = '1GB';

-- Set default var name column for new attachments
SET anndata_default_var_name_column = 'gene_symbols';

-- Set default obs name column for new attachments  
SET anndata_default_obs_name_column = 'cell_barcode';
```

## Implementation Considerations

### Performance Optimizations

1. **Lazy Loading**: Data is read on-demand from the HDF5 file
2. **Chunk Reading**: Large matrices are read in configurable chunks
3. **Sparse Matrix Support**: Efficient handling of sparse matrices using CSR/CSC formats
4. **Parallel Scanning**: Support for parallel table scans where possible
5. **Pushdown Filters**: WHERE clause predicates pushed to HDF5 reading

### Type Mapping

| AnnData/NumPy Type | DuckDB Type |
|-------------------|-------------|
| float32, float64 | DOUBLE |
| int8, int16, int32, int64 | BIGINT |
| uint8, uint16, uint32 | BIGINT |
| bool | BOOLEAN |
| string, object | VARCHAR |
| categorical | ENUM or VARCHAR |

### Limitations (Initial Release)

1. Read-only access (no modifications to .h5ad files)
2. No support for writing new AnnData files
3. Limited support for complex `uns` structures
4. No support for backed mode (all data loaded to memory on query)

## Error Handling

```sql
-- File not found
ATTACH 'nonexistent.h5ad' AS data (TYPE ANNDATA);
-- Error: Could not open AnnData file: nonexistent.h5ad

-- Invalid AnnData format
ATTACH 'invalid.h5' AS data (TYPE ANNDATA);
-- Error: File is not a valid AnnData (h5ad) file

-- Corrupted data
SELECT * FROM data.main;
-- Error: Could not read matrix 'X' from AnnData file
```

## Future Enhancements

### Phase 2: Advanced Features
- Support for backed mode (memory-mapped access)
- Query optimization for common single-cell workflows
- Integration with DuckDB's Arrow support
- Custom aggregate functions for single-cell analysis

### Phase 3: Write Support
- CREATE ANNDATA command
- INSERT/UPDATE operations
- Export to .h5ad format
- Transaction support

### Phase 4: Advanced Analytics
- Built-in single-cell analysis functions
- Integration with scanpy-like operations
- Automatic indexing for common query patterns

## Dependencies

- **HDF5**: For reading .h5ad files
- **DuckDB Extension API**: Core extension framework
- **Optional**: Arrow for zero-copy data transfer

## Testing Strategy

1. **Unit Tests**: Test individual components (schema discovery, type conversion)
2. **Integration Tests**: Test full workflow with sample AnnData files
3. **Performance Tests**: Benchmark against direct HDF5 reading
4. **Compatibility Tests**: Test with various AnnData versions and formats

## Security Considerations

- File access controlled by filesystem permissions
- No arbitrary code execution
- Validate file format before processing
- Respect DuckDB's access control when integrated

## References

- [AnnData Documentation](https://anndata.readthedocs.io/)
- [DuckDB Extension API](https://duckdb.org/docs/api/c/extension)
- [HDF5 Format](https://www.hdfgroup.org/solutions/hdf5/)
- [DuckDB SQLite Extension](https://duckdb.org/docs/extensions/sqlite)