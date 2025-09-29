# AnnData DuckDB Extension API Reference

## SQL Commands

### ATTACH

Attaches an AnnData file to the current DuckDB session.

#### Syntax
```sql
ATTACH 'path' AS alias (TYPE ANNDATA [, options]);
```

#### Parameters
- `path`: Path to the .h5ad file (required)
- `alias`: Database alias for referencing tables (required)
- `TYPE ANNDATA`: Specifies the AnnData extension handler (required)
- `options`: Optional configuration parameters

#### Options
| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `READONLY` | BOOLEAN | true | Open in read-only mode |
| `CACHE_SIZE` | VARCHAR | '100MB' | Memory cache size for data |
| `SPARSE_THRESHOLD` | DOUBLE | 0.1 | Density threshold for sparse optimization |
| `VAR_NAME_COLUMN` | VARCHAR | 'var_names' | Column from var DataFrame to use as gene names in main table |
| `VAR_ID_COLUMN` | VARCHAR | 'var_names' | Column from var DataFrame to use as gene IDs |
| `OBS_NAME_COLUMN` | VARCHAR | 'obs_names' | Column from obs DataFrame to use as cell names |
| `OBS_ID_COLUMN` | VARCHAR | 'obs_names' | Column from obs DataFrame to use as cell IDs |

#### Examples
```sql
-- Basic attach
ATTACH 'data.h5ad' AS scdata (TYPE ANNDATA);

-- With custom gene name column
ATTACH 'data.h5ad' AS scdata (
    TYPE ANNDATA, 
    VAR_NAME_COLUMN='gene_symbols'
);

-- With multiple options
ATTACH 'data.h5ad' AS scdata (
    TYPE ANNDATA, 
    CACHE_SIZE='500MB',
    VAR_NAME_COLUMN='gene_name',
    VAR_ID_COLUMN='ensembl_id',
    OBS_NAME_COLUMN='barcode'
);
```

### DETACH

Detaches an AnnData file from the session.

#### Syntax
```sql
DETACH alias;
```

#### Example
```sql
DETACH scdata;
```

## Table Functions

### Component-Specific Table Functions

Direct table functions for scanning AnnData components without attaching.

#### anndata_scan_obs
```sql
SELECT * FROM anndata_scan_obs('path');
```
Returns observation (cell) metadata with obs_idx and all metadata columns.

#### anndata_scan_var
```sql
SELECT * FROM anndata_scan_var('path');
```
Returns variable (gene) metadata with var_idx and all metadata columns.

#### anndata_scan_x
```sql
SELECT * FROM anndata_scan_x('path' [, var_column]);
```
Returns expression matrix in long format: (obs_idx, var_idx, var_name, value).
Optional var_column parameter specifies which var column to use for var_name.

#### anndata_scan_obsm / anndata_scan_varm
```sql
SELECT * FROM anndata_scan_obsm('path', 'matrix_name');
SELECT * FROM anndata_scan_varm('path', 'matrix_name');
```
Returns dimensional reduction matrices (e.g., 'X_pca', 'X_umap') with obs_idx/var_idx and dim_0, dim_1, ... columns.

#### anndata_scan_obsp / anndata_scan_varp
```sql
SELECT * FROM anndata_scan_obsp('path', 'matrix_name');
SELECT * FROM anndata_scan_varp('path', 'matrix_name');
```
Returns pairwise relationship matrices in sparse format: (obs_idx_1, obs_idx_2, value) or (var_idx_1, var_idx_2, value).

#### anndata_scan_layers
```sql
SELECT * FROM anndata_scan_layers('path', 'layer_name' [, var_column]);
```
Returns alternative expression matrices with same format as anndata_scan_x.

#### anndata_scan_uns
```sql
SELECT * FROM anndata_scan_uns('path');
```
Returns scalar metadata from the uns (unstructured) group.

### anndata_info

Returns metadata about an AnnData file as a table function.

#### Syntax
```sql
SELECT * FROM anndata_info('path');
```

#### Returns
Table with two columns:
| Column | Type | Description |
|--------|------|-------------|
| `property` | VARCHAR | Property name |
| `value` | VARCHAR | Property value |

Properties include:
- `n_obs`: Number of observations
- `n_vars`: Number of variables  
- `obsm_keys`: Available obsm matrices (comma-separated)
- `varm_keys`: Available varm matrices (comma-separated)
- `layers`: Available layers (comma-separated)
- `uns_keys`: Available uns keys (comma-separated)
- `obsp_keys`: Available obsp matrices (comma-separated)
- `varp_keys`: Available varp matrices (comma-separated)

#### Example
```sql
SELECT * FROM anndata_info('data.h5ad');
-- Returns:
-- property    | value
-- ------------|-------
-- n_obs       | 2638
-- n_vars      | 1838
-- obsm_keys   | X_pca, X_umap
-- layers      | raw, normalized
-- ...
```

## Configuration Parameters

### Global Settings

Set using DuckDB's SET command:

```sql
SET parameter = value;
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `anndata_chunk_size` | INTEGER | 10000 | Number of rows to read per chunk |
| `anndata_sparse_optimization` | BOOLEAN | true | Enable sparse matrix optimizations |
| `anndata_cache_size` | VARCHAR | '100MB' | Global cache size for all AnnData files |
| `anndata_parallel_scan` | BOOLEAN | true | Enable parallel scanning |
| `anndata_compression` | VARCHAR | 'auto' | Compression detection (auto/gzip/lzf/none) |
| `anndata_default_var_name_column` | VARCHAR | 'var_names' | Default column for gene names |
| `anndata_default_var_id_column` | VARCHAR | 'var_names' | Default column for gene IDs |
| `anndata_default_obs_name_column` | VARCHAR | 'obs_names' | Default column for cell names |
| `anndata_default_obs_id_column` | VARCHAR | 'obs_names' | Default column for cell IDs |

### Examples
```sql
-- Increase chunk size for large files
SET anndata_chunk_size = 50000;

-- Disable sparse optimization
SET anndata_sparse_optimization = false;

-- Set larger cache
SET anndata_cache_size = '2GB';
```

## Data Type Mappings

### Scalar Types

| AnnData/Python | DuckDB | Notes |
|----------------|--------|-------|
| np.float32 | FLOAT | Automatic conversion |
| np.float64 | DOUBLE | Native precision |
| np.int8 | TINYINT | |
| np.int16 | SMALLINT | |
| np.int32 | INTEGER | |
| np.int64 | BIGINT | |
| np.uint8 | SMALLINT | Promoted to signed |
| np.uint16 | INTEGER | Promoted to signed |
| np.uint32 | BIGINT | Promoted to signed |
| np.uint64 | HUGEINT | If supported |
| bool | BOOLEAN | |
| str | VARCHAR | |
| bytes | BLOB | |
| pd.Categorical | ENUM/VARCHAR | ENUM if cardinality < 256 |

### Complex Types

| AnnData Structure | DuckDB | Notes |
|-------------------|--------|-------|
| Dense Matrix | TABLE | Row-major to columnar |
| Sparse Matrix (CSR/CSC) | TABLE | Only non-zero values |
| DataFrame (obs) | TABLE | Direct mapping, one row per observation |
| DataFrame (var) with arrays | Multiple TABLEs | Scalar cols in main table, arrays in separate tables |
| Dict | STRUCT/JSON | Nested structures as JSON |
| List (fixed length) | LIST | Native list type |
| Array (variable length) | Separate TABLE | Normalized with (id, index, value) |
| Array (2D+) | Separate columns | dim_0, dim_1, etc. |

### Table Structure for Variable-Length Arrays

For columns in `var` containing variable-length arrays:
```sql
-- Main var table
CREATE TABLE var (
    var_id VARCHAR PRIMARY KEY,
    gene_name VARCHAR,
    n_cells INTEGER,
    -- array columns stored separately or as JSON
);

-- Array column table
CREATE TABLE var_highly_variable_rank (
    var_id VARCHAR,
    index INTEGER,
    value DOUBLE,
    PRIMARY KEY (var_id, index)
);
```

## Error Codes

| Code | Name | Description |
|------|------|-------------|
| A001 | FILE_NOT_FOUND | AnnData file does not exist |
| A002 | INVALID_FORMAT | File is not a valid .h5ad file |
| A003 | CORRUPTED_DATA | Data corruption detected |
| A004 | UNSUPPORTED_VERSION | AnnData version not supported |
| A005 | COMPONENT_NOT_FOUND | Requested component doesn't exist |
| A006 | TYPE_CONVERSION_ERROR | Cannot convert data type |
| A007 | MEMORY_ERROR | Insufficient memory for operation |
| A008 | HDF5_ERROR | HDF5 library error |
| A009 | PERMISSION_DENIED | No read permission for file |
| A010 | ALREADY_ATTACHED | Alias already in use |

## Performance Hints

### Query Optimization

```sql
-- Use column pruning
SELECT obs_id, value FROM scdata.main 
WHERE var_id = 'GENE1';  -- Faster than SELECT *

-- Filter early
SELECT * FROM scdata.obs 
WHERE cell_type = 'T cell'  -- Push down predicates
LIMIT 1000;

-- Use appropriate chunk size for your data
SET anndata_chunk_size = 25000;  -- For large dense matrices
SET anndata_chunk_size = 5000;   -- For sparse matrices
```

### Memory Management

```sql
-- Monitor memory usage
SELECT * FROM duckdb_memory();

-- Clear cache
CALL anndata_clear_cache();

-- Set memory limits
SET memory_limit = '4GB';
SET anndata_cache_size = '1GB';
```

## Utility Functions

### anndata_validate

Validates an AnnData file without loading data.

```sql
SELECT anndata_validate('path/to/file.h5ad');
```

Returns:
- `valid`: BOOLEAN
- `message`: VARCHAR (error details if invalid)

### anndata_list_components

Lists all available components in an AnnData file.

```sql
SELECT * FROM anndata_list_components('data.h5ad');
```

Returns:
- `component`: VARCHAR (component path)
- `type`: VARCHAR (data type)
- `shape`: VARCHAR (dimensions)
- `size_mb`: DOUBLE (size in megabytes)

## Pragma Commands

### PRAGMA anndata_version

Returns the extension version.

```sql
PRAGMA anndata_version;
```

### PRAGMA anndata_stats

Shows statistics for attached AnnData files.

```sql
PRAGMA anndata_stats;
```

Returns:
- `alias`: VARCHAR
- `file_path`: VARCHAR
- `cache_hits`: BIGINT
- `cache_misses`: BIGINT
- `bytes_read`: BIGINT
- `read_time_ms`: DOUBLE

## Advanced Features

### Virtual Tables

Each attached AnnData file creates virtual tables that can be queried:

```sql
-- After attaching
ATTACH 'data.h5ad' AS sc (TYPE ANNDATA);

-- Available tables
SHOW TABLES FROM sc;
-- Returns: main, obs, var, obsm_pca, obsm_umap, layers_raw, etc.

-- Table schema
DESCRIBE sc.obs;
DESCRIBE sc.main;
```

### Sparse Matrix Handling

The extension automatically detects and optimizes sparse matrices:

```sql
-- Sparse matrices are automatically handled
SELECT COUNT(*) FROM sc.main WHERE value != 0;  -- Efficient for sparse

-- Force dense reading
SET anndata_sparse_optimization = false;
SELECT * FROM sc.main;  -- Reads zeros explicitly
```

### Parallel Scanning

Enable parallel scanning for better performance:

```sql
SET threads = 4;
SET anndata_parallel_scan = true;

-- Parallel scan automatically applied
SELECT * FROM sc.main;
```