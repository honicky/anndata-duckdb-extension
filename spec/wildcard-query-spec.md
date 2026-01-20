# Wildcard Query Support Specification

## Overview

This specification outlines the implementation of wildcard (glob) pattern support for querying multiple AnnData files simultaneously. Users will be able to use patterns like `*.h5ad` or `samples/*.h5ad` to query across multiple files with automatic schema harmonization.

## Goals

1. **Glob Pattern Support**: Enable wildcard patterns in file paths for all `anndata_scan_*` functions
2. **Schema Harmonization**: Support both INTERSECTION (default) and UNION modes
3. **Source Tracking**: Add `_file_name` column to identify which file each row came from
4. **Local & Remote Support**: Work with local files, S3, HTTP/HTTPS, and GCS paths
5. **Performance**: Projection pushdown critical for X/layers with tens of thousands of columns

## SQL Interface

### Basic Usage

```sql
-- Query all .h5ad files in a directory (intersection mode - default)
SELECT * FROM anndata_scan_obs('data/*.h5ad');

-- Explicit intersection mode
SELECT * FROM anndata_scan_obs('data/*.h5ad', schema_mode := 'intersection');

-- Union mode - all columns, NULL for missing
SELECT * FROM anndata_scan_obs('data/*.h5ad', schema_mode := 'union');

-- Multiple patterns
SELECT * FROM anndata_scan_obs(['samples_a/*.h5ad', 'samples_b/*.h5ad']);

-- Remote files with wildcards (S3)
SELECT * FROM anndata_scan_obs('s3://bucket/project/*.h5ad');
```

### All Scanner Functions

Support wildcards in all existing table functions:

```sql
anndata_scan_obs(file_pattern, schema_mode := 'intersection')
anndata_scan_var(file_pattern, schema_mode := 'intersection')
anndata_scan_x(file_pattern, [var_name_column], schema_mode := 'intersection')
anndata_scan_obsm(file_pattern, matrix_name, schema_mode := 'intersection')
anndata_scan_varm(file_pattern, matrix_name, schema_mode := 'intersection')
anndata_scan_obsp(file_pattern, matrix_name)  -- Special handling required
anndata_scan_varp(file_pattern, matrix_name)  -- Special handling required
anndata_scan_layers(file_pattern, layer_name, [var_name_column], schema_mode := 'intersection')
anndata_scan_uns(file_pattern)
```

### Output Schema

All multi-file queries will prepend a `_file_name` column:

```sql
SELECT * FROM anndata_scan_obs('data/*.h5ad') LIMIT 3;
-- Returns:
-- _file_name    | obs_idx | cell_type | ...
-- sample_a.h5ad | 0       | T cell    | ...
-- sample_a.h5ad | 1       | B cell    | ...
-- sample_b.h5ad | 0       | Monocyte  | ...
```

## Schema Harmonization Modes

### Intersection Mode (Default)

- Include ONLY columns/genes present in ALL files
- Ensures consistent schema across all results
- Best for batch processing and reproducible analysis
- Fails if no common columns exist

```
File A: obs_idx, cell_type, sample_id
File B: obs_idx, cell_type, batch

Intersection schema: _file_name, obs_idx, cell_type
```

**Rationale for Default**: Intersection is safer for analysis pipelines because:
- Guarantees no NULL values from missing columns
- Ensures consistent feature space for X matrix (critical for ML)
- Matches typical scanpy/anndata concatenation behavior
- Prevents silent data quality issues

### Union Mode

- Include ALL columns found across ALL files
- Columns missing from a file are filled with NULL
- Schema discovery reads metadata from all files before scanning
- Best for exploratory analysis

```
File A: obs_idx, cell_type, sample_id
File B: obs_idx, cell_type, batch

Union schema: _file_name, obs_idx, cell_type, sample_id, batch
(sample_id NULL for File B rows, batch NULL for File A rows)
```

## Performance: Projection Pushdown for X and Layers

### The Challenge

X matrix and layers can have **tens of thousands of columns** (genes). Without projection pushdown:
- Schema discovery would read all column metadata from all files
- Scanning would read all genes even when query only needs a few
- Memory usage would explode for multi-file queries

### Solution: Lazy Schema + Projection Pushdown

```cpp
struct AnndataBindData {
    // For X/layers: don't compute full intersection until we know projected columns
    bool defer_schema_computation;

    // Projected columns from query (set by DuckDB optimizer)
    vector<column_t> projected_column_ids;

    // Only compute intersection for projected columns
    vector<string> projected_var_names;  // e.g., ["CD3D", "CD4", "CD8A"]
};
```

### Implementation Strategy

**Phase 1: Bind Time**
1. Expand glob pattern to file list
2. For obs/var: compute full intersection schema (small - hundreds of columns max)
3. For X/layers: return placeholder schema with `_file_name`, `obs_idx`, `var_idx`, `var_name`, `value`
4. Defer gene-level intersection until projection is known

**Phase 2: Init Time (after projection pushdown)**
1. Receive projected column IDs from DuckDB
2. For X/layers with `var_name IN (...)` filter:
   - Extract var_names from filter
   - Compute intersection only for those genes
   - Skip files missing any required genes (intersection mode)
3. Build per-file var_idx mappings

**Phase 3: Scan Time**
1. Read only projected genes from each file
2. Use existing sparse matrix optimizations
3. Map var_idx to common intersection indices

### Example: Efficient Gene Query

```sql
-- Only reads CD3D, CD4, CD8A from each file
SELECT _file_name, obs_idx, var_name, value
FROM anndata_scan_x('samples/*.h5ad', 'gene_symbols')
WHERE var_name IN ('CD3D', 'CD4', 'CD8A');
```

**Execution Flow**:
1. Bind: Glob expands to 10 files, defer X schema
2. Optimizer: Pushes `var_name IN (...)` predicate
3. Init: Check each file for CD3D, CD4, CD8A
   - If intersection mode: skip files missing any gene
   - Build var_idx mapping for each file
4. Scan: Read only 3 genes per file using sparse optimization

### Configuration

```sql
-- Force eager schema computation (useful for debugging)
SET anndata_eager_schema = false;  -- default

-- Maximum genes to include in automatic intersection
SET anndata_max_intersection_genes = 50000;  -- default
```

## Pairwise Matrices (obsp/varp): Special Handling

### The Problem

obsp and varp store pairwise relationships between observations or variables:
- `obsp['connectivities']` - cell-cell connectivity graph
- `obsp['distances']` - cell-cell distance matrix

When merging files:
- **Cell IDs differ between files** - pairs are file-specific
- **Cannot intersect pairs** - (cell_A_file1, cell_B_file1) ≠ (cell_A_file2, cell_B_file2)
- **Union creates sparse chaos** - most pairs would be NULL

### Solution: File-Scoped Pairs Only

For obsp/varp, pairs are **always file-scoped**:

```sql
-- Each file's pairs returned separately, distinguished by _file_name
SELECT * FROM anndata_scan_obsp('samples/*.h5ad', 'connectivities');
-- Returns:
-- _file_name    | obs_idx_i | obs_idx_j | value
-- sample_a.h5ad | 0         | 5         | 0.95
-- sample_a.h5ad | 0         | 12        | 0.87
-- sample_b.h5ad | 0         | 3         | 0.92  -- Different cells!
```

### Behavior Specification

1. **No schema_mode parameter** for obsp/varp - intersection/union doesn't apply
2. **All files must have the named matrix** - error if missing
3. **Pairs are file-local** - obs_idx/var_idx only meaningful within same _file_name
4. **Joining requires file matching**:

```sql
-- Correct: join within same file
SELECT p.*, o1.cell_type as type_i, o2.cell_type as type_j
FROM anndata_scan_obsp('samples/*.h5ad', 'connectivities') p
JOIN anndata_scan_obs('samples/*.h5ad') o1
    ON p._file_name = o1._file_name AND p.obs_idx_i = o1.obs_idx
JOIN anndata_scan_obs('samples/*.h5ad') o2
    ON p._file_name = o2._file_name AND p.obs_idx_j = o2.obs_idx;
```

### Error Handling

```
Error: Matrix 'connectivities' not found in file 'sample_c.h5ad'
Files matched: sample_a.h5ad, sample_b.h5ad, sample_c.h5ad
Hint: All files must contain the obsp matrix when using wildcards
```

## Implementation Architecture

### Phase 1: File Discovery & Glob Expansion

**New Module**: `src/glob_handler.cpp`

```cpp
struct GlobResult {
    vector<string> matched_files;
    bool is_remote;
    RemoteScheme scheme;  // LOCAL, S3, HTTPS, HTTP, GCS
};

// Expand glob pattern to list of files
GlobResult ExpandGlobPattern(ClientContext &context, const string &pattern);

// Support for multiple patterns
GlobResult ExpandGlobPatterns(ClientContext &context, const vector<string> &patterns);
```

**Local File Discovery**:
- Use DuckDB's `FileSystem::Glob()` API for consistency
- Handle both absolute and relative paths
- Support `*`, `**`, `?` patterns

**Remote File Discovery**:
- S3: Use ListObjectsV2 API with prefix matching
- HTTP/HTTPS: Not directly supported (explicit file list required)
- GCS: Use storage API for listing

### Phase 2: Multi-File Bind Data

**Modified**: `src/include/anndata_scanner.hpp`

```cpp
struct AnndataBindData : public TableFunctionData {
    // Single file (backward compatibility)
    string file_path;

    // Multi-file support (new)
    bool is_multi_file;
    vector<string> file_paths;          // Expanded from glob
    string original_pattern;            // Original glob pattern
    SchemaMode schema_mode;             // UNION or INTERSECTION

    // Schema (intersection or union)
    vector<string> result_column_names;
    vector<LogicalType> result_column_types;

    // Per-file column mappings (result_col_idx -> file_col_idx, -1 if missing)
    vector<vector<int>> file_column_mappings;

    // For X/layers: deferred schema for projection pushdown
    bool defer_var_intersection;
    vector<string> projected_var_names;  // Set after filter pushdown

    // Other existing fields...
};

enum class SchemaMode {
    INTERSECTION,  // Default
    UNION
};
```

### Phase 3: Schema Discovery with Projection Awareness

**New Function**: `ComputeSchema()`

```cpp
struct SchemaInfo {
    vector<string> column_names;
    vector<LogicalType> column_types;
};

// Get schema from a single file
SchemaInfo GetFileSchema(ClientContext &context, const string &path, ScanType type);

// Compute intersection of schemas (default)
SchemaInfo ComputeIntersectionSchema(const vector<SchemaInfo> &file_schemas);

// Compute union of schemas
SchemaInfo ComputeUnionSchema(const vector<SchemaInfo> &file_schemas);

// For X/layers: compute var intersection for specific genes only
struct VarIntersection {
    vector<string> common_var_names;
    vector<vector<idx_t>> per_file_var_indices;  // Mapping to file-local indices
};

VarIntersection ComputeVarIntersection(
    const vector<string> &file_paths,
    const vector<string> &projected_var_names,  // Empty = all genes
    SchemaMode mode
);
```

### Phase 4: Multi-File Scanner State

**Modified**: `src/anndata_scanner.cpp`

```cpp
struct AnndataGlobalState : public GlobalTableFunctionState {
    // Multi-file tracking
    idx_t current_file_idx;
    idx_t total_files;

    // Current file state
    unique_ptr<H5ReaderMultithreaded> h5_reader;
    idx_t current_row_in_file;
    idx_t rows_in_current_file;

    // Column mapping for current file
    vector<int> current_column_mapping;

    // For X/layers: var_idx mapping for current file
    vector<idx_t> current_var_mapping;  // result_var_idx -> file_var_idx

    // Source tracking
    string current_file_name;

    AnndataGlobalState() : current_file_idx(0), current_row_in_file(0) {}

    // Move to next file, returns false if no more files
    bool AdvanceToNextFile(const AnndataBindData &bind_data, ClientContext &context);
};
```

### Phase 5: Filter Pushdown for X/Layers

**Hook into DuckDB's filter pushdown**:

```cpp
// In table function registration
TableFunction x_func("anndata_scan_x", ...);
x_func.filter_pushdown = true;
x_func.filter_prune = true;

// Filter extraction in InitGlobal
void ExtractVarNameFilter(TableFunctionInitInput &input,
                          AnndataBindData &bind_data) {
    // Look for: WHERE var_name IN ('gene1', 'gene2', ...)
    // Or: WHERE var_name = 'gene1'
    for (auto &filter : input.filters) {
        if (filter.column_name == "var_name") {
            // Extract literal values
            bind_data.projected_var_names = ExtractStringLiterals(filter);
        }
    }
}
```

## File Structure Changes

```
src/
├── anndata_scanner.cpp           # Add multi-file scan logic
├── glob_handler.cpp              # NEW: Glob pattern handling
├── schema_harmonizer.cpp         # NEW: Union/intersection schema computation
├── include/
│   ├── anndata_scanner.hpp       # Extended bind data
│   ├── glob_handler.hpp          # NEW: Glob API
│   └── schema_harmonizer.hpp     # NEW: Schema harmonization API
```

## Configuration Options

```sql
-- Set default schema mode
SET anndata_multi_file_schema_mode = 'intersection';  -- default

-- Control parallel file processing (future)
SET anndata_parallel_file_reads = 4;

-- Force eager schema computation
SET anndata_eager_schema = false;  -- default: lazy for X/layers
```

## Error Handling

### No Files Match Pattern
```
Error: No files matching pattern 'data/*.h5ad' found
```

### No Common Columns (Intersection Mode)
```
Error: No common columns found across files in intersection mode.
Files checked: sample_a.h5ad, sample_b.h5ad
Hint: Use schema_mode := 'union' to include all columns
```

### No Common Genes (Intersection Mode for X)
```
Error: No common genes found across files for intersection.
Gene 'CD3D' missing from: sample_c.h5ad
Files checked: sample_a.h5ad, sample_b.h5ad, sample_c.h5ad
Hint: Use schema_mode := 'union' or filter to genes present in all files
```

### Type Mismatch for Same Column
```
Warning: Column 'batch' has different types across files:
  sample_a.h5ad: INTEGER
  sample_b.h5ad: VARCHAR
Using VARCHAR as common type.
```

### Missing obsp/varp Matrix
```
Error: Matrix 'connectivities' not found in file 'sample_c.h5ad'
Files matched: sample_a.h5ad, sample_b.h5ad, sample_c.h5ad
Hint: All files must contain the obsp/varp matrix when using wildcards
```

## Implementation Phases

### Phase 1: Local Glob Support (Core)
1. Implement `GlobHandler` for local file pattern expansion
2. Modify `AnndataBindData` to support file lists
3. Add `schema_mode` parameter (default: intersection)
4. Implement intersection schema computation for obs/var
5. Add `_file_name` column to output

### Phase 2: Schema Harmonization
1. Implement union mode as alternative
2. Add type coercion for mismatched column types
3. Build per-file column mapping
4. Handle NULL filling for union mode

### Phase 3: X/Layers Projection Pushdown
1. Defer var intersection until filter pushdown
2. Extract var_name predicates from filters
3. Compute var intersection for projected genes only
4. Build per-file var_idx mappings

### Phase 4: obsp/varp Handling
1. Implement file-scoped pair semantics
2. Require matrix presence in all files
3. Document join patterns for cross-file analysis

### Phase 5: Remote Glob Support
1. Implement S3 prefix listing for glob patterns
2. Handle S3 credentials for listing operations
3. Add GCS support (if feasible)
4. Document HTTP limitations (no glob support)

### Phase 6: Testing & Documentation
1. Unit tests for glob expansion
2. Integration tests for schema modes
3. Performance benchmarks with many files/genes
4. User documentation and examples

## Testing Strategy

### Unit Tests

```cpp
// test/unit/test_glob_handler.cpp
TEST_CASE("Local glob expansion") {
    // Test *.h5ad pattern
    // Test **/*.h5ad recursive
    // Test no matches
    // Test single file (no glob)
}

TEST_CASE("Schema harmonization") {
    // Test intersection of identical schemas
    // Test intersection with different columns
    // Test union mode
    // Test type coercion
}

TEST_CASE("Var intersection with projection") {
    // Test gene filter pushdown
    // Test missing gene handling
    // Test large gene list performance
}
```

### SQL Tests

```sql
-- test/sql/multi_file_scan.test

# Basic wildcard query (intersection default)
query I
SELECT COUNT(DISTINCT _file_name) FROM anndata_scan_obs('data/samples/*.h5ad');
----
3

# Intersection mode has common columns only
query I
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM anndata_scan_obs('data/samples/*.h5ad')
);
----
8

# Union mode includes all columns
query I
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM anndata_scan_obs('data/samples/*.h5ad', schema_mode := 'union')
);
----
15

# X query with gene filter - projection pushdown
query I
SELECT COUNT(*) FROM anndata_scan_x('data/samples/*.h5ad', 'gene_symbols')
WHERE var_name IN ('CD3D', 'CD4', 'CD8A');
----
9000

# obsp returns file-scoped pairs
query I
SELECT COUNT(DISTINCT _file_name) FROM anndata_scan_obsp('data/samples/*.h5ad', 'connectivities');
----
3
```

## Compatibility Notes

### Backward Compatibility

- Single file paths continue to work unchanged
- No `_file_name` column added for single-file queries
- Default schema_mode is 'intersection' for safe batch processing

### Breaking Changes

None - this is purely additive functionality.

## Future Enhancements

1. **Hive-style Partitioning**: Extract metadata from path components
   ```sql
   -- data/batch=A/sample.h5ad -> adds batch='A' column
   SELECT * FROM anndata_scan_obs('data/batch=*/sample.h5ad', hive_partitioning := true);
   ```

2. **File Filtering**: Filter files by metadata before full scan
   ```sql
   SELECT * FROM anndata_scan_obs('data/*.h5ad', file_filter := 'n_obs > 1000');
   ```

3. **Parallel File Processing**: Scan multiple files concurrently
   ```sql
   SET anndata_parallel_file_reads = 4;
   ```

4. **Incremental Schema**: Discover schema lazily during scan
   ```sql
   SELECT * FROM anndata_scan_obs('data/*.h5ad', lazy_schema := true);
   ```

## References

- DuckDB Multi-File Support: https://duckdb.org/docs/data/multiple_files/overview
- DuckDB read_parquet glob: https://duckdb.org/docs/data/parquet/overview#multi-file-reads-and-globs
- HDF5 Virtual Dataset (VDS): https://docs.hdfgroup.org/hdf5/develop/group___h5_d.html

## Appendix: Example Queries

### Cross-Sample Analysis

```sql
-- Compare cell type distributions across samples
SELECT
    _file_name as sample,
    cell_type,
    COUNT(*) as n_cells,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY _file_name) as pct
FROM anndata_scan_obs('samples/*.h5ad')
GROUP BY _file_name, cell_type
ORDER BY sample, n_cells DESC;
```

### Efficient Gene Query with Projection Pushdown

```sql
-- Only reads 3 genes from each file (projection pushdown)
SELECT
    _file_name as experiment,
    o.cell_type,
    x.var_name as gene,
    AVG(x.value) as mean_expr
FROM anndata_scan_x('experiments/*.h5ad', 'gene_symbols') x
JOIN anndata_scan_obs('experiments/*.h5ad') o
    ON x._file_name = o._file_name AND x.obs_idx = o.obs_idx
WHERE x.var_name IN ('CD3D', 'CD4', 'CD8A')
GROUP BY _file_name, o.cell_type, x.var_name;
```

### Quality Control Across Datasets

```sql
-- Check QC metrics across all files (intersection of common columns)
SELECT
    _file_name,
    COUNT(*) as n_cells,
    AVG(n_genes_by_counts) as avg_genes,
    AVG(total_counts) as avg_counts
FROM anndata_scan_obs('project/**/*.h5ad')
GROUP BY _file_name
ORDER BY n_cells DESC;
```

### obsp Analysis Within Files

```sql
-- Analyze connectivity patterns per file
SELECT
    p._file_name,
    o1.cell_type as type_i,
    o2.cell_type as type_j,
    AVG(p.value) as avg_connectivity,
    COUNT(*) as n_connections
FROM anndata_scan_obsp('samples/*.h5ad', 'connectivities') p
JOIN anndata_scan_obs('samples/*.h5ad') o1
    ON p._file_name = o1._file_name AND p.obs_idx_i = o1.obs_idx
JOIN anndata_scan_obs('samples/*.h5ad') o2
    ON p._file_name = o2._file_name AND p.obs_idx_j = o2.obs_idx
WHERE p.value > 0.5
GROUP BY p._file_name, o1.cell_type, o2.cell_type
ORDER BY avg_connectivity DESC;
```
