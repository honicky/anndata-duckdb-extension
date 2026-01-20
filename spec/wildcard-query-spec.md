# Wildcard Query Support Specification

## Overview

This specification outlines the implementation of wildcard (glob) pattern support for querying multiple AnnData files simultaneously. Users will be able to use patterns like `*.h5ad` or `samples/*.h5ad` to query across multiple files with automatic schema harmonization.

## Goals

1. **Glob Pattern Support**: Enable wildcard patterns in file paths for all `anndata_scan_*` functions
2. **Schema Harmonization**: Support both UNION (all columns) and INTERSECTION (common columns) modes
3. **Source Tracking**: Add `_file_name` column to identify which file each row came from
4. **Local & Remote Support**: Work with local files, S3, HTTP/HTTPS, and GCS paths
5. **Performance**: Lazy file discovery and streaming reads to handle large file sets

## SQL Interface

### Basic Usage

```sql
-- Query all .h5ad files in a directory (union mode - default)
SELECT * FROM anndata_scan_obs('data/*.h5ad');

-- Intersection mode - only columns present in ALL files
SELECT * FROM anndata_scan_obs('data/*.h5ad', schema_mode := 'intersection');

-- Explicit union mode
SELECT * FROM anndata_scan_obs('data/*.h5ad', schema_mode := 'union');

-- Multiple patterns with UNION
SELECT * FROM anndata_scan_obs(['samples_a/*.h5ad', 'samples_b/*.h5ad']);

-- Remote files with wildcards (S3)
SELECT * FROM anndata_scan_obs('s3://bucket/project/*.h5ad');
```

### All Scanner Functions

Support wildcards in all existing table functions:

```sql
anndata_scan_obs(file_pattern, schema_mode := 'union')
anndata_scan_var(file_pattern, schema_mode := 'union')
anndata_scan_x(file_pattern, [var_name_column], schema_mode := 'union')
anndata_scan_obsm(file_pattern, matrix_name, schema_mode := 'union')
anndata_scan_varm(file_pattern, matrix_name, schema_mode := 'union')
anndata_scan_obsp(file_pattern, matrix_name)
anndata_scan_varp(file_pattern, matrix_name)
anndata_scan_layers(file_pattern, layer_name, [var_name_column], schema_mode := 'union')
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

### Union Mode (Default)

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

### Intersection Mode

- Include ONLY columns present in ALL files
- Strict mode - fails if no common columns exist
- Best for consistent batch processing

```
File A: obs_idx, cell_type, sample_id
File B: obs_idx, cell_type, batch

Intersection schema: _file_name, obs_idx, cell_type
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

    // Union schema (computed from all files)
    vector<string> union_column_names;
    vector<LogicalType> union_column_types;

    // Per-file column mappings (union_col_idx -> file_col_idx, -1 if missing)
    vector<vector<int>> file_column_mappings;

    // Other existing fields...
};

enum class SchemaMode {
    UNION,
    INTERSECTION
};
```

### Phase 3: Schema Discovery

**New Function**: `ComputeUnionSchema()`

```cpp
struct SchemaInfo {
    vector<string> column_names;
    vector<LogicalType> column_types;
};

// Get schema from a single file
SchemaInfo GetFileSchema(ClientContext &context, const string &path, ScanType type);

// Compute union of schemas from multiple files
SchemaInfo ComputeUnionSchema(const vector<SchemaInfo> &file_schemas);

// Compute intersection of schemas
SchemaInfo ComputeIntersectionSchema(const vector<SchemaInfo> &file_schemas);

// Build column mapping for a file against union schema
vector<int> BuildColumnMapping(const SchemaInfo &file_schema,
                               const SchemaInfo &union_schema);
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

    // Source tracking
    string current_file_name;

    AnndataGlobalState() : current_file_idx(0), current_row_in_file(0) {}

    // Move to next file, returns false if no more files
    bool AdvanceToNextFile(const AnndataBindData &bind_data, ClientContext &context);
};
```

### Phase 5: Scan Implementation Changes

**Modified Scan Loop**:

```cpp
void AnndataScanner::MultiFileScan(ClientContext &context,
                                   TableFunctionInput &input,
                                   DataChunk &output) {
    auto &bind_data = input.bind_data->Cast<AnndataBindData>();
    auto &global_state = input.global_state->Cast<AnndataGlobalState>();

    while (output.size() < STANDARD_VECTOR_SIZE) {
        // Check if current file exhausted
        if (global_state.current_row_in_file >= global_state.rows_in_current_file) {
            if (!global_state.AdvanceToNextFile(bind_data, context)) {
                break;  // No more files
            }
        }

        // Read chunk from current file
        idx_t rows_to_read = MinValue(
            STANDARD_VECTOR_SIZE - output.size(),
            global_state.rows_in_current_file - global_state.current_row_in_file
        );

        // Read into temporary chunk
        DataChunk file_chunk;
        ReadFromCurrentFile(global_state, file_chunk, rows_to_read);

        // Map columns to union schema and append
        MapAndAppendChunk(bind_data, global_state, file_chunk, output);

        global_state.current_row_in_file += rows_to_read;
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
SET anndata_multi_file_schema_mode = 'union';  -- or 'intersection'

-- Control parallel file processing (future)
SET anndata_parallel_file_reads = 4;

-- Fail fast on schema mismatch (intersection mode)
SET anndata_strict_schema = true;
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

### Type Mismatch for Same Column
```
Warning: Column 'batch' has different types across files:
  sample_a.h5ad: INTEGER
  sample_b.h5ad: VARCHAR
Using VARCHAR as common type.
```

### Missing Required Component
```
Error: Matrix 'X_umap' not found in file 'sample_c.h5ad'
Files matched: sample_a.h5ad, sample_b.h5ad, sample_c.h5ad
Hint: Check that all files contain the required obsm matrix
```

## Implementation Phases

### Phase 1: Local Glob Support (Core)
1. Implement `GlobHandler` for local file pattern expansion
2. Modify `AnndataBindData` to support file lists
3. Add `schema_mode` parameter to all scan functions
4. Implement basic union schema computation
5. Add `_file_name` column to output

### Phase 2: Schema Harmonization
1. Implement intersection mode
2. Add type coercion for mismatched column types
3. Build per-file column mapping
4. Handle NULL filling for union mode

### Phase 3: Remote Glob Support
1. Implement S3 prefix listing for glob patterns
2. Handle S3 credentials for listing operations
3. Add GCS support (if feasible)
4. Document HTTP limitations (no glob support)

### Phase 4: Performance Optimization
1. Parallel schema discovery
2. File handle pooling for multi-file access
3. Statistics aggregation for query planning
4. Lazy schema discovery option

### Phase 5: Testing & Documentation
1. Unit tests for glob expansion
2. Integration tests for schema modes
3. Performance benchmarks with many files
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
    // Test union of identical schemas
    // Test union with different columns
    // Test intersection mode
    // Test type coercion
}
```

### SQL Tests

```sql
-- test/sql/multi_file_scan.test

# Basic wildcard query
query I
SELECT COUNT(DISTINCT _file_name) FROM anndata_scan_obs('data/samples/*.h5ad');
----
3

# Union mode includes all columns
query I
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM anndata_scan_obs('data/samples/*.h5ad', schema_mode := 'union')
);
----
15

# Intersection mode has fewer columns
query I
SELECT COUNT(*) FROM (
    DESCRIBE SELECT * FROM anndata_scan_obs('data/samples/*.h5ad', schema_mode := 'intersection')
);
----
8

# _file_name is always first column
query T
SELECT column_name FROM (
    DESCRIBE SELECT * FROM anndata_scan_obs('data/samples/*.h5ad')
) LIMIT 1;
----
_file_name
```

## Compatibility Notes

### Backward Compatibility

- Single file paths continue to work unchanged
- No `_file_name` column added for single-file queries
- Default schema_mode is 'union' for intuitive behavior

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

### Batch Integration

```sql
-- Combine expression data from multiple experiments
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
-- Check QC metrics across all files
SELECT
    _file_name,
    COUNT(*) as n_cells,
    AVG(n_genes_by_counts) as avg_genes,
    AVG(total_counts) as avg_counts,
    AVG(pct_counts_mt) as avg_mt_pct
FROM anndata_scan_obs('project/**/*.h5ad')
GROUP BY _file_name
ORDER BY n_cells DESC;
```
