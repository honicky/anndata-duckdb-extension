# Projection Pushdown Specification

## Problem Statement

Currently, the AnnData table functions (`anndata_scan_x`, `anndata_scan_layers`, etc.) read ALL columns from the HDF5 file regardless of which columns are requested in the SQL query. For wide matrices with tens of thousands of genes, this causes severe performance issues:

```sql
-- This reads ALL 35,044 columns even though only 1 is needed
SELECT Gene_000 FROM anndata_scan_layers('file.h5ad', 'raw_counts') LIMIT 10;
```

With 35,000+ genes, each row loads ~280KB of data (35K × 8 bytes) when only 8 bytes are needed.

**This affects both dense AND sparse matrices:**
- Dense: All columns read from HDF5
- Sparse: Non-zero values read efficiently, but then expanded to dense format with ALL columns

## Solution: Enable DuckDB's Projection Pushdown

DuckDB has **built-in projection pushdown** for table functions. We just need to:
1. Enable it on our table functions
2. Use the column IDs provided by DuckDB
3. Only read/materialize the requested columns

## Implementation

### 1. Enable Projection Pushdown on TableFunction

When registering table functions, set the `projection_pushdown` flag:

```cpp
// In RegisterAnndataTableFunctions()
TableFunction func("anndata_scan_layers", ...);
func.projection_pushdown = true;  // Tell DuckDB we support this
// ... register function
```

### 2. Use column_ids from TableFunctionInput

DuckDB provides the requested column indices via `TableFunctionInput`. Access them in the scan function:

```cpp
static void LayerScan(ClientContext &context, TableFunctionInput &data,
                      DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<AnndataBindData>();
    auto &gstate = data.global_state->Cast<AnndataGlobalState>();

    // DuckDB provides column_ids - which columns are actually needed
    // output.data[] corresponds to these columns in order

    idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE,
                                   bind_data.row_count - gstate.current_row);
    if (count == 0) return;

    // Only read/materialize requested columns
    for (idx_t i = 0; i < output.ColumnCount(); i++) {
        idx_t col_idx = data.column_ids[i];  // The actual column index
        if (col_idx == 0) {
            // obs_idx column - generate synthetic index
            GenerateObsIdx(output.data[i], gstate.current_row, count);
        } else {
            // Read specific gene column from HDF5
            idx_t gene_idx = col_idx - 1;  // Adjust for obs_idx
            gstate.h5_reader->ReadMatrixColumn(
                bind_data.layer_name,
                gene_idx,
                output.data[i],
                gstate.current_row,
                count
            );
        }
    }

    gstate.current_row += count;
    output.SetCardinality(count);
}
```

### 3. Add Column-Based HDF5 Reading Methods

Add methods to `H5ReaderMultithreaded` for reading specific columns:

```cpp
class H5ReaderMultithreaded {
public:
    // Read specific columns from X matrix (dense)
    void ReadXMatrixColumns(const vector<idx_t> &col_indices,
                            idx_t row_start, idx_t row_count,
                            DataChunk &output);

    // Read specific columns from layer (dense)
    void ReadLayerMatrixColumns(const string &layer_name,
                                const vector<idx_t> &col_indices,
                                idx_t row_start, idx_t row_count,
                                DataChunk &output);

    // Read specific columns from sparse matrix
    void ReadSparseMatrixColumns(const string &path,
                                 const vector<idx_t> &col_indices,
                                 idx_t row_start, idx_t row_count,
                                 DataChunk &output);
};
```

### 4. HDF5 Hyperslab Selection for Columns (Dense)

Use HDF5 hyperslab selection to read only specific columns:

```cpp
void H5ReaderMultithreaded::ReadXMatrixColumns(
    const vector<idx_t> &col_indices,
    idx_t row_start, idx_t row_count,
    DataChunk &output) {

    auto h5_lock = H5GlobalLock::Acquire();
    H5DatasetHandle dataset(*file_handle, "/X");
    H5DataspaceHandle file_space(dataset);

    for (idx_t i = 0; i < col_indices.size(); i++) {
        idx_t col_idx = col_indices[i];

        // Select single column hyperslab
        hsize_t start[2] = {row_start, col_idx};
        hsize_t count[2] = {row_count, 1};
        H5Sselect_hyperslab(file_space.id(), H5S_SELECT_SET,
                            start, NULL, count, NULL);

        // Read directly into output vector
        auto vec_data = FlatVector::GetData<double>(output.data[i]);
        H5Dread(dataset.id(), H5T_NATIVE_DOUBLE, ...);
    }
}
```

### 5. Sparse Matrix Column Selection

**CSC format (column-major):** Very efficient - columns stored contiguously
```cpp
// Read indptr[col_idx] and indptr[col_idx+1] to get value range
// Read only those indices and data values
// Scatter non-zeros into result, rest are zero
```

**CSR format (row-major):** Must filter per-row, but still avoids full materialization
```cpp
// For each row, binary search indices for requested columns
// Only materialize values for requested columns
// Skip dense conversion for unrequested columns
```

## Affected Table Functions

| Function | Matrix Type | Priority |
|----------|-------------|----------|
| `anndata_scan_x` | X matrix | High |
| `anndata_scan_layers` | Layer matrices | High |
| `anndata_scan_obsm` | obsm matrices | Medium |
| `anndata_scan_varm` | varm matrices | Medium |

Note: `anndata_scan_obs` and `anndata_scan_var` are typically narrow (few columns) so projection pushdown provides less benefit.

## Performance Impact

### Before (No Projection Pushdown)
```
SELECT Gene_000 FROM layers_raw_counts LIMIT 1000;
-- Reads: 1000 rows × 35,044 columns × 4 bytes = 140 MB
-- Time: ~10+ seconds
```

### After (With Projection Pushdown)
```
SELECT Gene_000 FROM layers_raw_counts LIMIT 1000;
-- Reads: 1000 rows × 1 column × 4 bytes = 4 KB
-- Time: ~10 ms
```

**Expected speedup: 100-1000x for single-column queries**

## Implementation Phases

### Phase 1: Enable Pushdown + Dense Matrix Support
1. Set `projection_pushdown = true` on X and layer table functions
2. Modify scan functions to use `data.column_ids` from DuckDB
3. Implement `ReadXMatrixColumns()` using HDF5 hyperslab selection
4. Test: `SELECT Gene_000 FROM anndata_scan_x(...)` should be fast

### Phase 2: Sparse Matrix Support
1. Implement column-selective reading for CSC format (efficient)
2. Implement column-selective reading for CSR format (filter per-row)
3. Avoid full dense materialization - only materialize requested columns

### Phase 3: ATTACH Integration Verification
1. Verify DuckDB pushes projections through views to table functions
2. Test: `SELECT Gene_000 FROM attached.X` should also be fast
3. If views block pushdown, consider alternative approaches

## Testing Plan

### Unit Tests
```sql
-- Single column selection
SELECT Gene_000 FROM anndata_scan_x('test.h5ad') LIMIT 10;

-- Multiple specific columns
SELECT Gene_000, Gene_100, Gene_500 FROM anndata_scan_x('test.h5ad') LIMIT 10;

-- Column with filter
SELECT Gene_000 FROM anndata_scan_x('test.h5ad') WHERE obs_idx < 100;

-- Through ATTACH
ATTACH 'test.h5ad' AS ad (TYPE ANNDATA);
SELECT Gene_000 FROM ad.X LIMIT 10;
```

### Performance Tests
```sql
-- Benchmark: full scan vs projection pushdown
-- Compare: SELECT * vs SELECT Gene_000
-- Measure: time, memory usage, I/O bytes
```

## Risks and Considerations

1. **View Layer**: Need to verify DuckDB pushes projections through views to underlying table functions

2. **Column Name Mapping**: Must maintain mapping between column names (Gene_000) and HDF5 indices

3. **Sparse Matrix Performance**: CSR format column selection is inherently slower than CSC

4. **Memory Allocation**: Partial reads may have different memory patterns than full reads

5. **Backward Compatibility**: Existing queries should continue to work unchanged
