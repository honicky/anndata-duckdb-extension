# HDF5 C API Migration - Implementation Plan

## Overview
This document provides a step-by-step implementation plan for migrating from HDF5 C++ API to the thread-safe C API. The migration will be done incrementally to maintain a working build at each step.

## Current State
- Using HDF5 C++ API with manual concurrency protection
- Cannot open multiple H5 files simultaneously 
- Tests use workarounds with temporary tables

## Target State  
- Thread-safe HDF5 C API
- Support for concurrent file access
- Full UNION query support across multiple files

## Implementation Status

### Progress Summary
- ✅ **Phase 0**: Preparation - COMPLETED
- ✅ **Phase 1**: RAII Wrappers - COMPLETED  
- ✅ **Phase 2**: Core Infrastructure - COMPLETED
- ✅ **Phase 3**: Metadata Reading - COMPLETED
- 🔄 **Phase 4**: Dense Matrix Reading - NEXT
- ⏳ **Phase 5**: Sparse Matrix Reading - PENDING
- ⏳ **Phase 6**: Complex Data Types - PENDING
- ⏳ **Phase 7**: UNION Query Testing - PENDING
- ⏳ **Phase 8**: Integration - PENDING

## Implementation Phases

### Phase 0: Preparation (Day 1) ✅
**Goal**: Set up parallel implementation without breaking existing code

1. Create new files alongside existing ones:
   - `src/include/h5_handles.hpp` - RAII wrappers for C API
   - `src/h5_reader_new.cpp` - New C API implementation
   - `src/include/h5_reader_new.hpp` - New header

2. Keep existing files unchanged:
   - `src/h5_reader.cpp` - Current C++ implementation
   - `src/include/h5_reader.hpp` - Current header

3. Add build flag to switch implementations:
   ```cmake
   option(USE_HDF5_C_API "Use HDF5 C API instead of C++ API" OFF)
   ```

4. **Testing**:
   - Verify existing build still works
   - Run existing test suite to establish baseline
   ```bash
   uv run make clean && uv run make
   uv run make test
   ```

### Phase 1: RAII Wrappers (Day 1-2)
**Goal**: Create safe C API wrappers that prevent resource leaks

Create `h5_handles.hpp` with wrapper classes:

1. **H5FileHandle**
   - Constructor: `H5Fopen()`
   - Destructor: `H5Fclose()`
   - Move semantics only (no copy)

2. **H5GroupHandle**
   - Constructor: `H5Gopen2()`
   - Destructor: `H5Gclose()`
   
3. **H5DatasetHandle**
   - Constructor: `H5Dopen2()`
   - Destructor: `H5Dclose()`

4. **H5DataspaceHandle**
   - Constructor: `H5Dget_space()` or `H5Screate_simple()`
   - Destructor: `H5Sclose()`

5. **H5DatatypeHandle**
   - Constructor: `H5Dget_type()` or `H5Tcopy()`
   - Destructor: `H5Tclose()` (only if owned)

6. **H5AttributeHandle**
   - Constructor: `H5Aopen()`
   - Destructor: `H5Aclose()`

**Testing**:
- Create unit test for each wrapper class
- Test resource cleanup with valgrind
- Test move semantics
```bash
# Compile with new headers
uv run make
# Run memory tests
valgrind --leak-check=full ./test_h5_handles
```

### Phase 2: Core Infrastructure (Day 2-3)
**Goal**: Implement constructor, destructor, and basic methods

1. **Constructor/Destructor**
   ```cpp
   H5Reader::H5Reader(const std::string &file_path) 
       : file_path(file_path), file(file_path, H5F_ACC_RDONLY) {
       // No concurrency tracking needed with thread-safe HDF5
   }
   ```

2. **Helper Methods**
   - `IsGroupPresent()` - Use `H5Lexists()` + `H5Oget_info_by_name()`
   - `IsDatasetPresent()` - Use `H5Lexists()` + type check
   - `GetGroupMembers()` - Use `H5Literate()` with callback
   - `H5TypeToDuckDBType()` - Map C API types

3. **Basic Queries**
   - `IsValidAnnData()` - Check /obs, /var, /X presence
   - `GetObsCount()` - Read dimensions from datasets
   - `GetVarCount()` - Read dimensions from datasets

**Testing**:
- Test file opening/closing
- Test group/dataset detection
- Test basic queries on test files
```bash
uv run make
# Test basic functionality
./build/release/duckdb -unsigned -c "LOAD 'build/release/extension/anndata/anndata.duckdb_extension'; SELECT * FROM anndata_info('test/data/test_small.h5ad');"
```

### Phase 3: Metadata Reading (Day 3-5)
**Goal**: Migrate column information methods

1. **GetObsColumns()**
   - Iterate group members with `H5Literate()`
   - Check categorical data (codes/categories)
   - Handle string types with `H5Tcopy(H5T_C_S1)`

2. **GetVarColumns()**
   - Similar to GetObsColumns
   - Handle array columns specially

3. **String Handling**
   - Fixed-length strings: Direct buffer read
   - Variable-length strings: Use `H5Dvlen_reclaim()`

**Testing**:
- Test column enumeration
- Test categorical detection
- Test string reading
```bash
uv run make test
# Specific tests
./build/release/test/unittest "[anndata_obs]"
./build/release/test/unittest "[anndata_var]"
```

### Phase 4: Dense Matrix Reading (Day 5-7)
**Goal**: Migrate dense matrix reading

1. **ReadObsColumn()**
   - Open dataset with `H5Dopen2()`
   - Create hyperslab with `H5Sselect_hyperslab()`
   - Read with `H5Dread()`

2. **ReadXMatrix()**
   - Handle 2D dense matrices
   - Support partial reads with hyperslabs
   - Type conversion handling

3. **ReadLayerMatrix()**
   - Similar to X matrix
   - Path construction for different layers

**Testing**:
- Test dense matrix reading
- Test partial reads with offsets
- Compare output with C++ version
```sql
-- Test dense matrix
SELECT COUNT(*) FROM anndata_scan_x('test/data/test_dense.h5ad');
SELECT * FROM anndata_scan_x('test/data/test_dense.h5ad') LIMIT 10;
```

### Phase 5: Sparse Matrix Reading (Day 7-10)
**Goal**: Migrate CSR/CSC sparse matrix support

1. **Sparse Format Detection**
   - Check indptr dimensions
   - Determine CSR vs CSC

2. **ReadSparseXMatrixCSR()**
   - Read indptr array
   - Read indices array
   - Read data array
   - Reconstruct dense view

3. **ReadSparseXMatrixCSC()**
   - Similar to CSR but transposed logic
   - Column-wise reconstruction

**Testing**:
- Test CSR format reading
- Test CSC format reading
- Verify sparse to dense conversion
```bash
uv run make test
# Run sparse-specific tests
./build/release/test/unittest "[sparse]"
# SQL tests
./build/release/duckdb test/sql/anndata_sparse.test
```

### Phase 6: Complex Data Types (Day 10-12)
**Goal**: Handle categorical, uns, obsm/varm

1. **Categorical Data**
   - Read categories dataset
   - Read codes dataset
   - Map codes to categories

2. **Obsm/Varm Matrices**
   - List available matrices
   - Read specific columns from matrices

3. **Uns (Unstructured) Data**
   - Handle scalars
   - Handle arrays
   - Handle nested groups

**Testing**:
- Test categorical columns
- Test obsm/varm reading
- Test uns data types
```sql
-- Test obsm
SELECT * FROM anndata_scan_obsm_umap('test/data/test_obsm.h5ad');
-- Test categorical
SELECT DISTINCT cell_type FROM anndata_scan_obs('test/data/test_categorical.h5ad');
-- Test uns
SELECT * FROM anndata_scan_uns('test/data/test_uns.h5ad');
```

### Phase 7: UNION Testing (Day 12-13)
**Goal**: Test concurrent file access

1. **Concurrent Access Tests**
   - Test UNION queries
   - Test parallel table functions
   - Verify no crashes or hangs

2. **Performance Tests**
   - Compare with C++ version
   - Measure thread-safe overhead
   - Profile hot paths

**Testing**:
```sql
-- The critical test: UNION across files
SELECT * FROM anndata_scan_x('test/data/test_small.h5ad')
UNION ALL
SELECT * FROM anndata_scan_x('test/data/test_sparse.h5ad');

-- Complex UNION
SELECT obs_idx, COUNT(*) as gene_count 
FROM (
    SELECT * FROM anndata_scan_x('file1.h5ad')
    UNION ALL
    SELECT * FROM anndata_scan_x('file2.h5ad')
) GROUP BY obs_idx;
```

### Phase 8: Integration (Day 13-14)
**Goal**: Switch to new implementation

1. **Update Build System**
   ```cmake
   if(USE_HDF5_C_API)
       set(EXTENSION_SOURCES 
           src/h5_reader_new.cpp
           # ... other sources
       )
       # Link only C library
       set(HDF5_LIBRARIES hdf5::hdf5-shared)
   else()
       # Keep existing setup
   endif()
   ```

2. **Update vcpkg.json**
   ```json
   "features": ["threadsafe", "szip", "zlib"]
   ```

3. **Rename Files**
   - Move `h5_reader.cpp` → `h5_reader_old.cpp`
   - Move `h5_reader_new.cpp` → `h5_reader.cpp`

### Phase 9: Final Validation (Day 14-15)
**Goal**: Complete test coverage and performance validation

1. **Full Test Suite**
   ```bash
   uv run make test
   ```

2. **Memory Validation**
   ```bash
   valgrind --leak-check=full ./build/release/test/unittest
   ```

3. **Performance Benchmarks**
   - Run benchmarks comparing C++ vs C API
   - Document performance differences

### Phase 10: Cleanup (Day 15-16)
**Goal**: Remove old code and documentation

1. **Remove Old Files**
   - Delete `h5_reader_old.cpp`
   - Remove C++ API references
   - Clean up CMakeLists.txt

2. **Documentation**
   - Update README
   - Document thread-safe capabilities
   - Add migration notes

## Migration Strategy for Each Method

### Method Categories

#### Simple Methods (Direct Port)
- `IsValidAnnData()`
- `GetObsCount()`
- `GetVarCount()`

#### Moderate Complexity
- `IsGroupPresent()`
- `IsDatasetPresent()`
- `GetGroupMembers()`
- `H5TypeToDuckDBType()`

#### Complex Methods (Need Redesign)
- `GetObsColumns()` - String handling changes
- `ReadObsColumn()` - Hyperslab operations
- `ReadSparseXMatrix*()` - Multiple coordinated reads
- Categorical data methods - Memory management

## Key Differences to Handle

### Error Handling
**C++ API**: Exceptions
```cpp
try {
    H5::Group group = file->openGroup(name);
} catch (H5::Exception &e) {
    return false;
}
```

**C API**: Return codes
```cpp
hid_t group_id = H5Gopen2(file_id, name, H5P_DEFAULT);
if (group_id < 0) {
    return false;
}
H5Gclose(group_id);
```

### String Handling
**C++ API**: `H5::StrType` with std::string
```cpp
H5::StrType str_type = dataset.getStrType();
std::string value;
dataset.read(value, str_type);
```

**C API**: Manual buffer management
```cpp
hid_t str_type = H5Dget_type(dataset_id);
size_t size = H5Tget_size(str_type);
char* buffer = new char[size + 1];
H5Dread(dataset_id, str_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer);
buffer[size] = '\0';
std::string value(buffer);
delete[] buffer;
```

### Group Iteration
**C++ API**: Index-based iteration
```cpp
for (hsize_t i = 0; i < group.getNumObjs(); i++) {
    std::string name = group.getObjnameByIdx(i);
}
```

**C API**: Callback-based iteration
```cpp
herr_t callback(hid_t loc_id, const char *name, 
                const H5L_info_t *info, void *op_data) {
    auto* vec = static_cast<std::vector<std::string>*>(op_data);
    vec->push_back(name);
    return 0;
}
H5Literate(group_id, H5_INDEX_NAME, H5_ITER_NATIVE, 
           nullptr, callback, &names);
```

## Testing Plan

### Test Coverage Required
1. All existing tests must pass
2. Add concurrent access tests
3. Add memory leak tests
4. Add performance benchmarks

### Specific Test Cases
```sql
-- Test 1: Basic UNION across files
SELECT * FROM anndata_scan_x('file1.h5ad')
UNION ALL
SELECT * FROM anndata_scan_x('file2.h5ad');

-- Test 2: Complex UNION with filters
SELECT * FROM anndata_scan_x('file1.h5ad') WHERE gene = 'CD3D'
UNION ALL
SELECT * FROM anndata_scan_x('file2.h5ad') WHERE gene = 'CD4';

-- Test 3: Parallel table functions
WITH data AS (
    SELECT * FROM anndata_scan_obs('file1.h5ad')
), genes AS (
    SELECT * FROM anndata_scan_var('file2.h5ad')
)
SELECT * FROM data CROSS JOIN genes;
```

## Risk Mitigation

### Risks
1. **API differences cause bugs**: Mitigate with extensive testing
2. **Performance regression**: Profile and optimize critical paths
3. **Memory leaks**: Use RAII wrappers, run valgrind
4. **Build issues**: Test on all platforms early

### Rollback Plan
- Keep old implementation available via build flag
- Can revert vcpkg.json if needed
- Tag release before migration

## Success Metrics
1. All existing tests pass
2. UNION queries work without crashes
3. No memory leaks detected
4. Performance within 10% of C++ version
5. Concurrent access works correctly

## Timeline Summary
- **Days 1-2**: Setup and RAII wrappers
- **Days 2-3**: Core infrastructure
- **Days 3-5**: Metadata reading
- **Days 5-7**: Dense matrix reading
- **Days 7-10**: Sparse matrix reading
- **Days 10-12**: Complex data types
- **Days 12-13**: Integration
- **Days 13-15**: Testing
- **Days 15-16**: Cleanup

**Total: 16 days** (with buffer for issues)