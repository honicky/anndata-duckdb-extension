# AnnData DuckDB Extension - Implementation Plan & Progress

## Overview
This document outlines the implementation plan for the AnnData DuckDB extension and tracks actual progress, learnings, and deviations from the original design.

## Current Status Summary (v0.7.0)

### ✅ Completed Phases
- **Phase 1**: Hello World Extension - Basic extension infrastructure working
- **Phase 2**: Python Test Infrastructure - Set up with real test data
- **Phase 4**: Read obs Table - Full implementation with categorical support
- **Phase 5**: Read var Table - Full implementation with _index support
- **Phase 6**: Read X Matrix - Dense matrix support with gene names
- **Phase 7**: Sparse Matrix Support - CSR/CSC format handling
- **Phase 8**: obsm/varm Tables - Dimensional reduction matrices
- **Phase 9**: obsp/varp Pairwise Tables - Cell-cell and gene-gene relationship matrices ✅
- **Phase 10**: Layers Support - Alternative expression matrices
- **Phase 11**: uns (Unstructured) Data - Scalar values, DataFrames, and JSON export ✅
- **HDF5 C API Migration**: Switched from C++ to C API for thread-safe builds ✅

### 🔄 In Progress
- None currently

### 📋 Pending
- **Phase 3**: ATTACH/DETACH - Deferred in favor of table functions
- **Phase 12**: ATTACH/DETACH Interface - Schema-based wrapper over table functions
## Implementation Approach Changes

### Major Decision: Table Functions Instead of ATTACH/DETACH
**Original Plan**: Use ATTACH/DETACH paradigm like SQLite extension
**Current Implementation**: Direct table functions

```sql
-- Original plan:
ATTACH 'file.h5ad' AS test (TYPE ANNDATA);
SELECT * FROM test.obs;

-- Current implementation:
SELECT * FROM anndata_scan_obs('file.h5ad');
SELECT * FROM anndata_scan_var('file.h5ad');
```

**Rationale**:
- Simpler initial implementation
- No catalog modifications needed
- Easier testing and debugging
- ATTACH/DETACH can be added later as a wrapper

## Phase Details

### Phase 1: Hello World Extension ✅
**Goal**: Set up basic DuckDB extension infrastructure

**Implementation**:
1. ✅ Used DuckDB's official extension template
2. ✅ Created basic extension entry point with functions
3. ✅ Implemented `anndata_version()` and `anndata_hello()`

**Key Learnings**:
- Must use DuckDB's official template with git submodules
- Version must match exactly (v1.1.0)
- Need `OVERRIDE_GIT_DESCRIBE=v1.1.0` for Docker builds

**Testing**:
```sql
LOAD 'build/release/extension/anndata/anndata.duckdb_extension';
SELECT anndata_hello();  -- Returns "Hello from AnnData extension!"
```

### Phase 2: Python Test Infrastructure ✅
**Goal**: Set up Python environment for test data

**Implementation**:
1. ✅ Created `pyproject.toml` with uv configuration
2. ✅ Installed dependencies: `anndata`, `pandas`, `numpy`, `h5py`
3. ✅ Using real test file: `2d85960a-2ba8-4f54-9aec-537fae839f5d_subset.h5ad`

**Test Data**:
- Real AnnData file: 8,417 cells × 33,145 genes
- Contains categorical columns, numerical features
- Located in `data/` directory (gitignored)

### Phase 3: Basic ATTACH/DETACH 📋
**Status**: Deferred - using table functions instead

**Revised Approach**:
- Implement table functions first
- Add ATTACH/DETACH as convenience wrapper later
- Focus on core functionality

### Phase 4: Read obs Table ✅
**Goal**: Access cell metadata

**Implementation**:
1. ✅ Created `anndata_scanner.cpp` with obs table function
2. ✅ Created `h5_reader.cpp` for HDF5 operations
3. ✅ Schema detection from `/obs` group
4. ✅ Categorical column support with caching
5. ✅ Full data reading implementation

**Index Handling**:
- **obs_idx**: Always present - synthetic integer index (0, 1, 2...) for reliable joins
- **_index** or **index**: Included when present in HDF5 - contains actual cell identifiers (e.g., barcodes)
- Duplicate column names (case-insensitive) are mangled with underscores (e.g., `cell_ID` and `cell_id` become `cell_ID` and `cell_id_`)

**Current Capabilities**:
- Detects all columns with correct types
- Handles categorical data (codes + categories)
- Includes _index/index columns when present
- Handles duplicate column names via mangling
- Caches categorical mappings for performance

**Testing**:
```sql
SELECT * FROM anndata_scan_obs('path/to/file.h5ad') LIMIT 5;
SELECT obs_idx, _index, cell_type FROM anndata_scan_obs('path/to/file.h5ad');
```

### Phase 5: Read var Table ✅
**Goal**: Access gene metadata

**Implementation**:
- ✅ Table function structure in place
- ✅ Schema detection working
- ✅ Full data reading implementation

**Index Handling** (same pattern as obs):
- **var_idx**: Always present - synthetic integer index (0, 1, 2...) for reliable joins
- **_index** or **index**: Included when present in HDF5 - contains actual gene identifiers (e.g., Ensembl IDs, gene symbols)
- Duplicate column names handled via mangling (same as obs)

**Testing**:
```sql
SELECT * FROM anndata_scan_var('path/to/file.h5ad') LIMIT 5;
SELECT var_idx, _index, highly_variable FROM anndata_scan_var('path/to/file.h5ad');
```

### Phase 6: Dense Matrix X ✅
**Goal**: Read main expression matrix

**Implementation**:
1. ✅ Added `anndata_scan_x` table function
2. ✅ Read `/X` dataset as dense array
3. ✅ Transform to long format: (obs_idx, var_idx, var_name, value)
4. ✅ Chunked reading with configurable var_column

### Phase 7: Sparse Matrix Support ✅
**Goal**: Efficient sparse matrix handling

**Implementation**:
1. ✅ Detect sparse format (CSR/CSC) in `/X`
2. ✅ Read only non-zero values
3. ✅ Automatic format detection and handling
4. ✅ Support for both CSR and CSC formats

### Phase 8: obsm/varm Tables ✅
**Goal**: Access dimensional reductions

**Implementation**:
1. ✅ Scan `/obsm` and `/varm` groups
2. ✅ Create `anndata_scan_obsm` and `anndata_scan_varm` functions with matrix_name parameter
3. ✅ Dynamic column generation (dim_0, dim_1, ...)
4. ✅ Support for PCA, UMAP, t-SNE, etc.

### Phase 9: obsp/varp Pairwise Tables ✅
**Goal**: Access pairwise relationship matrices (e.g., connectivities, distances)

**Implementation**:
1. ✅ Scan `/obsp` and `/varp` groups for sparse matrices
2. ✅ Create table functions:
   - `anndata_scan_obsp(file, matrix_name)` - Returns (obs_idx_1, obs_idx_2, value)
   - `anndata_scan_varp(file, matrix_name)` - Returns (var_idx_1, var_idx_2, value)
3. ✅ Handle sparse formats (CSR/CSC) efficiently
4. ✅ Only return non-zero values to avoid massive result sets

**Index Handling**:
- **obs_idx_1, obs_idx_2** (for obsp): Synthetic integer indices corresponding to row/column positions in the matrix
- **var_idx_1, var_idx_2** (for varp): Synthetic integer indices for gene pairs
- These indices match the `obs_idx` and `var_idx` values from their respective tables for joins
- Note: obsp/varp matrices typically don't have their own `_index` columns - they use the indices from obs/var

**Example Usage**:
```sql
-- Get connectivity matrix (nearest neighbor graph)
SELECT * FROM anndata_scan_obsp('file.h5ad', 'connectivities') 
WHERE obs_idx_1 = 0;

-- Get distance matrix
SELECT * FROM anndata_scan_obsp('file.h5ad', 'distances')
WHERE value < 0.5;
```

**Technical Notes**:
- These are typically very large sparse matrices (n×n where n = number of cells/genes)
- Store pairwise relationships like cell-cell distances or connectivity graphs
- Must handle efficiently as returning full matrix would be impractical
- Common matrices: 'connectivities' (kNN graph), 'distances' (pairwise distances)

### Phase 10: Layers Support ✅
**Goal**: Access alternative expression matrices

**Implementation**:
1. ✅ Scan `/layers` group
2. ✅ Create `anndata_scan_layers` function with layer_name parameter
3. ✅ Support both dense and sparse formats
4. ✅ Returns same format as main X matrix (obs_idx, var_idx, var_name, value)

### Phase 11: uns (Unstructured) Data ✅
**Goal**: Handle arbitrary metadata

**Implementation**:
1. ✅ Scan `/uns` group recursively  
2. ✅ Type detection (scalar, array, group, dataframe)
3. ✅ `anndata_scan_uns()` function returns scalar values
4. ✅ `anndata_scan_uns_<dataframe>()` functions for DataFrames
5. ✅ `anndata_scan_uns_json()` for complex nested structures

### anndata_info Function ✅
**Goal**: Provide file metadata as a table function

**Implementation**:
1. ✅ Converted from scalar function to table function (v0.5.0)
2. ✅ Returns key-value pairs: (property VARCHAR, value VARCHAR)
3. ✅ Properties include: n_obs, n_vars, obsm_keys, varm_keys, layers, uns_keys, obsp_keys, varp_keys

### Phase 12: ATTACH/DETACH Interface 📋
**Goal**: Provide standard DuckDB attachment semantics
**Prerequisites**: All table functions must be complete (Phases 4-10)

**Implementation Approach**:
After all table functions are working, wrap them in ATTACH/DETACH interface:

```sql
-- Future syntax (after table functions work)
ATTACH 'file.h5ad' AS mydata (TYPE ANNDATA);
SELECT * FROM mydata.obs;
SELECT * FROM mydata.var;
SELECT * FROM mydata.main;  -- X matrix
DETACH mydata;
```

**Required Components**:

1. **StorageExtension Class**
   ```cpp
   class AnndataStorageExtension : public StorageExtension {
       // Register TYPE ANNDATA
       // Handle attach/detach requests
   };
   ```

2. **Custom Catalog**
   ```cpp
   class AnndataCatalog : public Catalog {
       // Create virtual tables that wrap our table functions:
       // - obs → wraps anndata_scan_obs()
       // - var → wraps anndata_scan_var()  
       // - main → wraps anndata_scan_x()
       // - obsm_* → wraps anndata_scan_obsm_*()
       // - etc.
   };
   ```

3. **Attach Function**
   - Parse AttachInfo for options (VAR_NAME_COLUMN, etc.)
   - Validate H5AD file
   - Create catalog with all virtual tables
   - Each virtual table delegates to existing table functions

4. **Benefits**:
   - Namespace isolation under schema
   - Multiple simultaneous attachments
   - Cleaner SQL without file paths
   - Configuration per attachment
   - Standard DuckDB pattern

**Note**: This is a wrapper layer - all functionality comes from the underlying table functions, which must be completed first.

## Technical Architecture

### Core Components

```
src/
├── anndata_extension.cpp           # Extension entry point
├── anndata_scanner.cpp             # Table function implementations  
├── h5_reader_multithreaded.cpp     # HDF5 C API operations (thread-safe)
└── include/
    ├── anndata_extension.hpp
    ├── anndata_scanner.hpp
    ├── h5_reader_multithreaded.hpp
    └── h5_handles.hpp              # RAII wrappers for HDF5 C API
```

### Major Architecture Change: HDF5 C API Migration
**Why**: The HDF5 C++ API is incompatible with thread-safe builds (`--enable-threadsafe`)
**Solution**: Complete migration to HDF5 C API with RAII wrappers

### H5ReaderMultithreaded Class Design
```cpp
class H5ReaderMultithreaded {
    // File management with RAII wrapper
    H5FileHandle file;  // RAII wrapper for hid_t
    
    // Thread-safe initialization
    static std::once_flag hdf5_init_flag;
    
    // Categorical caching
    struct CategoricalCache {
        std::vector<std::string> categories;
        std::vector<int> codes;
    };
    std::unordered_map<std::string, CategoricalCache> categorical_cache;
    
    // Core operations using HDF5 C API
    bool IsValidAnnData();
    std::vector<ColumnInfo> GetObsColumns();
    void ReadObsColumn(const std::string &column_name, Vector &result, 
                      idx_t offset, idx_t count);
};
```

### RAII Handle Wrappers for HDF5 C API
```cpp
// Example from h5_handles.hpp
class H5FileHandle {
    hid_t id;
public:
    explicit H5FileHandle(const std::string &path, unsigned int flags = H5F_ACC_RDONLY);
    ~H5FileHandle() { if (id >= 0) H5Fclose(id); }
    // Move-only semantics, no copying
};
```

### Type Mapping
| HDF5 Type | DuckDB Type | Notes |
|-----------|-------------|-------|
| H5T_INTEGER (≤4 bytes) | INTEGER | 32-bit |
| H5T_INTEGER (>4 bytes) | BIGINT | 64-bit |
| H5T_FLOAT (≤4 bytes) | FLOAT | 32-bit |
| H5T_FLOAT (>4 bytes) | DOUBLE | 64-bit |
| H5T_STRING | VARCHAR | Trimmed |
| Categorical | VARCHAR | Via cache |

## Build System

### Docker-Based Development
All dependencies handled via Docker for consistency:

```yaml
services:
  dev:    # Development shell
  build:  # Build service
  test:   # Test runner
```

**Build Command**:
```bash
OVERRIDE_GIT_DESCRIBE=v1.1.0 make
```

### Key Requirements
- DuckDB v1.3.2+ 
- HDF5 1.14.0+ with thread-safe support (C API only, no C++)
- CMake 3.5+
- C++11 compiler
- vcpkg for dependency management

## Testing Strategy

### Current Test File
- **File**: `2d85960a-2ba8-4f54-9aec-537fae839f5d_subset.h5ad`
- **Size**: 8,417 cells × 33,145 genes
- **Features**: Categorical columns, numerical data, real biological data

### Test Commands
```sql
-- Load extension (development)
duckdb -unsigned

-- Load extension
LOAD 'build/release/extension/anndata/anndata.duckdb_extension';

-- Test functions
SELECT anndata_info('data/cellxgene/test_v1/file.h5ad');
SELECT * FROM anndata_scan_obs('data/cellxgene/test_v1/file.h5ad') LIMIT 5;
SELECT COUNT(*) FROM anndata_scan_obs('data/cellxgene/test_v1/file.h5ad');
```

### Python Validation (Future)
```python
import anndata
import duckdb

# Load data both ways
adata = anndata.read_h5ad("test.h5ad")
con = duckdb.connect()
con.execute("LOAD 'anndata.duckdb_extension'")

# Compare results
db_count = con.execute("SELECT COUNT(*) FROM anndata_scan_obs('test.h5ad')").fetchone()[0]
assert db_count == adata.n_obs
```

## Performance Considerations

### Current Implementation
- Chunked reading (STANDARD_VECTOR_SIZE)
- Categorical mapping cache
- Hyperslab selections for HDF5

### Future Optimizations
1. **Parallel Scanning**: Use DuckDB's parallel capabilities
2. **Pushdown Filters**: Apply WHERE at HDF5 level
3. **Column Pruning**: Only read requested columns
4. **Sparse Optimization**: Skip zeros in sparse matrices

## Key Learnings

### What Worked Well
1. **Docker-first development** - Consistent environment
2. **Table functions** - Simpler than ATTACH/DETACH
3. **Official DuckDB template** - Handles complexity
4. **Early real data testing** - Revealed schema issues
5. **Categorical caching** - Good performance boost

### Challenges Overcome
1. **Git version in Docker** - Fixed with OVERRIDE_GIT_DESCRIBE
2. **HDF5 string padding** - Trim whitespace after reading
3. **Build cache** - Removed `make clean` from docker-compose
4. **Memory management** - Use `make_uniq` not `make_unique`

### Important Notes
- Always use `-unsigned` flag during development
- Test with real data files early
- Cache categorical mappings aggressively
- Handle NULLs gracefully for missing data

## Next Immediate Steps

1. ✅ Complete actual HDF5 data reading for obs
2. ✅ Implement var data reading
3. ✅ Add dense X matrix support
4. ✅ Performance benchmarking
5. ✅ Add sparse matrix support

## Success Criteria

### Phase Completion Metrics
1. **Hello World** ✅: Extension loads, functions work
2. **Test Data** ✅: Real .h5ad file available
3. **obs Table** 🔄: Schema ✅, Data reading in progress
4. **var Table** 📋: Schema works, data pending
5. **Dense X** 📋: Not started
6. **Sparse X** 📋: Not started
7. **obsm/varm** 📋: Not started
8. **Layers** 📋: Not started
9. **uns** 📋: Not started

### Performance Targets
- Small dataset (< 10K cells): < 1 second to scan
- Medium dataset (100K cells): < 5 seconds full scan
- Large dataset (1M cells): Chunked, < 1 GB memory
- Sparse matrices: 10x faster than dense

## Directory Structure

```
anndata-duckdb/
├── CMakeLists.txt           # DuckDB template-based
├── Dockerfile               # Development environment
├── docker-compose.yml       # Service definitions
├── docker-dev.sh           # Helper script
├── pyproject.toml          # Python dependencies
├── src/
│   ├── anndata_extension.cpp
│   ├── anndata_scanner.cpp
│   ├── h5_reader.cpp
│   └── include/
│       ├── anndata_extension.hpp
│       ├── anndata_scanner.hpp
│       └── h5_reader.hpp
├── test/
│   └── sql/
│       └── test_real_file.sql
├── spec/
│   ├── anndata-duckdb-extension.md
│   ├── api-reference.md
│   ├── examples.md
│   └── implementation-plan.md (this file)
└── data/                    # Git-ignored test data
    └── cellxgene/
        └── test_v1/
            └── *.h5ad
```

## Implementation Notes

### Key Decisions and Rationale

#### 1. Table Functions vs ATTACH/DETACH
**Decision**: Start with table functions, defer ATTACH/DETACH
- Simpler implementation and testing
- No catalog modifications needed
- ATTACH/DETACH can wrap table functions later
- Faster iteration during development

#### 2. Docker-First Development
**Decision**: All dependencies in Docker
- Consistent HDF5 versions across environments
- No local system pollution
- Reproducible builds
- Simplified CI/CD

#### 3. H5Reader Class Architecture
**Decision**: Centralized HDF5 handling with caching
```cpp
class H5Reader {
    // Categorical cache for performance
    std::unordered_map<std::string, CategoricalCache> categorical_cache;
    
    // Lazy file loading
    std::unique_ptr<H5::H5File> file;
};
```
- Reduces repeated HDF5 reads
- Centralizes error handling
- Simplifies type conversions

#### 4. Memory Management
**Decision**: Use DuckDB's `make_uniq` not `std::make_unique`
- DuckDB tracks custom allocations
- Ensures proper cleanup
- Consistent with DuckDB codebase

### Challenges and Solutions

#### Git Version in Docker
**Problem**: Submodule `.git` structure breaks in mounted volumes
**Solution**: `OVERRIDE_GIT_DESCRIBE=v1.1.0` in build command

#### HDF5 String Padding
**Problem**: Fixed-length strings have trailing spaces
**Solution**: Trim after reading:
```cpp
str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);
```

#### Build Cache Destruction
**Problem**: `make clean` in docker-compose destroyed cache
**Solution**: Removed `make clean`, preserve incremental builds

#### Extension Loading During Development
**Problem**: Unsigned extension warning
**Solution**: Always use `duckdb -unsigned` for development

### Categorical Data Implementation
AnnData stores categoricals as separate arrays:
- `/obs/column_name/codes` - integer indices
- `/obs/column_name/categories` - string values

Our approach:
1. Cache categories on first read
2. Map codes to strings during scan
3. Return VARCHAR to DuckDB
4. Handle missing codes as NULL

### Performance Optimizations Applied
1. **Categorical Caching**: Read categories once per column
2. **Hyperslab Selection**: Read only requested rows from HDF5
3. **Chunked Reading**: Process STANDARD_VECTOR_SIZE rows at a time
4. **Type-Specific Paths**: Optimized code for each HDF5 type

### Lessons Learned

#### What Worked Well
- **DuckDB Extension Template**: Handles build complexity
- **Docker Environment**: Eliminates "works on my machine"
- **Real Data Early**: Found schema issues immediately
- **Table Functions First**: Simpler than expected
- **Categorical Caching**: Major performance improvement

#### What Was Harder Than Expected
- **Git Submodules in Docker**: Required workaround
- **HDF5 String Types**: Multiple formats to handle
- **Build System**: Template learning curve
- **Type Conversions**: Many edge cases

#### Key Insights
1. Start with simplest viable approach (table functions)
2. Test with real data from day one
3. Cache expensive operations aggressively
4. Handle missing/malformed data gracefully
5. Use official templates and tools
6. Document workarounds immediately

### Future Architectural Considerations

#### Sparse Matrix Handling
- Detect format: CSR vs CSC vs COO
- Create iterator-based scanning
- Consider memory-mapped access for large matrices

#### ATTACH/DETACH Wrapper
```sql
-- Future syntax
ATTACH 'file.h5ad' AS mydata (TYPE ANNDATA);
-- Would create virtual schema with all tables
```

#### Parallel Scanning
- Implement DuckDB's parallel scan interface
- Partition by row ranges
- Thread-safe HDF5 access considerations

#### Performance Targets Not Yet Measured
- Small files (< 10K cells): < 1 second
- Medium files (100K cells): < 5 seconds  
- Large files (1M cells): Streaming, < 1GB memory
- Sparse vs Dense: 10x improvement goal

## Appendix: Command Reference

### Build Commands
```bash
# Build extension
./docker-dev.sh build

# Enter development shell
./docker-dev.sh shell

# Run tests
./docker-dev.sh test

# Clean build
./docker-dev.sh clean
```

### SQL Commands
```sql
-- Development loading
duckdb -unsigned
LOAD 'build/release/extension/anndata/anndata.duckdb_extension';

-- Query obs table
SELECT * FROM anndata_scan_obs('file.h5ad') LIMIT 10;

-- Query var table  
SELECT * FROM anndata_scan_var('file.h5ad') LIMIT 10;

-- Get file info
SELECT anndata_info('file.h5ad');
```

### Git Commands
```bash
# Commit with co-author
git commit -m "feat: Your message

🤖 Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>"
```