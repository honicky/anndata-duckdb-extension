# HDF5 C API Migration Specification

## Overview
This specification describes the migration from HDF5 C++ API to the thread-safe C API to enable concurrent access to multiple HDF5 files in the AnnData DuckDB extension.

## Motivation
- **Current limitation**: Cannot open multiple HDF5 files concurrently due to C++ API thread-safety issues
- **User impact**: UNION queries across multiple AnnData files fail
- **Solution**: Migrate to HDF5 C API with thread-safe build enabled

## Thread-Safe HDF5 Behavior

### What Gets Serialized vs What Runs in Parallel

The thread-safe HDF5 library uses a global recursive mutex that protects critical sections, but **not all operations are fully serialized**:

**Serialized (mutex held):**
- Opening/closing files, groups, datasets
- ID allocation and management
- Metadata cache access
- Property list operations
- Error stack management
- Reference counting

**Can run in parallel (mutex released during I/O):**
- Actual disk reads/writes after metadata resolution
- Data type conversions in user buffers
- Compression/decompression of data chunks
- Network I/O for remote files

**Example timeline:**
```
Time →
Thread 1: [Open file1] → [Open dataset] → [Get metadata] → ====Read 100MB data====
Thread 2:                   [Open file2] → [Open dataset] → [Get metadata] → ====Read 50MB data====
          ↑─── Serialized ──↑            ↑─── Serialized ──↑              ↑── Parallel I/O ──↑
```

This means for our AnnData use case:
- **UNION across files**: Good parallelism - different files can be read simultaneously
- **Multiple columns from same file**: Moderate parallelism - metadata serialized, I/O parallel
- **Sparse matrix reads**: May serialize more due to multiple metadata lookups
- **Small reads**: Higher overhead from mutex contention
- **Large batch reads**: Better performance as I/O dominates

## Technical Requirements

### Build Configuration Changes

#### 1. Update vcpkg.json
```json
{
  "dependencies": [
    {
      "name": "hdf5",
      "features": ["threadsafe", "szip", "zlib"],
      "platform": "!emscripten"
    }
  ]
}
```
Note: Remove "cpp" feature, add "threadsafe"

#### 2. Custom HDF5 Port (if needed)
If vcpkg doesn't support threadsafe feature, create custom port:
```cmake
# ports/hdf5-threadsafe/portfile.cmake
vcpkg_configure_cmake(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        -DHDF5_ENABLE_THREADSAFE=ON
        -DHDF5_BUILD_CPP_LIB=OFF  # Incompatible with threadsafe
        -DBUILD_SHARED_LIBS=OFF
        -DHDF5_ENABLE_Z_LIB_SUPPORT=ON
        -DHDF5_ENABLE_SZIP_SUPPORT=ON
)
```

### API Migration Patterns

#### 1. File Operations
**C++ API (Current):**
```cpp
H5::H5File file(file_path, H5F_ACC_RDONLY);
file.close();
```

**C API (New):**
```cpp
hid_t file_id = H5Fopen(file_path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
if (file_id < 0) {
    throw IOException("Failed to open HDF5 file: " + file_path);
}
// ... use file ...
H5Fclose(file_id);
```

#### 2. Group Operations
**C++ API (Current):**
```cpp
try {
    H5::Group group = file->openGroup(group_name);
    return true;
} catch (...) {
    return false;
}
```

**C API (New):**
```cpp
htri_t exists = H5Lexists(file_id, group_name.c_str(), H5P_DEFAULT);
if (exists > 0) {
    hid_t group_id = H5Gopen2(file_id, group_name.c_str(), H5P_DEFAULT);
    if (group_id >= 0) {
        H5Gclose(group_id);
        return true;
    }
}
return false;
```

#### 3. Dataset Operations
**C++ API (Current):**
```cpp
H5::DataSet dataset = file->openDataSet(path);
H5::DataSpace dataspace = dataset.getSpace();
H5::DataType datatype = dataset.getDataType();
dataset.read(buffer, H5::PredType::NATIVE_DOUBLE);
```

**C API (New):**
```cpp
hid_t dataset_id = H5Dopen2(file_id, path.c_str(), H5P_DEFAULT);
if (dataset_id < 0) {
    throw IOException("Failed to open dataset: " + path);
}

hid_t dataspace_id = H5Dget_space(dataset_id);
hid_t datatype_id = H5Dget_type(dataset_id);

herr_t status = H5Dread(dataset_id, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, 
                        H5P_DEFAULT, buffer);
if (status < 0) {
    // Cleanup and throw
}

H5Tclose(datatype_id);
H5Sclose(dataspace_id);
H5Dclose(dataset_id);
```

#### 4. Resource Management Pattern
Create RAII wrappers for automatic cleanup:
```cpp
class H5FileHandle {
private:
    hid_t id;
public:
    H5FileHandle(const std::string& path) {
        id = H5Fopen(path.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
        if (id < 0) throw IOException("Failed to open: " + path);
    }
    ~H5FileHandle() { if (id >= 0) H5Fclose(id); }
    operator hid_t() const { return id; }
};

class H5DatasetHandle {
private:
    hid_t id;
public:
    H5DatasetHandle(hid_t file_id, const std::string& path) {
        id = H5Dopen2(file_id, path.c_str(), H5P_DEFAULT);
        if (id < 0) throw IOException("Failed to open dataset: " + path);
    }
    ~H5DatasetHandle() { if (id >= 0) H5Dclose(id); }
    operator hid_t() const { return id; }
};
```

### Key Migration Areas

#### 1. h5_reader.cpp Constructor/Destructor
- Replace `H5::H5File` with `H5Fopen/H5Fclose`
- Remove all exception-based error handling
- Use return codes and manual cleanup

#### 2. IsGroupPresent Method
```cpp
bool H5Reader::IsGroupPresent(const std::string &group_name) {
    htri_t exists = H5Lexists(file_id, group_name.c_str(), H5P_DEFAULT);
    return exists > 0;
}
```

#### 3. GetObsColumns/GetVarColumns Methods
- Replace `H5::Group::getNumObjs()` with `H5Gget_info`
- Replace `H5::Group::getObjnameByIdx()` with `H5Literate`
- Manual type checking with `H5Oget_info`

#### 4. ReadObsColumn/ReadVarColumn Methods
- Replace DataSet/DataSpace/DataType classes with handles
- Use `H5Sselect_hyperslab` for partial reads
- Manual memory type mapping

#### 5. Sparse Matrix Reading
- Replace nested group/dataset operations
- Careful handle management for multiple datasets
- Use compound operations where possible

### Error Handling Strategy

#### 1. Error Callback
```cpp
herr_t h5_error_callback(hid_t estack, void* client_data) {
    // Log error details
    H5Eprint2(estack, stderr);
    return 0;
}

// Set custom error handler
H5Eset_auto2(H5E_DEFAULT, h5_error_callback, nullptr);
```

#### 2. Consistent Error Checking
```cpp
#define H5_CHECK(call) \
    do { \
        herr_t _status = (call); \
        if (_status < 0) { \
            throw IOException("HDF5 error in " #call); \
        } \
    } while(0)

// Usage
H5_CHECK(H5Dread(dataset_id, type_id, mem_space, file_space, H5P_DEFAULT, buffer));
```

### Type Mapping

| C++ API | C API | Notes |
|---------|--------|-------|
| `H5::PredType::NATIVE_INT32` | `H5T_NATIVE_INT32` | Direct mapping |
| `H5::PredType::NATIVE_DOUBLE` | `H5T_NATIVE_DOUBLE` | Direct mapping |
| `H5::StrType` | `H5Tcopy(H5T_C_S1)` + `H5Tset_size` | Manual setup |
| `H5::CompType` | `H5Tcreate(H5T_COMPOUND, size)` | Manual construction |

### Testing Strategy

#### 1. Unit Tests
- Test each migrated method in isolation
- Verify error handling for invalid inputs
- Memory leak detection with valgrind/ASAN

#### 2. Compatibility Tests
- Ensure identical output for all test files
- Compare performance before/after migration
- Stress test with concurrent file access

#### 3. Thread Safety Tests
```cpp
TEST_CASE("Concurrent file access") {
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; i++) {
        threads.emplace_back([i]() {
            auto reader = H5Reader("test" + std::to_string(i) + ".h5ad");
            // Perform operations
        });
    }
    for (auto& t : threads) t.join();
    // Should complete without errors
}
```

### Implementation Phases

#### Phase 1: Core Infrastructure (3-4 days)
1. Update build configuration for thread-safe HDF5
2. Create RAII wrapper classes for HDF5 handles
3. Implement error handling framework
4. Migrate constructor/destructor

#### Phase 2: Basic Operations (4-5 days)
1. Migrate group/dataset existence checks
2. Migrate metadata reading (dimensions, types)
3. Migrate column listing functions
4. Test with simple queries

#### Phase 3: Data Reading (5-6 days)
1. Migrate dense matrix reading
2. Migrate sparse matrix reading (CSR/CSC)
3. Migrate string/categorical handling
4. Performance optimization

#### Phase 4: Complex Features (3-4 days)
1. Migrate obsm/varm/layers support
2. Migrate uns (unstructured) data
3. Edge case handling
4. Comprehensive testing

#### Phase 5: Optimization & Polish (2-3 days)
1. Performance profiling and optimization
2. Memory usage optimization
3. Documentation updates
4. Final testing and validation

**Total estimate: 17-22 days**

### Performance Considerations

#### 1. Thread-Safe Implementation Details
HDF5's thread-safe mode uses a global mutex (H5_g mutex) that protects:
- **Global data structures**: ID tables, property lists, error stacks
- **Critical sections**: Metadata cache operations, file access coordination
- **NOT every operation**: Actual I/O operations can proceed in parallel once metadata is resolved

The locking is more fine-grained than complete serialization:
```
Thread 1: H5Dopen → [acquire mutex] → update ID table → [release mutex]
Thread 2: H5Dread → [acquire mutex] → get metadata → [release mutex] → [I/O proceeds]
Thread 1: H5Dread → [acquire mutex] → get metadata → [release mutex] → [I/O proceeds]
Both threads: Actual disk I/O can overlap if different datasets/files
```

This means:
- Multiple threads reading different datasets can have overlapping I/O
- Multiple threads reading the same file but different datasets works well
- Metadata operations are serialized but typically fast
- Actual data transfer can happen in parallel for independent operations

#### 2. Expected Performance Impact
- **Single file, single dataset**: ~5-10% overhead from mutex operations
- **Single file, multiple datasets**: Better parallelism, threads can overlap I/O
- **Multiple files**: Near-linear scaling for independent file operations
- **Metadata-heavy operations**: More serialization, higher overhead
- **Large data reads**: Lower overhead as I/O dominates over mutex waits

#### 3. Optimization Opportunities
- Batch metadata operations to reduce mutex contention
- Use dataset/dataspace caching to avoid repeated metadata lookups
- Leverage chunk caching for repeated access patterns
- Consider H5Pset_cache() to tune metadata cache size

### Risks and Mitigations

| Risk | Impact | Mitigation |
|------|---------|------------|
| Memory leaks | High | RAII wrappers, extensive testing |
| Performance regression | Medium | Profiling, optimization phase |
| API incompatibility | Low | Thorough testing against all test files |
| Build complexity | Medium | Custom vcpkg port if needed |
| Handle exhaustion | Medium | Proper resource management |

### Success Criteria

1. **Functional**: All existing tests pass
2. **Performance**: No more than 10% degradation for single-file queries
3. **Concurrency**: UNION queries across multiple files work correctly
4. **Stability**: No crashes or hangs under concurrent load
5. **Memory**: No memory leaks detected by sanitizers

### Migration Checklist

- [ ] Build configuration updated
- [ ] RAII wrappers implemented
- [ ] Error handling framework in place
- [ ] File operations migrated
- [ ] Group operations migrated
- [ ] Dataset reading migrated
- [ ] Sparse matrix support migrated
- [ ] String/categorical support migrated
- [ ] obsm/varm support migrated
- [ ] layers support migrated
- [ ] uns support migrated
- [ ] All unit tests passing
- [ ] Performance benchmarks acceptable
- [ ] Thread safety tests passing
- [ ] Memory leak tests passing
- [ ] Documentation updated

### Example: Migrated ReadObsColumn

```cpp
void H5Reader::ReadObsColumn(const std::string &column_name, Vector &result, 
                             idx_t offset, idx_t count) {
    // Open dataset
    H5DatasetHandle dataset(file_id, "/obs/" + column_name);
    H5DataspaceHandle dataspace(H5Dget_space(dataset));
    H5DatatypeHandle datatype(H5Dget_type(dataset));
    
    // Get dimensions
    hsize_t dims[1];
    H5Sget_simple_extent_dims(dataspace, dims, nullptr);
    
    // Setup hyperslab for partial read
    hsize_t h_offset[1] = {offset};
    hsize_t h_count[1] = {count};
    H5Sselect_hyperslab(dataspace, H5S_SELECT_SET, h_offset, nullptr, 
                        h_count, nullptr);
    
    // Create memory space
    H5DataspaceHandle memspace(H5Screate_simple(1, h_count, nullptr));
    
    // Determine type and read
    if (H5Tequal(datatype, H5T_NATIVE_INT32)) {
        auto data = FlatVector::GetData<int32_t>(result);
        H5_CHECK(H5Dread(dataset, H5T_NATIVE_INT32, memspace, dataspace, 
                         H5P_DEFAULT, data));
    } else if (H5Tequal(datatype, H5T_NATIVE_DOUBLE)) {
        auto data = FlatVector::GetData<double>(result);
        H5_CHECK(H5Dread(dataset, H5T_NATIVE_DOUBLE, memspace, dataspace, 
                         H5P_DEFAULT, data));
    } else if (H5Tget_class(datatype) == H5T_STRING) {
        // String handling - more complex
        ReadStringColumn(dataset, datatype, memspace, dataspace, result);
    }
}
```

## Conclusion

This migration is substantial but necessary to properly support concurrent HDF5 file access. The thread-safe C API provides the only reliable solution for multi-file queries while maintaining data integrity and avoiding the crashes/hangs seen with other approaches.