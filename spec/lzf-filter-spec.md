# LZF Filter Support Specification

## Problem Statement

Many AnnData (.h5ad) files use LZF compression for their datasets. LZF (filter ID 32000) is a fast, lightweight compression algorithm commonly used in Python's h5py library. However, LZF is not included in the standard HDF5 library distribution.

When the extension attempts to read an LZF-compressed h5ad file:

```
HDF5-DIAG: Error detected in HDF5 (1.14.6):
  ...
  #008: H5Z.c line 1410 in H5Z_pipeline(): required filter 'lzf' is not registered
```

The HDF5 `H5Dread` function returns an error code (-1), but the current code doesn't check return values, causing:
- Garbage data to be used for sparse matrix indices
- Infinite loops in `ReadSparseMatrixCSR` and `ReadSparseMatrixCSC`
- No meaningful error message to the user

## Solution

Bundle the LZF filter directly into the extension so LZF-compressed files can be read transparently without any user configuration.

### Why Static Bundling?

| Approach | Pros | Cons |
|----------|------|------|
| **Static bundling** | Self-contained, no user config, works everywhere | Slightly larger binary |
| Dynamic plugin | Smaller binary | Requires `HDF5_PLUGIN_PATH` env var, user setup |
| Error message only | Easy to implement | Doesn't solve the problem |

**Decision**: Static bundling is the right approach for a DuckDB extension that should "just work".

## Implementation

### 1. LZF Source Files

Copy the following files from [h5py/lzf](https://github.com/h5py/h5py/tree/master/lzf) into `src/lzf/`:

| File | Purpose |
|------|---------|
| `lzf_filter.c` | HDF5 filter wrapper, contains `register_lzf()` |
| `lzf_filter.h` | Header declaring `register_lzf()` |
| `lzf.h` | LZF library public interface |
| `lzfP.h` | LZF internal definitions |
| `lzf_c.c` | LZF compression implementation |
| `lzf_d.c` | LZF decompression implementation |

**License**: BSD (compatible with Apache 2.0)

### 2. CMakeLists.txt Changes

```cmake
# Add LZF filter sources
set(LZF_SOURCES
    src/lzf/lzf_filter.c
    src/lzf/lzf_c.c
    src/lzf/lzf_d.c
)

# Add to extension sources
set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES}
    ${LZF_SOURCES}
)

# Add include directory
include_directories(src/lzf)
```

### 3. Extension Initialization

In `src/anndata_extension.cpp`, register the filter at extension load:

```cpp
#include "lzf/lzf_filter.h"

void AnndataExtension::LoadInternal(DatabaseInstance &db) {
    // Register LZF filter for reading compressed h5ad files
    if (register_lzf() < 0) {
        // Warning only - actual error will surface on read if needed
    }

    // ... rest of initialization
}
```

### 4. Defensive Error Checking

Even with LZF support, other unsupported filters could cause the same issue. Add error checking to `H5Dread` calls:

```cpp
// In src/include/h5_handles.hpp
#define H5_READ_CHECK(call, dataset_path)                                      \
    do {                                                                       \
        herr_t _h5_status = (call);                                            \
        if (_h5_status < 0) {                                                  \
            throw std::runtime_error("HDF5 read error at '" +                  \
                std::string(dataset_path) + "': " #call);                      \
        }                                                                      \
    } while (0)
```

Apply to critical functions that cause infinite loops:
- `ReadSparseMatrixCSR` (lines 2710-2828)
- `ReadSparseMatrixCSC` (lines 2830-2947)

Fix silent error swallowing in catch blocks (lines 2823-2825, 2942-2944).

## How LZF Filter Registration Works

The HDF5 filter API allows registering custom compression filters:

```c
// From lzf_filter.c
int register_lzf(void) {
    H5Z_class2_t filter_class = {
        H5Z_CLASS_T_VERS,           // Version
        (H5Z_filter_t)H5Z_FILTER_LZF, // Filter ID = 32000
        1, 1,                        // Encoder/decoder present
        "lzf",                       // Filter name
        NULL,                        // can_apply callback
        NULL,                        // set_local callback
        (H5Z_func_t)lzf_filter       // Filter function
    };
    return H5Zregister(&filter_class);
}
```

Once registered, HDF5 automatically uses the filter when reading datasets compressed with filter ID 32000.

## Testing

1. **Unit test**: Read a known LZF-compressed h5ad file
2. **Integration test**: Verify the file that triggered the original bug works
3. **Error handling test**: Test with a file using an unsupported filter to verify proper error message

## Files Modified

| File | Change |
|------|--------|
| `src/lzf/*` | New directory with LZF filter source |
| `CMakeLists.txt` | Add LZF sources to build |
| `src/anndata_extension.cpp` | Call `register_lzf()` at load |
| `src/include/h5_handles.hpp` | Add H5_READ_CHECK macro |
| `src/h5_reader_multithreaded.cpp` | Add error checking to sparse matrix reads |

## Future Considerations

Other HDF5 filters commonly used in scientific data:
- **BLOSC** (ID 32001) - Very common in Zarr, sometimes in h5ad
- **ZSTD** (ID 32015) - Modern, high-performance compression
- **LZ4** (ID 32004) - Extremely fast compression

If these become common in h5ad files, the same bundling approach can be applied.
