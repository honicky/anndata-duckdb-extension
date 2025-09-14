# AnnData DuckDB Extension

This extension provides DuckDB with the ability to read AnnData (`.h5ad`) files, which are the standard format for single-cell genomics data analysis.

## Features

- Read-only access to AnnData HDF5 files
- Table function interface for querying expression matrices
- Support for categorical data in observation metadata
- Efficient HDF5 data reading with proper memory management

## Building

### Prerequisites

- CMake 3.5+
- C++11 compatible compiler
- Git
- VCPKG (will be installed automatically if not present)
- ninja and ccache (optional, for faster builds)

### Quick Setup

Run the setup script to install dependencies:

```bash
./setup-dev.sh
```

This will:
- Install VCPKG if not present
- Configure VCPKG environment variables
- Install HDF5 via VCPKG
- Set up Python environment (if uv is installed)

### Build Commands

```bash
# Standard build
make

# Build with ninja (faster)
GEN=ninja make

# Debug build
make debug

# Clean build
make clean
```

The built extension will be located at:
`build/release/extension/anndata/anndata.duckdb_extension`

## Usage

```sql
-- Load the extension
LOAD 'path/to/anndata.duckdb_extension';

-- Query an AnnData file
SELECT * FROM anndata_scan('data.h5ad', 'obs');
SELECT * FROM anndata_scan('data.h5ad', 'var');
SELECT * FROM anndata_scan('data.h5ad', 'X');
```

## Development

This extension uses the DuckDB extension template and VCPKG for dependency management. 

### Project Structure

```
├── src/
│   ├── anndata_extension.cpp    # Extension entry point
│   ├── anndata_scanner.cpp       # Table function implementation
│   ├── h5_reader.cpp            # HDF5 data reading
│   └── include/                 # Header files
├── vcpkg.json                   # VCPKG manifest
├── CMakeLists.txt              # Build configuration
└── extension_config.cmake      # DuckDB extension config
```

### Testing

```bash
make test
```

## License

MIT