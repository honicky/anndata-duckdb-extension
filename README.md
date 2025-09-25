# AnnData DuckDB Extension

This extension provides DuckDB with the ability to read AnnData (`.h5ad`) files, which are the standard format for single-cell genomics data analysis.

## Features

- Read-only access to AnnData HDF5 files
- Query observation (cell) metadata from `.obs`
- Query variable (gene) metadata from `.var`
- Query expression matrix (`.X`) in wide format with genes as columns
- Query unstructured metadata from `.uns`
- Support for categorical data in observation and variable metadata
- Configurable gene name columns for expression matrix
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

-- Query observation (cell) metadata
SELECT * FROM anndata_scan_obs('data.h5ad');

-- Query variable (gene) metadata  
SELECT * FROM anndata_scan_var('data.h5ad');

-- Query expression matrix in wide format (rows=cells, columns=genes)
SELECT * FROM anndata_scan_x('data.h5ad');

-- Select specific genes by column name
SELECT obs_idx, gene_0, gene_10, gene_20
FROM anndata_scan_x('data.h5ad')
WHERE obs_idx < 10;

-- Use custom gene name column (default is '_index')
SELECT * FROM anndata_scan_x('data.h5ad', 'gene_symbols');

-- Join expression data with cell metadata using obs_idx
SELECT o.cell_type, AVG(x.Gene_000) as avg_gene_0_expression
FROM anndata_scan_x('data.h5ad') x
JOIN anndata_scan_obs('data.h5ad') o 
  ON x.obs_idx = o.obs_idx
GROUP BY o.cell_type;

-- Query unstructured metadata (uns)
SELECT * FROM anndata_scan_uns('data.h5ad');

-- Filter to view only scalar values with their actual values
SELECT key, dtype, value 
FROM anndata_scan_uns('data.h5ad')
WHERE type = 'scalar';
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