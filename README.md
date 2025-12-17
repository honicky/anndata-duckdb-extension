# AnnData DuckDB Extension

This extension provides DuckDB with the ability to read AnnData (`.h5ad`) files, which are the standard format for single-cell genomics data analysis.

## Quick Start

### Install from the Community Extension Repository

```sql
-- Install the first time you use it
INSTALL anndata; 

-- Load the extension before using 
LOAD anndata;
```

### Install from the Custom Extension Repository (not )

```sql
-- Allow unsigned extensions (required for non-community extensions)
SET allow_unsigned_extensions = true;

-- Set the extension repository -- first time you use it, or updating to a new version
SET custom_extension_repository = 'https://software-releasers.s3.us-west-2.amazonaws.com';

```

### Use it!

The ATTACH syntax provides a the most intuitive way to work with AnnData files, similar to how DuckDB handles SQLite databases:

```sql
-- Attach an AnnData file
ATTACH 'data.h5ad' AS scdata (TYPE ANNDATA);

-- Query using schema-qualified table names
SELECT * FROM scdata.obs WHERE cell_type = 'T cell';
SELECT * FROM scdata.var WHERE highly_variable = true;
SELECT * FROM scdata.X LIMIT 100;

-- Access dimensional reductions
SELECT * FROM scdata.obsm_X_pca;
SELECT * FROM scdata.obsm_X_umap;

-- Access layers
SELECT * FROM scdata.layers_raw;

-- Detach when done
DETACH scdata;
```

You can also use the function syntax:
```sql
-- Load the extension
LOAD anndata;

-- Get file information
SELECT * FROM anndata_info('data.h5ad');

-- Query observation (cell) metadata
SELECT * FROM anndata_scan_obs('data.h5ad');

-- Query variable (gene) metadata
SELECT * FROM anndata_scan_var('data.h5ad');

-- Query expression matrix
SELECT * FROM anndata_scan_x('data.h5ad');
```


## Features

- Read-only access to AnnData HDF5 files
- Attach to an AnnData file like a database
- Query observation (cell) metadata from `.obs`
- Query variable (gene) metadata from `.var`
- Query expression matrix (`.X`) in wide format with genes as columns
- Query dimensional reductions from `.obsm` and `.varm` (PCA, UMAP, etc.)
- Query pairwise matrices from `.obsp` and `.varp`
- Query alternative expression matrices from `.layers`
- Query unstructured metadata from `.uns`
- Configurable gene name columns for expression matrix
- Efficient HDF5 data reading with proper memory management
- Thread-safe operation on all platforms

## Limitations

- Read only
- Single file access (for now)
- No support for remote files (for now)
- Windows HDF5 library limitations mean that we don't support threading on Windows. This limits the throughput of more complicated queries on Windows.

## Usage

### Table Functions

#### Core Data

```sql
-- Observation (cell) metadata
SELECT * FROM anndata_scan_obs('data.h5ad');

-- Variable (gene) metadata
SELECT * FROM anndata_scan_var('data.h5ad');

-- Expression matrix in wide format (rows=cells, columns=genes)
SELECT * FROM anndata_scan_x('data.h5ad');

-- Use custom gene name column (default is '_index')
SELECT * FROM anndata_scan_x('data.h5ad', 'gene_symbols');
```

#### Dimensional Reductions (obsm/varm)

```sql
-- List available matrices
SELECT * FROM anndata_info('data.h5ad');

-- Query PCA coordinates
SELECT * FROM anndata_scan_obsm('data.h5ad', 'X_pca');

-- Query UMAP coordinates
SELECT * FROM anndata_scan_obsm('data.h5ad', 'X_umap');

-- Query variable embeddings
SELECT * FROM anndata_scan_varm('data.h5ad', 'PCs');
```

#### Pairwise Matrices (obsp/varp)

```sql
-- Query cell-cell distances/connectivities (sparse format)
SELECT * FROM anndata_scan_obsp('data.h5ad', 'distances');
SELECT * FROM anndata_scan_obsp('data.h5ad', 'connectivities');

-- Query gene-gene relationships
SELECT * FROM anndata_scan_varp('data.h5ad', 'correlations');
```

#### Layers

```sql
-- Query alternative expression matrices
SELECT * FROM anndata_scan_layers('data.h5ad', 'raw');
SELECT * FROM anndata_scan_layers('data.h5ad', 'normalized');
```

#### Unstructured Data (uns)

```sql
-- Query unstructured metadata
SELECT * FROM anndata_scan_uns('data.h5ad');

-- Filter to view only scalar values
SELECT key, dtype, value
FROM anndata_scan_uns('data.h5ad')
WHERE type = 'scalar';
```

### Common Patterns

```sql
-- Join expression data with cell metadata
SELECT o.cell_type, AVG(x.Gene_000) as avg_expression
FROM anndata_scan_x('data.h5ad') x
JOIN anndata_scan_obs('data.h5ad') o
  ON x.obs_idx = o.obs_idx
GROUP BY o.cell_type;

-- Combine data from multiple files
SELECT 'sample1' as source, * FROM anndata_scan_obs('sample1.h5ad')
UNION ALL
SELECT 'sample2' as source, * FROM anndata_scan_obs('sample2.h5ad');

-- Export to Parquet
COPY (SELECT * FROM anndata_scan_obs('data.h5ad'))
TO 'obs_metadata.parquet';

-- Combine with other data sourcesL
select count(*)
from scdata.var
where scdata.var.gene_ids in (
  select human_EnsemblID
  from read_csv('https://raw.githubusercontent.com/AllenInstitute/GeneOrthology/refs/heads/main/csv/mouse_human_marmoset_macaque_orthologs_20231113.csv')
);
```

## Development

This extension uses the DuckDB extension template and VCPKG for dependency management.

### Prerequisites

For macOS:
```bash
# Required for timeout command in tests
brew install coreutils
```

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
# Standard build (recommended: use uv for reproducible Python environment)
uv run make

# Build with ninja (faster)
GEN=ninja uv run make

# Debug build
uv run make debug

# Clean build
make clean
```

The built extension will be located at:
`build/release/extension/anndata/anndata.duckdb_extension`

### Code Quality

Before committing code, always run format and tidy checks:

```bash
# Check and fix code formatting (uses DuckDB's clang-format rules)
uv run make format

# Run clang-tidy static analysis
uv run make tidy-check
```

The CI/CD pipeline will fail if format or tidy checks don't pass.

### Testing

```bash
# Run all SQL tests
./build/release/test/unittest --test-dir test "[sql]"

# Run specific test file
./build/release/test/unittest --test-dir test "*layers*"

# Run DuckDB with the extension loaded (for manual testing)
./build/release/duckdb -unsigned
```

### Version Management

Use the version bump script to update the version number:

```bash
# Bump patch version (e.g., 0.1.0 -> 0.1.1)
uv run python scripts/bump_version.py patch

# Bump minor version (e.g., 0.1.0 -> 0.2.0)
uv run python scripts/bump_version.py minor

# Bump major version (e.g., 0.1.0 -> 1.0.0)
uv run python scripts/bump_version.py major

# Set a specific version
uv run python scripts/bump_version.py set 0.3.0
```

The script updates the VERSION file and adds a new section to CHANGELOG.md. **Note:** You must manually edit CHANGELOG.md to fill in the Added/Changed/Fixed sections with your actual changes. See [docs/UPDATING.md](docs/UPDATING.md) for the complete version bump procedure.

### Project Structure

```
├── src/
│   ├── anndata_extension.cpp        # Extension entry point
│   ├── anndata_scanner.cpp          # Table function implementations
│   ├── h5_reader_multithreaded.cpp  # Thread-safe HDF5 data reading
│   └── include/
│       ├── h5_reader_multithreaded.hpp  # HDF5 reader interface
│       └── h5_file_cache.hpp            # File handle caching & thread safety
├── spec/                        # Design specifications
├── test/sql/                    # SQL-based tests
├── vcpkg.json                   # VCPKG manifest (HDF5 dependency)
├── CMakeLists.txt              # Build configuration
└── extension_config.cmake      # DuckDB extension config
```

## License

MIT