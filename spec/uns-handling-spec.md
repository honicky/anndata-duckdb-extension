# Unstructured Data (uns) Handling Specification

## Overview
The `uns` (unstructured) slot in AnnData contains heterogeneous metadata that doesn't fit into the structured obs/var/X/layers paradigm. In the HDF5 file, `uns` is stored as a hierarchical structure using HDF5 groups and datasets, allowing arbitrary nesting of data.

## Hierarchical Structure in HDF5

### Storage Format
The `uns` data is stored under the `/uns` group in the HDF5 file with arbitrary nesting:
```
/uns/
├── leiden/                    # HDF5 Group
│   ├── params/                # Nested Group
│   │   ├── resolution         # Dataset (scalar)
│   │   └── random_state       # Dataset (scalar)
│   └── colors                 # Dataset (array of strings)
├── pca/                       # HDF5 Group
│   ├── variance_ratio         # Dataset (1D array)
│   ├── variance               # Dataset (1D array)
│   └── params/                # Nested Group
│       └── n_components       # Dataset (scalar)
├── rank_genes_groups/         # HDF5 Group (complex structure)
│   ├── names                  # Dataset (2D string array)
│   ├── scores                 # Dataset (2D numeric array)
│   ├── pvals                  # Dataset (2D numeric array)
│   └── params/                # Nested Group
│       ├── groupby            # Dataset (scalar string)
│       └── method             # Dataset (scalar string)
└── schema_version             # Dataset (scalar at root level)
```

### Common Content Types
1. **Scalar values**: Individual datasets containing single values
2. **Arrays**: 1D or 2D datasets with numeric or string data
3. **DataFrames**: Groups containing parallel arrays representing columns
4. **Nested groups**: Arbitrary depth hierarchical organization
5. **Sparse matrices**: Groups with 'data', 'indices', 'indptr' datasets
6. **Special objects**: Color palettes, dendrograms, graph structures

### Typical uns Contents
- `leiden`: Clustering parameters and results (nested groups)
- `pca`: PCA results with variance arrays and parameter subgroups
- `neighbors`: Graph connectivity parameters in nested structure
- `umap`: UMAP parameters and derived values
- `rank_genes_groups`: Complex nested structure with multiple arrays
- Color palettes: `leiden_colors`, `cell_type_colors` as string arrays
- Metadata: `hvg` (highly variable genes info) as nested groups
- Version info: Tool versions as scalars or small groups

## Design Approach

### Core Principle
Provide flexible access to uns data while maintaining SQL queryability. Support common use cases with specialized functions while providing generic access for arbitrary data.

### Access Patterns

#### 1. Hierarchical Discovery
```sql
-- List all uns entries with full paths and hierarchy
SELECT * FROM anndata_uns_keys('file.h5ad');
-- Returns: path, parent_path, name, node_type (group/dataset), dtype, shape

-- Example output:
-- /uns/leiden                    /uns         leiden           group      null       null
-- /uns/leiden/params             /uns/leiden  params           group      null       null  
-- /uns/leiden/params/resolution  /uns/leiden/params resolution  dataset    float64    []
-- /uns/leiden/colors             /uns/leiden  colors           dataset    string     [8]
```

#### 2. Path-Based Access
```sql
-- Access any dataset by its full HDF5 path
SELECT * FROM anndata_uns_get('file.h5ad', '/uns/leiden/params/resolution');
-- Returns value based on dataset type

-- Access all scalars under a group
SELECT * FROM anndata_uns_scalars('file.h5ad', '/uns/leiden/params');
-- Returns: key, value for all scalar datasets in that group
```

#### 3. DataFrame-like Structure Access
```sql
-- Access parallel arrays as a unified table
SELECT * FROM anndata_uns_table('file.h5ad', '/uns/rank_genes_groups');
-- Detects parallel arrays and combines them as columns
```

#### 4. Array Access
```sql
-- Access arrays with their indices
SELECT * FROM anndata_uns_array('file.h5ad', '/uns/pca/variance_ratio');
-- Returns: index, value columns
```

#### 5. JSON Export for Complex Hierarchies
```sql
-- Export entire group hierarchy as nested JSON
SELECT anndata_uns_json('file.h5ad', '/uns/leiden');
-- Returns: JSON representation preserving full hierarchy
```

## Implementation Strategy

### Phase 1: Hierarchical Discovery
- Implement `anndata_uns_keys()` to recursively traverse `/uns` group
- Build complete hierarchy tree with parent-child relationships
- Track node types (HDF5 Group vs Dataset)
- Extract metadata (dtype, shape, attributes) for each node
- Support filtering by node type or path pattern

### Phase 2: Path-Based Direct Access
- Implement `anndata_uns_get()` for direct HDF5 path access
- Auto-detect dataset type and return appropriate SQL type
- Handle scalars, 1D arrays, 2D arrays differently
- Support both absolute paths (`/uns/...`) and relative paths

### Phase 3: Group-Based Operations
- Implement `anndata_uns_scalars()` to extract all scalars from a group
- Implement `anndata_uns_arrays()` to list all arrays in a group
- Support recursive descent with depth limits
- Handle heterogeneous types within groups

### Phase 4: Structured Data Recognition
- Detect parallel array patterns (same shape arrays in same group)
- Implement `anndata_uns_table()` for DataFrame-like structures
- Recognize common patterns (e.g., `rank_genes_groups` structure)
- Support column name inference from dataset names

### Phase 5: Complex Hierarchy Export
- Implement `anndata_uns_json()` for full hierarchy export
- Preserve HDF5 group structure in JSON nesting
- Handle attributes as metadata fields
- Provide options for array handling (full, summary, exclude)
- Support selective export by path prefix

## Type Mapping

### HDF5 to SQL Type Conversion
- **H5T_INTEGER** → INTEGER or BIGINT (based on size)
- **H5T_FLOAT** → FLOAT or DOUBLE (based on size)
- **H5T_STRING** → VARCHAR
- **H5T_COMPOUND** → STRUCT (if simple) or JSON (if complex)
- **H5T_ARRAY** → LIST (if 1D) or JSON (if multi-dimensional)
- **Groups** → JSON (nested structure)

### Special Handling

#### Color Palettes
- Detect by key pattern (`*_colors`)
- Return as table: (category, color)
- Support both hex and RGB formats

#### Clustering Results
- Detect standard structures (leiden, louvain)
- Provide both raw access and interpreted views
- Parameters as scalar table, results in obs

#### Differential Expression
- Recognize `rank_genes_groups` structure
- Provide multiple views:
  - Summary statistics per group
  - Top genes per group
  - Full results with filtering

## Query Examples

### Discovering Hierarchical Structure
```sql
-- Explore the uns hierarchy
SELECT path, node_type, dtype, shape
FROM anndata_uns_keys('data.h5ad')
WHERE path LIKE '/uns/leiden%'
ORDER BY path;

-- Find all scalar parameters
SELECT path, name, dtype
FROM anndata_uns_keys('data.h5ad') 
WHERE node_type = 'dataset' AND shape = '[]';

-- Find all 2D arrays (potential tables)
SELECT parent_path, COUNT(*) as array_count
FROM anndata_uns_keys('data.h5ad')
WHERE node_type = 'dataset' AND shape LIKE '[%,%]'
GROUP BY parent_path;
```

### Navigating Nested Groups
```sql
-- Get all parameters from a nested analysis
SELECT * 
FROM anndata_uns_scalars('data.h5ad', '/uns/leiden/params');

-- Access deeply nested value
SELECT value 
FROM anndata_uns_get('data.h5ad', '/uns/rank_genes_groups/params/method');
```

### Working with Structured Data
```sql
-- Access differential expression results as table
SELECT gene_name, score, pval, log2fc
FROM anndata_uns_table('data.h5ad', '/uns/rank_genes_groups')
WHERE pval < 0.01
ORDER BY ABS(log2fc) DESC;

-- Get PCA variance explained
SELECT index AS component, value AS variance_ratio
FROM anndata_uns_array('data.h5ad', '/uns/pca/variance_ratio')
WHERE index < 10;
```

### Extracting Complex Hierarchies
```sql
-- Export entire analysis branch as JSON
SELECT anndata_uns_json('data.h5ad', '/uns/leiden') AS leiden_full;

-- Get just the parameters subtree
SELECT anndata_uns_json('data.h5ad', '/uns/leiden/params') AS leiden_params;
```

## Error Handling

### Key Not Found
- Return empty result set with appropriate schema
- Optional: Provide suggestions for similar keys

### Type Mismatch
- Clear error when accessing with wrong function
- Suggest appropriate function based on actual type

### Large Data Warning
- Warn when accessing very large structures
- Provide options to limit/sample data
- Default limits on JSON export size

## Performance Considerations

### Lazy Loading
- Don't load uns data unless explicitly requested
- Cache key catalog after first scan
- Stream large arrays rather than loading fully

### Indexing
- No indexing needed for uns data (typically small)
- Consider caching frequently accessed paths

### Memory Management
- Set maximum JSON export size (default 10MB)
- Chunk large array reads
- Provide sampling options for exploration

## Future Extensions

### Writing Support
- `anndata_uns_insert()` for adding metadata
- `anndata_uns_update()` for modifying values
- Transaction support for consistency

### Advanced Queries
- JSONPath expressions for complex navigation
- Recursive structure search
- Pattern matching on keys

### Integration
- Automatic uns enrichment from obs/var operations
- Provenance tracking for analysis parameters
- Standard schemas for common analysis types