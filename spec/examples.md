# AnnData DuckDB Extension - Usage Examples

## Basic Usage

### 1. Exploring an AnnData File

```sql
-- Load the extension
LOAD anndata;

-- Check file metadata
SELECT * FROM anndata_info('pbmc3k.h5ad');

-- Get specific metadata
SELECT value FROM anndata_info('pbmc3k.h5ad') WHERE property = 'n_obs';

-- Check dimensions directly from X matrix
SELECT COUNT(DISTINCT obs_idx) as n_cells,
       COUNT(DISTINCT var_idx) as n_genes
FROM anndata_scan_x('pbmc3k.h5ad')
LIMIT 1000;  -- Sample for quick count

-- Query expression matrix with gene names
SELECT DISTINCT var_idx, var_name 
FROM anndata_scan_x('pbmc3k.h5ad', 'gene_symbols')
LIMIT 10;

-- Note: ATTACH syntax is planned for future releases
-- Currently use direct table functions: anndata_scan_obs(), anndata_scan_var(), etc.
```

### 2. Basic Cell and Gene Queries

```sql
-- View first 10 cells metadata
SELECT * FROM anndata_scan_obs('pbmc3k.h5ad') LIMIT 10;

-- Count cells by type
SELECT cell_type, COUNT(*) as n_cells
FROM anndata_scan_obs('pbmc3k.h5ad')
GROUP BY cell_type
ORDER BY n_cells DESC;

-- Find highly variable genes
SELECT var_idx,
       _index as gene_name,
       mean_counts,
       n_cells,
       highly_variable
FROM anndata_scan_var('pbmc3k.h5ad')
WHERE highly_variable = 'True'  -- Note: boolean columns may be stored as strings
ORDER BY mean_counts DESC
LIMIT 20;

-- Get gene statistics
SELECT 
    var_idx,
    _index as gene_id,
    n_cells_by_counts,
    pct_dropout_by_counts
FROM anndata_scan_var('pbmc3k.h5ad')
WHERE n_cells_by_counts > 100
ORDER BY n_cells_by_counts DESC;
```

## Single-Cell Analysis Workflows

### 1. Quality Control Metrics

```sql
-- Calculate QC metrics per cell
WITH cell_stats AS (
    SELECT 
        obs_idx,
        COUNT(*) as n_genes_expressed,
        SUM(value) as total_counts,
        AVG(value) as mean_expression
    FROM anndata_scan_x('pbmc3k.h5ad')
    WHERE value > 0
    GROUP BY obs_idx
)
SELECT 
    o.*,
    cs.n_genes_expressed,
    cs.total_counts,
    cs.mean_expression
FROM anndata_scan_obs('pbmc3k.h5ad') o
JOIN cell_stats cs ON o.obs_idx = cs.obs_idx
WHERE cs.n_genes_expressed > 200  -- Filter low-quality cells
  AND cs.n_genes_expressed < 2500;
```

### 2. Gene Expression Analysis

```sql
-- Get expression of specific genes across cell types (using gene names)
SELECT 
    o.cell_type,
    x.var_name as gene,
    AVG(x.value) as mean_expression,
    STDDEV(x.value) as std_expression,
    COUNT(*) as n_cells
FROM anndata_scan_x('pbmc3k.h5ad', 'gene_symbols') x
JOIN anndata_scan_obs('pbmc3k.h5ad') o ON x.obs_idx = o.obs_idx
WHERE x.var_name IN ('CD3D', 'CD4', 'CD8A', 'CD19', 'MS4A1')
GROUP BY o.cell_type, x.var_name
ORDER BY o.cell_type, mean_expression DESC;

-- Query expression for specific var indices
SELECT 
    o.cell_type,
    x.var_idx,
    x.var_name,
    AVG(x.value) as mean_expression
FROM anndata_scan_x('pbmc3k.h5ad', 'gene_symbols') x
JOIN anndata_scan_obs('pbmc3k.h5ad') o ON x.obs_idx = o.obs_idx
WHERE x.var_idx IN (0, 1, 2, 3, 4)  -- First 5 genes
GROUP BY o.cell_type, x.var_idx, x.var_name;
```

### 3. Differential Expression

```sql
-- Compare expression between two cell types
WITH t_cells AS (
    SELECT var_id, AVG(value) as mean_t_cell
    FROM pbmc.main m
    JOIN pbmc.obs o ON m.obs_id = o.obs_id
    WHERE o.cell_type = 'CD4 T'
    GROUP BY var_id
),
b_cells AS (
    SELECT var_id, AVG(value) as mean_b_cell
    FROM pbmc.main m
    JOIN pbmc.obs o ON m.obs_id = o.obs_id
    WHERE o.cell_type = 'B'
    GROUP BY var_id
)
SELECT 
    t.var_id,
    v.gene_name,
    t.mean_t_cell,
    b.mean_b_cell,
    LOG(t.mean_t_cell + 0.001) - LOG(b.mean_b_cell + 0.001) as log_fold_change
FROM t_cells t
JOIN b_cells b ON t.var_id = b.var_id
JOIN pbmc.var v ON t.var_id = v.var_id
WHERE ABS(LOG(t.mean_t_cell + 0.001) - LOG(b.mean_b_cell + 0.001)) > 1
ORDER BY log_fold_change DESC
LIMIT 50;
```

## Working with Dimensional Reductions

### 1. Accessing UMAP/PCA Coordinates

```sql
-- Get UMAP coordinates with cell metadata
SELECT 
    o.obs_idx,
    o.cell_type,
    o.sample_id,
    u.dim_0 as umap_1,
    u.dim_1 as umap_2
FROM anndata_scan_obs('pbmc3k.h5ad') o
JOIN anndata_scan_obsm('pbmc3k.h5ad', 'X_umap') u ON o.obs_idx = u.obs_idx;

-- Get PCA coordinates for specific cells
SELECT 
    p.*
FROM anndata_scan_obsm('pbmc3k.h5ad', 'X_pca') p
JOIN anndata_scan_obs('pbmc3k.h5ad') o ON p.obs_idx = o.obs_idx
WHERE o.cell_type = 'NK'
LIMIT 100;

-- List available dimensional reductions
SELECT value 
FROM anndata_info('pbmc3k.h5ad') 
WHERE property = 'obsm_keys';
```

### 2. Clustering Analysis

```sql
-- Analyze cluster composition
SELECT 
    leiden_cluster,
    cell_type,
    COUNT(*) as n_cells,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY leiden_cluster) as pct_of_cluster
FROM pbmc.obs
GROUP BY leiden_cluster, cell_type
ORDER BY leiden_cluster, pct_of_cluster DESC;

-- Find cluster-specific markers
WITH cluster_expression AS (
    SELECT 
        o.leiden_cluster,
        m.var_id,
        AVG(m.value) as mean_expr,
        COUNT(CASE WHEN m.value > 0 THEN 1 END) * 100.0 / COUNT(*) as pct_expressed
    FROM pbmc.main m
    JOIN pbmc.obs o ON m.obs_id = o.obs_id
    GROUP BY o.leiden_cluster, m.var_id
)
SELECT 
    leiden_cluster,
    var_id,
    mean_expr,
    pct_expressed
FROM cluster_expression
WHERE pct_expressed > 25
ORDER BY leiden_cluster, mean_expr DESC;
```

## Working with Layers

### 1. Comparing Raw and Normalized Data

```sql
-- Attach file with multiple layers
ATTACH 'processed_data.h5ad' AS data (TYPE ANNDATA);

-- Compare raw vs normalized counts for specific genes
SELECT 
    r.obs_id,
    r.var_id,
    r.value as raw_count,
    n.value as normalized_count,
    l.value as log_normalized
FROM data.layers_raw r
JOIN data.layers_normalized n 
    ON r.obs_id = n.obs_id AND r.var_id = n.var_id
JOIN data.layers_lognorm l
    ON r.obs_id = l.obs_id AND r.var_id = l.var_id
WHERE r.var_id IN ('GAPDH', 'ACTB', 'B2M')
  AND r.value > 0
LIMIT 100;
```

### 2. Layer-Specific Analysis

```sql
-- Calculate statistics across different layers
SELECT 
    'raw' as layer,
    COUNT(*) as n_nonzero,
    AVG(value) as mean_value,
    MAX(value) as max_value
FROM data.layers_raw
WHERE value > 0
UNION ALL
SELECT 
    'normalized' as layer,
    COUNT(*) as n_nonzero,
    AVG(value) as mean_value,
    MAX(value) as max_value
FROM data.layers_normalized
WHERE value > 0;
```

## Advanced Queries

### 1. Working with Unstructured Data (uns)

```sql
-- Access simple scalar values from uns
SELECT * FROM pbmc.uns_scalar;

-- Access PCA variance explained (stored as array)
SELECT 
    index as pc_component,
    value as variance_explained
FROM pbmc.uns_pca_variance
ORDER BY index
LIMIT 10;

-- Query rank_genes_groups results (stored as DataFrame)
SELECT 
    gene_name,
    score,
    pval,
    pval_adj,
    log_fold_change
FROM pbmc.uns_rank_genes_groups
WHERE group_name = 'CD4 T'
ORDER BY score DESC
LIMIT 50;

-- Access complex nested parameters as JSON
SELECT 
    key,
    JSON_EXTRACT(value, '$.resolution') as leiden_resolution,
    JSON_EXTRACT(value, '$.n_neighbors') as n_neighbors
FROM pbmc.uns_json
WHERE key LIKE '%params%';
```

### 2. Trajectory Analysis Data

```sql
-- Access pseudotime and trajectory data
SELECT 
    o.obs_id,
    o.cell_type,
    o.pseudotime,
    d.dim_0 as diffmap_1,
    d.dim_1 as diffmap_2,
    u.dim_0 as umap_1,
    u.dim_1 as umap_2
FROM pbmc.obs o
JOIN pbmc.obsm_diffmap d ON o.obs_id = d.obs_id
JOIN pbmc.obsm_umap u ON o.obs_id = u.obs_id
WHERE o.pseudotime IS NOT NULL
ORDER BY o.pseudotime;
```

### 3. Multi-Sample Analysis

```sql
-- Compare expression across samples
SELECT 
    o.sample_id,
    o.batch,
    m.var_id,
    AVG(m.value) as mean_expression,
    STDDEV(m.value) as std_expression,
    COUNT(DISTINCT o.obs_id) as n_cells
FROM pbmc.main m
JOIN pbmc.obs o ON m.obs_id = o.obs_id
WHERE m.var_id IN (
    SELECT var_id FROM pbmc.var 
    WHERE highly_variable = true 
    LIMIT 100
)
GROUP BY o.sample_id, o.batch, m.var_id;

-- Batch effect assessment
WITH batch_stats AS (
    SELECT 
        batch,
        var_id,
        AVG(value) as batch_mean,
        COUNT(*) as n_obs
    FROM pbmc.main m
    JOIN pbmc.obs o ON m.obs_id = o.obs_id
    GROUP BY batch, var_id
)
SELECT 
    var_id,
    MAX(batch_mean) - MIN(batch_mean) as max_batch_difference,
    STDDEV(batch_mean) as batch_std
FROM batch_stats
GROUP BY var_id
HAVING COUNT(DISTINCT batch) > 1
ORDER BY max_batch_difference DESC
LIMIT 20;
```

### 4. Gene Set Analysis

```sql
-- Create temporary gene set table
CREATE TEMPORARY TABLE gene_sets (
    set_name VARCHAR,
    gene_id VARCHAR
);

INSERT INTO gene_sets VALUES
    ('Cell_Cycle', 'MCM5'),
    ('Cell_Cycle', 'PCNA'),
    ('Cell_Cycle', 'TYMS'),
    ('Mitochondrial', 'MT-CO1'),
    ('Mitochondrial', 'MT-CO2'),
    ('Mitochondrial', 'MT-ATP6');

-- Calculate gene set scores
WITH gene_set_expression AS (
    SELECT 
        m.obs_id,
        gs.set_name,
        AVG(m.value) as set_score
    FROM pbmc.main m
    JOIN gene_sets gs ON m.var_id = gs.gene_id
    GROUP BY m.obs_id, gs.set_name
)
SELECT 
    o.obs_id,
    o.cell_type,
    gse.set_name,
    gse.set_score
FROM pbmc.obs o
JOIN gene_set_expression gse ON o.obs_id = gse.obs_id
ORDER BY o.cell_type, gse.set_name;
```

## Performance Optimization Examples

### 1. Using Sparse Matrix Optimization

```sql
-- Check sparsity
SELECT 
    COUNT(*) as total_entries,
    COUNT(CASE WHEN value > 0 THEN 1 END) as nonzero_entries,
    COUNT(CASE WHEN value > 0 THEN 1 END) * 100.0 / COUNT(*) as density_pct
FROM pbmc.main;

-- If sparse (< 10% density), ensure optimization is on
SET anndata_sparse_optimization = true;

-- Efficient query for sparse data
SELECT obs_id, var_id, value
FROM pbmc.main
WHERE value > 0  -- Only non-zero values
  AND var_id IN (SELECT var_id FROM pbmc.var WHERE highly_variable = true);
```

### 2. Chunked Processing for Large Datasets

```sql
-- Process large dataset in chunks
SET anndata_chunk_size = 5000;

-- Create summary statistics in chunks
CREATE TABLE expression_summary AS
WITH RECURSIVE chunks AS (
    SELECT 0 as chunk_id, 0 as start_idx, 5000 as end_idx
    UNION ALL
    SELECT 
        chunk_id + 1,
        start_idx + 5000,
        end_idx + 5000
    FROM chunks
    WHERE start_idx < (SELECT COUNT(DISTINCT obs_id) FROM pbmc.obs)
)
SELECT 
    var_id,
    AVG(value) as mean_expr,
    STDDEV(value) as std_expr,
    MAX(value) as max_expr
FROM pbmc.main
GROUP BY var_id;
```

### 3. Parallel Query Execution

```sql
-- Enable parallel execution
SET threads = 8;
SET anndata_parallel_scan = true;

-- Parallel computation of cell statistics
CREATE TABLE cell_qc_metrics AS
SELECT 
    obs_id,
    COUNT(DISTINCT var_id) FILTER (WHERE value > 0) as n_genes,
    SUM(value) as total_counts,
    SUM(value) FILTER (WHERE var_id LIKE 'MT-%') / NULLIF(SUM(value), 0) as mt_fraction
FROM pbmc.main
GROUP BY obs_id;
```

## Export and Integration

### 1. Export to Parquet

```sql
-- Export expression matrix to Parquet
COPY (SELECT * FROM pbmc.main WHERE value > 0) 
TO 'expression_matrix.parquet' (FORMAT PARQUET);

-- Export with metadata
COPY (
    SELECT 
        m.*,
        o.cell_type,
        o.sample_id,
        v.gene_name
    FROM pbmc.main m
    JOIN pbmc.obs o ON m.obs_id = o.obs_id
    JOIN pbmc.var v ON m.var_id = v.var_id
    WHERE m.value > 0
) TO 'annotated_expression.parquet' (FORMAT PARQUET);
```

### 2. Integration with Other Data

```sql
-- Join with external annotation
CREATE TABLE gene_annotation (
    gene_id VARCHAR,
    pathway VARCHAR,
    go_term VARCHAR
);

-- Load external data
COPY gene_annotation FROM 'gene_pathways.csv' (FORMAT CSV, HEADER);

-- Integrate with AnnData
SELECT 
    v.*,
    ga.pathway,
    ga.go_term
FROM pbmc.var v
LEFT JOIN gene_annotation ga ON v.var_id = ga.gene_id;
```

## Configuration Examples

### 1. Working with Different Gene Identifier Schemes

```sql
-- When var_names contains Ensembl IDs but you have gene symbols in another column
ATTACH 'mouse_brain.h5ad' AS mouse (
    TYPE ANNDATA,
    VAR_ID_COLUMN='ensembl_id',      -- Primary identifier (ENSMUSG...)
    VAR_NAME_COLUMN='gene_symbol'     -- Human-readable names (Sox2, Pax6, etc.)
);

-- Now you can query by either
SELECT * FROM mouse.main 
WHERE var_name = 'Sox2';  -- Query by gene symbol

SELECT * FROM mouse.main 
WHERE var_id = 'ENSMUSG00000074637';  -- Query by Ensembl ID

-- Set defaults for all future attachments
SET anndata_default_var_name_column = 'gene_symbol';
SET anndata_default_var_id_column = 'ensembl_id';
```

### 2. Custom Cell Identifiers

```sql
-- When obs_names are not the barcodes you want to use
ATTACH 'multiplexed.h5ad' AS multi (
    TYPE ANNDATA,
    OBS_ID_COLUMN='cell_barcode',     -- Use barcode as primary ID
    OBS_NAME_COLUMN='sample_cell_id'  -- Use combined sample+cell ID for display
);
```

## Monitoring and Debugging

### 1. Check Extension Status

```sql
-- Version information
PRAGMA anndata_version;

-- Statistics for attached files
PRAGMA anndata_stats;

-- Memory usage
SELECT * FROM duckdb_memory();
```

### 2. Validate Data Integrity

```sql
-- Check for data consistency
SELECT 
    'obs' as table_name,
    COUNT(*) as row_count
FROM pbmc.obs
UNION ALL
SELECT 
    'main_unique_obs' as table_name,
    COUNT(DISTINCT obs_id) as row_count
FROM pbmc.main;

-- Verify dimension consistency
SELECT 
    (SELECT COUNT(*) FROM pbmc.obs) as n_obs_metadata,
    (SELECT COUNT(DISTINCT obs_id) FROM pbmc.main) as n_obs_matrix,
    (SELECT COUNT(*) FROM pbmc.var) as n_var_metadata,
    (SELECT COUNT(DISTINCT var_id) FROM pbmc.main) as n_var_matrix;
```