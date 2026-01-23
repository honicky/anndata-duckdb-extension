#!/usr/bin/env python3
"""
Create a comprehensive test AnnData file with all features for remote testing.
Includes: obs, var, X (sparse), obsm, varm, layers, obsp, varp, uns
"""

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse
import os

# Set seed for reproducibility
np.random.seed(42)

# Dimensions
n_obs = 100
n_vars = 50

# Create sparse X matrix (CSR format)
density = 0.3
X = sparse.random(n_obs, n_vars, density=density, format='csr', dtype=np.float32)
X.data = np.abs(X.data) * 10  # Make values positive counts

# Create obs DataFrame with categorical and numeric columns
cell_types = ['T cell', 'B cell', 'Monocyte', 'NK cell', 'Dendritic']
obs = pd.DataFrame(
    {
        'cell_type': pd.Categorical(np.random.choice(cell_types, n_obs)),
        'n_counts': np.random.randint(1000, 10000, n_obs),
        'n_genes': np.random.randint(100, 500, n_obs),
        'percent_mito': np.random.uniform(0, 0.1, n_obs),
        'batch': pd.Categorical(np.random.choice(['batch1', 'batch2', 'batch3'], n_obs)),
    },
    index=[f'cell_{i}' for i in range(n_obs)],
)

# Create var DataFrame with categorical and numeric columns
gene_types = ['protein_coding', 'lncRNA', 'miRNA', 'pseudogene']
var = pd.DataFrame(
    {
        'gene_name': [f'Gene_{i}' for i in range(n_vars)],
        'gene_type': pd.Categorical(np.random.choice(gene_types, n_vars)),
        'chromosome': pd.Categorical(np.random.choice([f'chr{i}' for i in range(1, 23)], n_vars)),
        'start': np.random.randint(1000000, 100000000, n_vars),
        'end': np.random.randint(1000000, 100000000, n_vars),
        'highly_variable': np.random.choice([True, False], n_vars),
    },
    index=[f'ENSG{i:08d}' for i in range(n_vars)],
)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var)

# Add obsm (dimensional reductions)
adata.obsm['X_pca'] = np.random.randn(n_obs, 50).astype(np.float32)
adata.obsm['X_umap'] = np.random.randn(n_obs, 2).astype(np.float32)
adata.obsm['X_tsne'] = np.random.randn(n_obs, 2).astype(np.float32)

# Add varm (gene embeddings)
adata.varm['PCs'] = np.random.randn(n_vars, 50).astype(np.float32)
adata.varm['gene_loadings'] = np.random.randn(n_vars, 10).astype(np.float32)

# Add layers
adata.layers['raw_counts'] = sparse.random(n_obs, n_vars, density=density, format='csr', dtype=np.float32)
adata.layers['normalized'] = X.copy()
adata.layers['normalized'].data = np.log1p(adata.layers['normalized'].data)

# Add obsp (cell-cell matrices) - sparse
adata.obsp['distances'] = sparse.random(n_obs, n_obs, density=0.1, format='csr', dtype=np.float32)
adata.obsp['connectivities'] = sparse.random(n_obs, n_obs, density=0.1, format='csr', dtype=np.float32)

# Add varp (gene-gene matrices) - sparse
adata.varp['correlations'] = sparse.random(n_vars, n_vars, density=0.1, format='csr', dtype=np.float32)

# Add uns (unstructured)
adata.uns['method'] = 'test_comprehensive'
adata.uns['version'] = '1.0'
adata.uns['neighbors'] = {'n_neighbors': 15, 'method': 'umap'}

# Save
output_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'test_comprehensive.h5ad')
adata.write_h5ad(output_path)
print(f"Created {output_path}")
print(f"  n_obs: {adata.n_obs}")
print(f"  n_vars: {adata.n_vars}")
print(f"  X shape: {adata.X.shape}, sparse: {sparse.issparse(adata.X)}")
print(f"  obsm keys: {list(adata.obsm.keys())}")
print(f"  varm keys: {list(adata.varm.keys())}")
print(f"  layers: {list(adata.layers.keys())}")
print(f"  obsp keys: {list(adata.obsp.keys())}")
print(f"  varp keys: {list(adata.varp.keys())}")
print(f"  uns keys: {list(adata.uns.keys())}")
