#!/usr/bin/env python3
"""Create a small test file with obsp and varp data for testing."""

import anndata
import numpy as np
from scipy import sparse

# Create a small AnnData object
n_obs = 100
n_var = 50

# Create basic data
X = np.random.rand(n_obs, n_var)
adata = anndata.AnnData(X=X)
adata.obs_names = [f'cell_{i}' for i in range(n_obs)]
adata.var_names = [f'gene_{i}' for i in range(n_var)]

# Add some obs and var metadata
adata.obs['cell_type'] = np.random.choice(['A', 'B', 'C'], n_obs)
adata.var['gene_type'] = np.random.choice(['protein', 'lncRNA', 'miRNA'], n_var)

# Create sparse obsp matrices (cell-cell relationships)
print("Creating obsp matrices...")

# 1. Connectivities matrix (sparse, symmetric)
np.random.seed(42)
n_connections = 500
row_idx = np.random.randint(0, n_obs, n_connections)
col_idx = np.random.randint(0, n_obs, n_connections)
values = np.random.rand(n_connections)
connectivities = sparse.csr_matrix((values, (row_idx, col_idx)), shape=(n_obs, n_obs))
adata.obsp['connectivities'] = connectivities

# 2. Distances matrix (sparse, also symmetric)
n_distances = 300
row_idx = np.random.randint(0, n_obs, n_distances)
col_idx = np.random.randint(0, n_obs, n_distances)
values = np.random.rand(n_distances) * 10  # Distances in range [0, 10]
distances = sparse.csr_matrix((values, (row_idx, col_idx)), shape=(n_obs, n_obs))
adata.obsp['distances'] = distances

# Create sparse varp matrices (gene-gene relationships)
print("Creating varp matrices...")

# 1. Gene correlation matrix
n_correlations = 200
row_idx = np.random.randint(0, n_var, n_correlations)
col_idx = np.random.randint(0, n_var, n_correlations)
values = np.random.rand(n_correlations) * 2 - 1  # Correlations in range [-1, 1]
correlations = sparse.csr_matrix((values, (row_idx, col_idx)), shape=(n_var, n_var))
adata.varp['correlations'] = correlations

# 2. Gene coexpression matrix
n_coexp = 150
row_idx = np.random.randint(0, n_var, n_coexp)
col_idx = np.random.randint(0, n_var, n_coexp)
values = np.random.rand(n_coexp)
coexpression = sparse.csr_matrix((values, (row_idx, col_idx)), shape=(n_var, n_var))
adata.varp['coexpression'] = coexpression

# Save the file
output_file = 'test/data/test_obsp_varp.h5ad'
adata.write(output_file)

# Print summary
print(f"\nCreated: {output_file}")
print(f"  n_obs: {n_obs}")
print(f"  n_var: {n_var}")
print("\nobsp matrices:")
for key in adata.obsp.keys():
    mat = adata.obsp[key]
    print(f"  {key}: shape={mat.shape}, nnz={mat.nnz}, format={mat.format}")

print("\nvarp matrices:")
for key in adata.varp.keys():
    mat = adata.varp[key]
    print(f"  {key}: shape={mat.shape}, nnz={mat.nnz}, format={mat.format}")
