#!/usr/bin/env python3
"""Create a test AnnData file with sparse X matrix."""

import numpy as np
import scipy.sparse as sp
import anndata as ad
import pandas as pd

# Set seed for reproducibility
np.random.seed(42)

# Create sparse matrix (10 observations x 20 variables)
# Only ~10% of values are non-zero
n_obs = 10
n_var = 20
density = 0.1

# Generate sparse data
sparse_data = sp.random(n_obs, n_var, density=density, format='csr', random_state=42)
sparse_data.data = np.round(sparse_data.data * 10, 2)  # Scale and round values

# Create observation metadata
obs_df = pd.DataFrame(
    {
        'cell_type': ['TypeA'] * 3 + ['TypeB'] * 3 + ['TypeC'] * 4,
        'batch': ['Batch1'] * 5 + ['Batch2'] * 5,
        'n_genes': np.random.randint(100, 500, n_obs),
    },
    index=[f'Cell_{i:03d}' for i in range(n_obs)],
)

# Create variable metadata
var_df = pd.DataFrame(
    {
        'gene_id': [f'ENSG{i:08d}' for i in range(n_var)],
        'gene_name': [f'Gene_{i:03d}' for i in range(n_var)],
        'highly_variable': np.random.choice([True, False], n_var),
    },
    index=[f'Gene_{i:03d}' for i in range(n_var)],
)

# Create AnnData object with sparse matrix
adata = ad.AnnData(X=sparse_data, obs=obs_df, var=var_df)

# Save to file
adata.write_h5ad('test_sparse.h5ad')
print(f"Created test_sparse.h5ad with sparse matrix ({n_obs} obs x {n_var} var)")
print(f"Sparsity: {1 - sparse_data.nnz / (n_obs * n_var):.1%}")
print(f"Non-zero elements: {sparse_data.nnz}")
