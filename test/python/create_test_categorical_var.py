#!/usr/bin/env python3
"""Create test h5ad file with categorical var columns."""

import anndata as ad
import numpy as np
import pandas as pd

# Create a small test dataset
n_obs = 20
n_vars = 10

# Random expression data
X = np.random.rand(n_obs, n_vars).astype(np.float32)

# Observation metadata
obs = pd.DataFrame(
    {
        'cell_type': pd.Categorical(['TypeA', 'TypeB'] * (n_obs // 2)),
    },
    index=[f'cell_{i}' for i in range(n_obs)],
)

# Variable metadata with categorical gene_name column
gene_names = [f'Gene_{i}' for i in range(n_vars)]
gene_ids = [f'ENSG{i:011d}' for i in range(n_vars)]

var = pd.DataFrame(
    {
        'gene_name': pd.Categorical(gene_names),  # Categorical column
        'gene_id': gene_ids,  # Regular string column
    },
    index=[f'var_{i}' for i in range(n_vars)],
)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var)

# Save to h5ad
output_path = 'test/data/test_categorical_var.h5ad'
adata.write_h5ad(output_path)
print(f"Created {output_path}")

# Verify the structure
import h5py

with h5py.File(output_path, 'r') as f:
    print("\nVar structure:")
    for key in f['var'].keys():
        item = f['var'][key]
        if isinstance(item, h5py.Group):
            print(f"  {key}: Group (categorical)")
            for subkey in item.keys():
                print(f"    {subkey}: {item[subkey].shape}")
        else:
            print(f"  {key}: Dataset {item.shape}")
