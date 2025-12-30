#!/usr/bin/env python3
"""Create test h5ad file with large categorical columns (>127 categories) and integer categories."""

import anndata as ad
import numpy as np
import pandas as pd

# Create a dataset with enough variables to require int16 codes
n_obs = 50
n_vars = 200  # More than 127 to require int16 codes

# Random expression data
X = np.random.rand(n_obs, n_vars).astype(np.float32)

# Observation metadata
obs = pd.DataFrame(
    {
        "cell_type": pd.Categorical(["TypeA", "TypeB"] * (n_obs // 2)),
    },
    index=[f"cell_{i}" for i in range(n_obs)],
)

# Variable metadata with:
# 1. feature_name as categorical with 200 unique values (requires int16 codes)
# 2. feature_length as categorical with integer values
gene_names = [f"Gene_{i}" for i in range(n_vars)]
gene_lengths = [1000 + i * 10 for i in range(n_vars)]  # Unique integer lengths

var = pd.DataFrame(
    {
        "feature_name": pd.Categorical(gene_names),  # 200 categories -> int16 codes
        "feature_length": pd.Categorical(gene_lengths),  # Integer categories
        "gene_id": [f"ENSG{i:011d}" for i in range(n_vars)],
    },
    index=[f"var_{i}" for i in range(n_vars)],
)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var)

# Save to h5ad
output_path = "test/data/test_large_categorical.h5ad"
adata.write_h5ad(output_path)
print(f"Created {output_path}")

# Verify the structure
import h5py

with h5py.File(output_path, "r") as f:
    print("\nVar structure:")
    for key in f["var"].keys():
        item = f["var"][key]
        if isinstance(item, h5py.Group):
            print(f"  {key}: Group (categorical)")
            for subkey in item.keys():
                subitem = item[subkey]
                print(f"    {subkey}: {subitem.dtype} shape={subitem.shape}")
        else:
            print(f"  {key}: Dataset {item.dtype} shape={item.shape}")
