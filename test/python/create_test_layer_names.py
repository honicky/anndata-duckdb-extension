#!/usr/bin/env python3
"""Create a test AnnData file with real gene names and layers to test layer column naming."""

import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp

# Create a small test dataset with real gene names
n_obs = 5
n_var = 10

# Use actual gene names (not generic Gene_000 style)
gene_names = ["A1BG", "BRCA1", "CD3D", "EGFR", "FOXP3", "GAPDH", "HLA-A", "IL2", "JAK2", "KRAS"]

# Create observation metadata
obs = pd.DataFrame(
    {
        "cell_type": ["T_cell", "B_cell", "T_cell", "Monocyte", "NK_cell"],
        "batch": ["batch1", "batch1", "batch2", "batch2", "batch1"],
    },
    index=[f"cell_{i}" for i in range(n_obs)],
)

# Create variable metadata with gene names as index
var = pd.DataFrame(
    {
        "gene_id": [f"ENSG{i:08d}" for i in range(n_var)],
        "highly_variable": [True, False, True, False, True, False, True, False, True, False],
    },
    index=gene_names,
)

# Create dense X matrix
np.random.seed(42)
X = np.random.rand(n_obs, n_var).astype(np.float32)

# Create a layer with integer counts
raw_counts = np.random.randint(0, 100, size=(n_obs, n_var)).astype(np.int32)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var, layers={"raw_counts": raw_counts})

# Save to file
output_path = "test/data/test_layer_gene_names.h5ad"
adata.write_h5ad(output_path)
print(f"Created {output_path}")
print(f"Gene names: {list(adata.var_names)}")
print(f"Layer 'raw_counts' shape: {adata.layers['raw_counts'].shape}")
