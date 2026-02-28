#!/usr/bin/env python3
"""Create a test AnnData file with a .raw section to test raw data support."""

import anndata as ad
import h5py
import numpy as np
import pandas as pd
import scipy.sparse as sp

np.random.seed(42)

n_obs = 10
n_var = 8  # main vars (fewer)
n_raw_var = 12  # raw vars (more genes)

# Main gene names
gene_names = [f"gene_{i}" for i in range(n_var)]
# Raw gene names (different set, more genes)
raw_gene_names = [f"raw_gene_{i}" for i in range(n_raw_var)]

# Create observation metadata
obs = pd.DataFrame(
    {
        "cell_type": [
            "T_cell",
            "B_cell",
            "T_cell",
            "Monocyte",
            "NK_cell",
            "T_cell",
            "B_cell",
            "Monocyte",
            "NK_cell",
            "T_cell",
        ],
        "batch": ["batch1", "batch1", "batch2", "batch2", "batch1", "batch1", "batch2", "batch2", "batch1", "batch2"],
    },
    index=[f"cell_{i}" for i in range(n_obs)],
)

# Create main variable metadata
var = pd.DataFrame(
    {
        "gene_id": [f"ENSG{i:08d}" for i in range(n_var)],
        "highly_variable": [True, False, True, False, True, False, True, False],
    },
    index=gene_names,
)

# Create normalized X matrix (float, dense)
X = np.random.rand(n_obs, n_var).astype(np.float32)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var)

# Create raw variable metadata
raw_var = pd.DataFrame(
    {
        "gene_id": [f"ENSG{i:08d}" for i in range(100, 100 + n_raw_var)],
        "is_expressed": [True] * 8 + [False] * 4,
    },
    index=raw_gene_names,
)

# Create raw X matrix (sparse CSR with integer-like counts)
raw_X_data = np.random.randint(0, 20, size=(n_obs, n_raw_var)).astype(np.float32)
# Zero out ~30% to make it sparse
raw_X_data[np.random.random(raw_X_data.shape) < 0.3] = 0
raw_X_sparse = sp.csr_matrix(raw_X_data)

# Create raw AnnData and assign
raw_adata = ad.AnnData(X=raw_X_sparse, var=raw_var, obs=obs)
adata.raw = raw_adata

# Save to file
output_path = "test/data/test_raw.h5ad"
adata.write_h5ad(output_path)

# Now add raw/varm using h5py directly (anndata doesn't support raw.varm well)
with h5py.File(output_path, "a") as f:
    if "raw/varm" not in f:
        f.create_group("raw/varm")
    # Add a PCs matrix (n_raw_var x 3)
    pcs_data = np.random.rand(n_raw_var, 3).astype(np.float64)
    f.create_dataset("raw/varm/PCs", data=pcs_data)

print(f"Created {output_path}")
print(f"Main X shape: {adata.X.shape}, n_vars: {n_var}")
print(f"Raw X shape: {adata.raw.X.shape}, n_raw_vars: {n_raw_var}")
print(f"Main gene names: {gene_names}")
print(f"Raw gene names: {raw_gene_names}")

# Print some raw X values for test verification
raw_dense = raw_X_sparse.toarray()
print(f"\nRaw X first 3 rows, first 3 cols:")
for i in range(3):
    print(f"  row {i}: {raw_dense[i, :3].tolist()}")
print(f"Raw X total count: {int(raw_dense.sum())}")
