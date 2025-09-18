#!/usr/bin/env python3
import anndata as ad
import numpy as np
import pandas as pd
import scipy.sparse as sp
import h5py

# Create test data with layers
n_obs = 100
n_var = 50

# Create base AnnData object
obs = pd.DataFrame({
    'cell_type': np.random.choice(['T cell', 'B cell', 'NK cell'], n_obs),
    'n_genes': np.random.randint(100, 1000, n_obs)
}, index=[f'Cell_{i:03d}' for i in range(n_obs)])

var = pd.DataFrame({
    'gene_id': [f'ENSG{i:011d}' for i in range(n_var)],
    'gene_name': [f'Gene_{i:03d}' for i in range(n_var)],
    'highly_variable': np.random.choice([True, False], n_var)
}, index=[f'Gene_{i:03d}' for i in range(n_var)])

# Main X matrix (normalized data)
X = np.random.randn(n_obs, n_var).astype(np.float32)

adata = ad.AnnData(X=X, obs=obs, var=var)

# Add layers with different data types and sparsity
# Raw counts (integer, sparse)
raw_counts = np.random.poisson(5, size=(n_obs, n_var))
adata.layers['raw_counts'] = sp.csr_matrix(raw_counts)

# Log normalized (float32, dense)
log_norm = np.log1p(raw_counts).astype(np.float32)
adata.layers['log_norm'] = log_norm

# Scaled data (float64, dense)
scaled = (X - X.mean(axis=0)) / X.std(axis=0)
adata.layers['scaled'] = scaled.astype(np.float64)

# Binary data (int32, sparse)
binary = (raw_counts > 5).astype(np.int32)
adata.layers['binary'] = sp.csc_matrix(binary)

# Save the file
adata.write_h5ad('test/data/test_layers.h5ad')

# Print layer information
print("Created test_layers.h5ad with layers:")
for name, layer in adata.layers.items():
    if sp.issparse(layer):
        print(f"  {name}: shape={layer.shape}, dtype={layer.dtype}, sparse format={layer.format}")
    else:
        print(f"  {name}: shape={layer.shape}, dtype={layer.dtype}, dense")

# Inspect HDF5 structure
print("\nHDF5 structure:")
with h5py.File('test/data/test_layers.h5ad', 'r') as f:
    def print_structure(name, obj):
        if isinstance(obj, h5py.Group):
            print(f"  Group: {name}")
        elif isinstance(obj, h5py.Dataset):
            print(f"  Dataset: {name}, shape={obj.shape}, dtype={obj.dtype}")
    
    f.visititems(print_structure)

# Print sample values for verification
print("\nSample values for testing:")
print(f"raw_counts[0,0] = {adata.layers['raw_counts'][0, 0]}")
print(f"log_norm[0,0] = {adata.layers['log_norm'][0, 0]:.6f}")
print(f"scaled[0,0] = {adata.layers['scaled'][0, 0]:.6f}")
print(f"binary[0,0] = {adata.layers['binary'][0, 0]}")