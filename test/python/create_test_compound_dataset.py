#!/usr/bin/env python3
"""Create test h5ad file with older AnnData format where obs/var are compound datasets.

This format was used in older versions of AnnData where observations and variables
metadata are stored as HDF5 compound datasets instead of groups with separate datasets
per column.
"""

import h5py
import numpy as np

output_path = "test/data/test_compound_dataset.h5ad"

n_obs = 50
n_vars = 20

# Create compound dtype for obs
obs_dtype = np.dtype(
    [
        ('_index', 'S32'),  # Cell ID (fixed-length string)
        ('n_genes', '<i4'),  # Number of genes detected (int32)
        ('total_counts', '<f8'),  # Total UMI counts (float64)
        ('pct_mito', '<f4'),  # Percent mitochondrial (float32)
    ]
)

# Create compound dtype for var
var_dtype = np.dtype(
    [
        ('_index', 'S32'),  # Gene ID (fixed-length string)
        ('gene_name', 'S32'),  # Gene symbol (fixed-length string)
        ('n_cells', '<i4'),  # Number of cells expressing (int32)
        ('mean_expr', '<f8'),  # Mean expression (float64)
    ]
)

with h5py.File(output_path, "w") as f:
    # X matrix (dense)
    np.random.seed(42)
    X_data = np.random.rand(n_obs, n_vars).astype(np.float32)
    X = f.create_dataset("X", data=X_data)
    X.attrs["encoding-type"] = "array"
    X.attrs["encoding-version"] = "0.2.0"

    # Create obs as a compound dataset (older format)
    obs_data = np.zeros(n_obs, dtype=obs_dtype)
    for i in range(n_obs):
        obs_data[i]['_index'] = f"cell_{i:04d}".encode('utf-8')
        obs_data[i]['n_genes'] = 100 + i * 2
        obs_data[i]['total_counts'] = 1000.0 + i * 50.5
        obs_data[i]['pct_mito'] = 0.5 + i * 0.1

    obs_ds = f.create_dataset("obs", data=obs_data)
    # Note: No encoding-type attribute for compound datasets in older format

    # Create var as a compound dataset (older format)
    var_data = np.zeros(n_vars, dtype=var_dtype)
    for i in range(n_vars):
        var_data[i]['_index'] = f"ENSG{i:08d}".encode('utf-8')
        var_data[i]['gene_name'] = f"Gene{i}".encode('utf-8')
        var_data[i]['n_cells'] = 10 + i * 3
        var_data[i]['mean_expr'] = 0.1 + i * 0.05

    var_ds = f.create_dataset("var", data=var_data)

    # Empty groups for other AnnData components
    for grp_name in ["obsm", "varm", "obsp", "varp", "layers", "uns"]:
        grp = f.create_group(grp_name)
        grp.attrs["encoding-type"] = "dict"
        grp.attrs["encoding-version"] = "0.1.0"

print(f"Created {output_path}")

# Verify the structure
with h5py.File(output_path, "r") as f:
    print("\nFile structure:")
    print(f"  /X: {f['X'].dtype} shape={f['X'].shape}")

    obs = f['obs']
    print(f"\n  /obs: {type(obs).__name__}")
    if isinstance(obs, h5py.Dataset):
        print(f"    dtype: {obs.dtype}")
        print(f"    shape: {obs.shape}")
        print(f"    fields: {list(obs.dtype.names)}")
        print(f"    First record: {obs[0]}")
        print(f"    Last record: {obs[-1]}")
    else:
        print("    ERROR: /obs is not a Dataset!")

    var = f['var']
    print(f"\n  /var: {type(var).__name__}")
    if isinstance(var, h5py.Dataset):
        print(f"    dtype: {var.dtype}")
        print(f"    shape: {var.shape}")
        print(f"    fields: {list(var.dtype.names)}")
        print(f"    First record: {var[0]}")
        print(f"    Last record: {var[-1]}")
    else:
        print("    ERROR: /var is not a Dataset!")
