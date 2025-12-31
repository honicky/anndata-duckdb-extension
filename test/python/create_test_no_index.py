#!/usr/bin/env python3
"""Create test h5ad file without _index datasets (relies on X shape attribute)."""

import h5py
import numpy as np
from scipy import sparse

output_path = "test/data/test_no_index.h5ad"

n_obs = 50
n_vars = 100

# Create sparse CSR matrix
density = 0.1
nnz = int(n_obs * n_vars * density)
rows = np.random.randint(0, n_obs, nnz)
cols = np.random.randint(0, n_vars, nnz)
data = np.random.rand(nnz).astype(np.float32)
X = sparse.csr_matrix((data, (rows, cols)), shape=(n_obs, n_vars))

with h5py.File(output_path, "w") as f:
    # X as sparse CSR - with shape attribute
    x_grp = f.create_group("X")
    x_grp.attrs["encoding-type"] = "csr_matrix"
    x_grp.attrs["encoding-version"] = "0.1.0"
    x_grp.attrs["shape"] = np.array([n_obs, n_vars], dtype=np.int64)
    x_grp.create_dataset("data", data=X.data)
    x_grp.create_dataset("indices", data=X.indices.astype(np.int32))
    x_grp.create_dataset("indptr", data=X.indptr.astype(np.int32))

    # obs group - WITHOUT _index dataset
    obs = f.create_group("obs")
    obs.attrs["encoding-type"] = "dataframe"
    obs.attrs["encoding-version"] = "0.2.0"
    obs.attrs["_index"] = "_index"  # Reference to non-existent dataset
    obs.attrs["column-order"] = np.array(["cell_type"], dtype="S")

    # Add a categorical column to obs
    cell_type_grp = obs.create_group("cell_type")
    cell_type_grp.attrs["encoding-type"] = "categorical"
    cell_type_grp.attrs["encoding-version"] = "0.2.0"
    cell_type_grp.attrs["ordered"] = False
    cell_types = ["TypeA", "TypeB", "TypeC"]
    cell_type_grp.create_dataset("categories", data=np.array(cell_types, dtype="S"))
    cell_type_grp.create_dataset("codes", data=np.array([i % 3 for i in range(n_obs)], dtype=np.int8))

    # var group - WITHOUT _index dataset
    var = f.create_group("var")
    var.attrs["encoding-type"] = "dataframe"
    var.attrs["encoding-version"] = "0.2.0"
    var.attrs["_index"] = "_index"  # Reference to non-existent dataset
    var.attrs["column-order"] = np.array(["gene_name"], dtype="S")

    # Add gene_name column
    gene_name_grp = var.create_group("gene_name")
    gene_name_grp.attrs["encoding-type"] = "categorical"
    gene_name_grp.attrs["encoding-version"] = "0.2.0"
    gene_name_grp.attrs["ordered"] = False
    gene_names = [f"Gene_{i}" for i in range(n_vars)]
    gene_name_grp.create_dataset("categories", data=np.array(gene_names, dtype="S"))
    gene_name_grp.create_dataset("codes", data=np.arange(n_vars, dtype=np.int8))

    # Empty groups for other AnnData components
    for grp_name in ["obsm", "varm", "obsp", "varp", "layers", "uns"]:
        grp = f.create_group(grp_name)
        grp.attrs["encoding-type"] = "dict"
        grp.attrs["encoding-version"] = "0.1.0"

print(f"Created {output_path}")

# Verify structure
with h5py.File(output_path, "r") as f:
    print("\nStructure:")
    print(f"  X shape attr: {f['X'].attrs['shape'][:]}")
    print(f"  obs keys: {list(f['obs'].keys())}")
    print(f"  var keys: {list(f['var'].keys())}")
    print(f"  '_index' in obs: {'_index' in f['obs']}")
    print(f"  '_index' in var: {'_index' in f['var']}")
