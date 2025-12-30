#!/usr/bin/env python3
"""Create test h5ad file with int32 codes (forced, not natural) to test code path."""

import h5py
import numpy as np

output_path = "test/data/test_int32_codes.h5ad"

n_obs = 10
n_vars = 5

with h5py.File(output_path, "w") as f:
    # Create minimal AnnData structure

    # X matrix (dense)
    X = f.create_dataset("X", data=np.random.rand(n_obs, n_vars).astype(np.float32))
    X.attrs["encoding-type"] = "array"
    X.attrs["encoding-version"] = "0.2.0"

    # obs group
    obs = f.create_group("obs")
    obs.attrs["encoding-type"] = "dataframe"
    obs.attrs["encoding-version"] = "0.2.0"
    obs.attrs["_index"] = "_index"
    obs.attrs["column-order"] = []

    # obs _index
    obs_index = [f"cell_{i}" for i in range(n_obs)]
    obs.create_dataset("_index", data=np.array(obs_index, dtype="S"))

    # var group
    var = f.create_group("var")
    var.attrs["encoding-type"] = "dataframe"
    var.attrs["encoding-version"] = "0.2.0"
    var.attrs["_index"] = "_index"
    var.attrs["column-order"] = np.array(["gene_name", "gene_length"], dtype="S")

    # var _index
    var_index = [f"var_{i}" for i in range(n_vars)]
    var.create_dataset("_index", data=np.array(var_index, dtype="S"))

    # gene_name as categorical with int32 codes (forced)
    gene_name_grp = var.create_group("gene_name")
    gene_name_grp.attrs["encoding-type"] = "categorical"
    gene_name_grp.attrs["encoding-version"] = "0.2.0"
    gene_name_grp.attrs["ordered"] = False

    gene_names = [f"Gene_{i}" for i in range(n_vars)]
    gene_name_grp.create_dataset("categories", data=np.array(gene_names, dtype="S"))
    # Force int32 codes even though we only have 5 categories
    gene_name_grp.create_dataset("codes", data=np.arange(n_vars, dtype=np.int32))

    # gene_length as categorical with int32 codes and int64 categories
    gene_length_grp = var.create_group("gene_length")
    gene_length_grp.attrs["encoding-type"] = "categorical"
    gene_length_grp.attrs["encoding-version"] = "0.2.0"
    gene_length_grp.attrs["ordered"] = False

    gene_lengths = np.array([1000 + i * 100 for i in range(n_vars)], dtype=np.int64)
    gene_length_grp.create_dataset("categories", data=gene_lengths)
    # Force int32 codes
    gene_length_grp.create_dataset("codes", data=np.arange(n_vars, dtype=np.int32))

    # Empty groups for other AnnData components
    for grp_name in ["obsm", "varm", "obsp", "varp", "layers", "uns"]:
        grp = f.create_group(grp_name)
        grp.attrs["encoding-type"] = "dict"
        grp.attrs["encoding-version"] = "0.1.0"

print(f"Created {output_path}")

# Verify
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
