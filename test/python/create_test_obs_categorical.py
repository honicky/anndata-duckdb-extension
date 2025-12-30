#!/usr/bin/env python3
"""Create test h5ad file with large categorical columns in obs (int16/int32 codes)."""

import h5py
import numpy as np

output_path = "test/data/test_obs_categorical.h5ad"

n_obs = 200  # Enough to require int16 codes for some categoricals
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
    obs.attrs["column-order"] = np.array(["cell_type", "cell_size", "cluster_id"], dtype="S")

    # obs _index
    obs_index = [f"cell_{i}" for i in range(n_obs)]
    obs.create_dataset("_index", data=np.array(obs_index, dtype="S"))

    # cell_type as categorical with int16 codes (200 unique values)
    cell_type_grp = obs.create_group("cell_type")
    cell_type_grp.attrs["encoding-type"] = "categorical"
    cell_type_grp.attrs["encoding-version"] = "0.2.0"
    cell_type_grp.attrs["ordered"] = False

    cell_types = [f"CellType_{i}" for i in range(n_obs)]
    cell_type_grp.create_dataset("categories", data=np.array(cell_types, dtype="S"))
    cell_type_grp.create_dataset("codes", data=np.arange(n_obs, dtype=np.int16))

    # cell_size as categorical with int32 codes and int64 categories
    cell_size_grp = obs.create_group("cell_size")
    cell_size_grp.attrs["encoding-type"] = "categorical"
    cell_size_grp.attrs["encoding-version"] = "0.2.0"
    cell_size_grp.attrs["ordered"] = False

    cell_sizes = np.array([100 + i * 5 for i in range(n_obs)], dtype=np.int64)
    cell_size_grp.create_dataset("categories", data=cell_sizes)
    cell_size_grp.create_dataset("codes", data=np.arange(n_obs, dtype=np.int32))

    # cluster_id as categorical with int8 codes (only 10 clusters, repeating)
    cluster_grp = obs.create_group("cluster_id")
    cluster_grp.attrs["encoding-type"] = "categorical"
    cluster_grp.attrs["encoding-version"] = "0.2.0"
    cluster_grp.attrs["ordered"] = False

    clusters = [f"Cluster_{i}" for i in range(10)]
    cluster_grp.create_dataset("categories", data=np.array(clusters, dtype="S"))
    cluster_grp.create_dataset("codes", data=np.array([i % 10 for i in range(n_obs)], dtype=np.int8))

    # var group
    var = f.create_group("var")
    var.attrs["encoding-type"] = "dataframe"
    var.attrs["encoding-version"] = "0.2.0"
    var.attrs["_index"] = "_index"
    var.attrs["column-order"] = []

    var_index = [f"gene_{i}" for i in range(n_vars)]
    var.create_dataset("_index", data=np.array(var_index, dtype="S"))

    # Empty groups for other AnnData components
    for grp_name in ["obsm", "varm", "obsp", "varp", "layers", "uns"]:
        grp = f.create_group(grp_name)
        grp.attrs["encoding-type"] = "dict"
        grp.attrs["encoding-version"] = "0.1.0"

print(f"Created {output_path}")

# Verify
with h5py.File(output_path, "r") as f:
    print("\nObs structure:")
    for key in f["obs"].keys():
        item = f["obs"][key]
        if isinstance(item, h5py.Group):
            print(f"  {key}: Group (categorical)")
            for subkey in item.keys():
                subitem = item[subkey]
                print(f"    {subkey}: {subitem.dtype} shape={subitem.shape}")
        else:
            print(f"  {key}: Dataset {item.dtype} shape={item.shape}")
