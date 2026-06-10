#!/usr/bin/env python3
"""Create a test AnnData file with duplicate var (gene) names.

This reproduces the situation where var_names are not unique (common in
public datasets, e.g. when feature_name is used as both var_id and var_name).
The X wide table must rename duplicate columns with _1, _2, ... suffixes so
that binding a column does not fail with a duplicate-column-name error.
"""

import anndata as ad
import numpy as np
import pandas as pd

n_obs = 4
n_var = 6

# Gene names with intentional duplicates: GENEA appears 3x, GENEB appears 2x.
gene_names = ["GENEA", "GENEB", "GENEA", "UNIQUE1", "GENEB", "GENEA"]

obs = pd.DataFrame(
    {
        "cell_type": ["T_cell", "B_cell", "T_cell", "NK_cell"],
    },
    index=[f"cell_{i}" for i in range(n_obs)],
)

var = pd.DataFrame(
    {
        "gene_id": [f"ENSG{i:08d}" for i in range(n_var)],
    },
    index=pd.Index(gene_names, name="feature_name"),
)

# Deterministic X so the test can assert exact values per (renamed) column.
# Column j filled with value (j + 1) so GENEA=1, GENEB=2, GENEA_1=3, etc.
X = np.tile(np.arange(1, n_var + 1, dtype=np.float32), (n_obs, 1))

adata = ad.AnnData(X=X, obs=obs, var=var)

output_path = "test/data/test_duplicate_var_names.h5ad"
adata.write_h5ad(output_path)
print(f"Created {output_path}")
print(f"Var names (with duplicates): {list(adata.var_names)}")
