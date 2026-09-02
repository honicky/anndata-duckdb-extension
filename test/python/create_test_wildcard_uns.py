#!/usr/bin/env python3
"""
Create small test AnnData files for wildcard (multi-file) testing of
anndata_scan_uns() and anndata_info().

The files are fully deterministic (no random numbers) so that SQL tests can
assert exact row values.

Files created:
- wildcard_uns_sample1.h5ad: n_obs=5, uns keys {shared + alpha_only}
- wildcard_uns_sample2.h5ad: n_obs=6, uns keys {shared + beta_only}
- wildcard_uns_sample3.h5ad: n_obs=7, uns keys {shared + gamma_only}
- wildcard_uns_yempty.h5ad:  n_obs=3, no uns data (empty /uns group); sorts
  after the samples so it sits in the MIDDLE of 'wildcard_uns_*' and at the
  END of 'wildcard_uns_[sy]*'
- wildcard_uns_zlast.h5ad:   n_obs=2, two uns keys; sorts last

Every file has n_vars=3 (Gene_A, Gene_B, Gene_C), a dense float32 X built
from np.arange, and a single categorical obs column 'cell_type'.

Shared uns keys (present in every sample file, with per-file values):
  dataset_name (str), n_cells (int), normalization (str),
  qc_thresholds (float64 array of varying length), pca/params/n_comps (int)

File-specific uns keys:
  sample1: alpha_only = 'only in sample one'   (str)
  sample2: beta_only  = 42                     (int)
  sample3: gamma_only = ['a', 'b', 'c']        (list of str)

No obsm/varm/layers/obsp/varp/raw are written.
"""

import os

import anndata as ad
import numpy as np
import pandas as pd

N_VARS = 3
GENE_NAMES = ['Gene_A', 'Gene_B', 'Gene_C']
CELL_TYPES = ['T cell', 'B cell']

# name -> (n_obs, obs index prefix, uns contents). Values are plain Python /
# numpy objects so that anndata writes them as scalar datasets, 1-D float64
# arrays, nested groups, or (for gamma_only) a 1-D string array.
FILES = {
    'wildcard_uns_sample1': (
        5,
        's1',
        {
            'dataset_name': 'Sample One',
            'n_cells': 5,
            'normalization': 'log1p',
            'qc_thresholds': np.array([200.0, 2500.0]),
            'pca': {'params': {'n_comps': 30}},
            'alpha_only': 'only in sample one',
        },
    ),
    'wildcard_uns_sample2': (
        6,
        's2',
        {
            'dataset_name': 'Sample Two',
            'n_cells': 6,
            'normalization': 'log1p',
            'qc_thresholds': np.array([150.0, 3000.0, 5.0]),
            'pca': {'params': {'n_comps': 50}},
            'beta_only': 42,
        },
    ),
    'wildcard_uns_sample3': (
        7,
        's3',
        {
            'dataset_name': 'Sample Three',
            'n_cells': 7,
            'normalization': 'scran',
            'qc_thresholds': np.array([100.0]),
            'pca': {'params': {'n_comps': 20}},
            'gamma_only': ['a', 'b', 'c'],
        },
    ),
    # No uns data at all: contributes zero rows to a multi-file uns scan
    'wildcard_uns_yempty': (3, 'y', {}),
    # Sorts after the empty file so 'wildcard_uns_*' has an empty file in the middle
    'wildcard_uns_zlast': (2, 'z', {'dataset_name': 'Sample Last', 'n_cells': 2}),
}


def create_file(name: str, n_obs: int, prefix: str, uns: dict, output_dir: str):
    """Create a single deterministic test h5ad file."""
    # Dense, deterministic X: row-major 0, 1, 2, ... as float32
    X = np.arange(n_obs * N_VARS, dtype=np.float32).reshape(n_obs, N_VARS)

    # obs: single categorical column cycling through the cell types
    obs = pd.DataFrame(
        {
            'cell_type': pd.Categorical(
                [CELL_TYPES[i % len(CELL_TYPES)] for i in range(n_obs)],
                categories=CELL_TYPES,
            ),
        },
        index=[f'{prefix}_cell_{i}' for i in range(n_obs)],
    )

    # var: gene_name column equal to the index
    var = pd.DataFrame({'gene_name': GENE_NAMES}, index=GENE_NAMES)

    adata = ad.AnnData(X=X, obs=obs, var=var)

    for key, value in uns.items():
        adata.uns[key] = value

    output_path = os.path.join(output_dir, f'{name}.h5ad')
    adata.write_h5ad(output_path)
    print(f"Created {output_path}")
    print(f"  n_obs: {adata.n_obs}, n_vars: {adata.n_vars}")
    print(f"  uns keys: {sorted(adata.uns.keys())}")

    return adata


def main():
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)

    for name, (n_obs, prefix, uns) in FILES.items():
        create_file(name, n_obs, prefix, uns, output_dir)

    print("\n" + "=" * 60)
    print("Wildcard uns/info test files created!")
    print("=" * 60)
    print("\nExpected results:")
    print("  Shared uns keys: dataset_name, n_cells, normalization, qc_thresholds, pca/params/n_comps")
    print("  File-specific uns keys: alpha_only (1), beta_only (2), gamma_only (3)")
    print("  n_obs per sample file: 5, 6, 7 (total 18); n_vars per file: 3")
    print("  wildcard_uns_yempty: no uns rows; wildcard_uns_zlast: 2 uns rows")


if __name__ == '__main__':
    main()
