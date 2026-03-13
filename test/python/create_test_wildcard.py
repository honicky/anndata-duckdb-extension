#!/usr/bin/env python3
"""
Create multiple test AnnData files for wildcard query testing.
Creates files with overlapping and non-overlapping columns to test
intersection and union schema modes.

Files created:
- wildcard_sample1.h5ad: genes A-D, obs columns {shared + unique1}
- wildcard_sample2.h5ad: genes B-E, obs columns {shared + unique2}
- wildcard_sample3.h5ad: genes C-F, obs columns {shared + unique3}

Gene intersection: C, D
Gene union: A, B, C, D, E, F
Obs column intersection: shared columns
Obs column union: shared + unique1 + unique2 + unique3
"""

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse
import os

# Set seed for reproducibility
np.random.seed(42)

# Common dimensions
n_obs = 20  # Small for testing
n_common_genes = 2  # C, D are common to all
n_unique_genes = 2  # Each file has 2 unique genes


def create_sample(sample_num: int, gene_names: list, obs_unique_col: str, output_dir: str):
    """Create a single test sample h5ad file."""
    n_genes = len(gene_names)

    # Create sparse X matrix
    X = sparse.random(n_obs, n_genes, density=0.5, format='csr', dtype=np.float32)
    X.data = np.abs(X.data) * 10

    # Create obs DataFrame with shared and unique columns
    cell_types = ['T cell', 'B cell', 'Monocyte']
    obs = pd.DataFrame(
        {
            # Shared columns (present in all files)
            'cell_type': pd.Categorical(np.random.choice(cell_types, n_obs)),
            'n_counts': np.random.randint(1000, 5000, n_obs),
            'sample_id': f'sample{sample_num}',
            # Unique column for this file
            obs_unique_col: np.random.uniform(0, 1, n_obs),
        },
        index=[f's{sample_num}_cell_{i}' for i in range(n_obs)],
    )

    # Create var DataFrame
    var = pd.DataFrame(
        {
            'gene_name': gene_names,
            'highly_variable': np.random.choice([True, False], n_genes),
        },
        index=gene_names,
    )

    # Create AnnData object
    adata = ad.AnnData(X=X, obs=obs, var=var)

    # Add obsm (dimensional reductions) - different dimensions per file
    n_pca_dims = 10 + sample_num  # 11, 12, 13 dimensions
    adata.obsm['X_pca'] = np.random.randn(n_obs, n_pca_dims).astype(np.float32)
    adata.obsm['X_umap'] = np.random.randn(n_obs, 2).astype(np.float32)

    # Add varm
    adata.varm['loadings'] = np.random.randn(n_genes, 5).astype(np.float32)

    # Add layers
    adata.layers['raw'] = sparse.random(n_obs, n_genes, density=0.5, format='csr', dtype=np.float32)
    adata.layers['normalized'] = X.copy()

    # Add obsp (cell-cell matrices)
    adata.obsp['distances'] = sparse.random(n_obs, n_obs, density=0.2, format='csr', dtype=np.float32)

    # Add varp (gene-gene matrices)
    adata.varp['correlations'] = sparse.random(n_genes, n_genes, density=0.3, format='csr', dtype=np.float32)

    # Save
    output_path = os.path.join(output_dir, f'wildcard_sample{sample_num}.h5ad')
    adata.write_h5ad(output_path)
    print(f"Created {output_path}")
    print(f"  n_obs: {adata.n_obs}, n_vars: {adata.n_vars}")
    print(f"  genes: {gene_names}")
    print(f"  obs columns: {list(adata.obs.columns)}")
    print(f"  obsm X_pca dims: {adata.obsm['X_pca'].shape[1]}")

    return adata


def main():
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)

    # Define gene sets with overlap:
    # Sample 1: A, B, C, D
    # Sample 2: B, C, D, E
    # Sample 3: C, D, E, F
    # Intersection: C, D
    # Union: A, B, C, D, E, F

    gene_sets = [
        ['Gene_A', 'Gene_B', 'Gene_C', 'Gene_D'],
        ['Gene_B', 'Gene_C', 'Gene_D', 'Gene_E'],
        ['Gene_C', 'Gene_D', 'Gene_E', 'Gene_F'],
    ]

    unique_obs_cols = ['metric_alpha', 'metric_beta', 'metric_gamma']

    for i, (genes, obs_col) in enumerate(zip(gene_sets, unique_obs_cols), start=1):
        create_sample(i, genes, obs_col, output_dir)

    print("\n" + "=" * 60)
    print("Wildcard test files created!")
    print("=" * 60)
    print("\nExpected results:")
    print("  Gene intersection (C, D): 2 genes")
    print("  Gene union (A-F): 6 genes")
    print("  Obs column intersection: cell_type, n_counts, sample_id")
    print("  Obs column union: above + metric_alpha, metric_beta, metric_gamma")
    print("\nTotal observations across all files: 60 (20 per file)")


if __name__ == '__main__':
    main()
