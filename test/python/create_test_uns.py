import anndata as ad
import pandas as pd
import numpy as np

# Create a small test dataset
n_obs = 100
n_vars = 50

X = np.random.randn(n_obs, n_vars)

obs = pd.DataFrame(
    {
        'cell_type': np.random.choice(['T cell', 'B cell', 'NK cell'], n_obs),
        'batch': np.random.choice(['batch1', 'batch2'], n_obs),
        'n_genes': np.random.randint(100, 500, n_obs),
    }
)

var = pd.DataFrame(
    {
        'gene_name': [f'Gene_{i}' for i in range(n_vars)],
        'highly_variable': np.random.choice([True, False], n_vars),
        'mean': np.random.rand(n_vars),
        'std': np.random.rand(n_vars),
    }
)

# Create AnnData object
adata = ad.AnnData(X=X, obs=obs, var=var)

# Add various types of uns data
adata.uns['dataset_name'] = 'Test Dataset'
adata.uns['version'] = '1.0.0'
adata.uns['n_cells'] = n_obs
adata.uns['processing_date'] = '2024-01-01'
adata.uns['is_normalized'] = True
adata.uns['normalization_method'] = 'log1p'

# Add arrays
adata.uns['quality_metrics'] = np.array([0.95, 0.87, 0.92, 0.88, 0.91])
adata.uns['batch_effects'] = np.random.rand(10)

# Add nested dictionaries (common in scanpy)
adata.uns['pca'] = {
    'params': {'n_comps': 50, 'use_highly_variable': True, 'random_state': 42},
    'variance': np.random.rand(50),
    'variance_ratio': np.random.rand(50),
}

adata.uns['umap'] = {'params': {'n_neighbors': 15, 'min_dist': 0.1, 'spread': 1.0, 'random_state': 42}}

# Add color palettes (common in scanpy)
adata.uns['cell_type_colors'] = ['#1f77b4', '#ff7f0e', '#2ca02c']
adata.uns['batch_colors'] = ['#d62728', '#9467bd']

# Add a DataFrame (often used for storing marker genes)
adata.uns['rank_genes_groups'] = pd.DataFrame(
    {
        'names': ['CD3D', 'CD3E', 'CD8A', 'CD4', 'CD19'],
        'scores': np.random.rand(5),
        'pvals': np.random.rand(5),
        'pvals_adj': np.random.rand(5),
        'logfoldchanges': np.random.randn(5),
    }
)

# Add clustering parameters
adata.uns['leiden'] = {'params': {'resolution': 1.0, 'n_iterations': -1}}

# Save the file
adata.write('../data/test_uns.h5ad')
print(f"Created test_uns.h5ad with {len(adata.uns)} uns keys:")
for k, v in adata.uns.items():
    if isinstance(v, dict):
        print(f"  {k}: dict with {len(v)} keys")
    elif isinstance(v, pd.DataFrame):
        print(f"  {k}: DataFrame {v.shape}")
    elif isinstance(v, np.ndarray):
        print(f"  {k}: array {v.shape}")
    else:
        print(f"  {k}: {type(v).__name__} = {v}")
