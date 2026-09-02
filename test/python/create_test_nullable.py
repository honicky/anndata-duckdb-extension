#!/usr/bin/env python3
"""
Create a small test AnnData file exercising nullable (masked) integer and
boolean columns in obs and var.

pandas extension dtypes 'Int64' / 'Int32' / 'boolean' carry an explicit
missing-value mask. anndata does NOT write these as a plain HDF5 dataset; it
writes an HDF5 GROUP with attribute encoding-type = "nullable-integer" or
"nullable-boolean" containing two datasets:

  values  the underlying integer / bool array (missing slots hold a filler)
  mask    a bool array, True where the value is MISSING

The file is fully deterministic (no random numbers) so that SQL tests can
assert exact row values, including which rows are NULL.

File created:
- test_nullable.h5ad: n_obs=6 (c0..c5), n_vars=3 (g0..g2), dense float32 X
  built from np.arange.

obs columns (in this order):
  n_int      Int64    [10, NA, 30, NA, 50, 60]      -> nullable-integer group
  flag       boolean  [T, NA, F, T, NA, F]          -> nullable-boolean group
  small_int  Int32    [1, 2, NA, 4, 5, 6]           -> nullable-integer group
  score      float64  [1.5, nan, 2.5, 3.5, nan, 4.5] -> plain dataset (NaN, NOT masked)
  cell_type  category ['A','B','A','B','A','B']    -> categorical group
  label      str      ['c0'..'c5']                  -> plain string dataset
  obs_idx    int64    [100..105]                    -> plain dataset whose name clashes with the
                                                      synthetic index column (exposed as obs_idx_)
  u8         UInt8    [200, NA, 255, 0, 1, 2]       -> nullable-integer, unsigned values (UTINYINT)
  u16        UInt16   [60000, 65535, NA, 1, 2, 3]   -> nullable-integer, unsigned (USMALLINT)
  u32        UInt32   [4000000000, NA, 4294967295, 1, 2, 3] -> UINTEGER
  u64        UInt64   [2**64-1, NA, 5, 1, 2, 3]     -> UBIGINT
  nstr       string   ['p', NA, 'q', 'p', 'q', 'r']  -> written as a categorical whose categories are a
                                                      nullable-string-array GROUP (values + mask)
  nstr_uniq  string   ['s0', NA, 's2', 's3', 's4', 's5'] -> nullable-string-array group (values + mask)

test_malformed_group.h5ad has obs columns a_ok, m_broken (encoding-type nullable-integer but NO values
dataset, added with h5py) and zz_last.

A second file, test_nullable_plain.h5ad, carries the same column names with PLAIN (non-nullable)
dtypes so multi-file union/intersection over 'test_nullable*.h5ad' exercises type harmonization:
  n_int int64 [1..6]; flag numpy bool [T,F,T,F,T,F]; small_int int32 [7..12]; u8 uint8 [200..205];
  score float64 [0.5..5.5]; cell_type categorical ['B','B','A','A','B','A']; label str; obs_idx int64 [900..905]

var columns:
  n_cells_expr Int64   [5, NA, 3]                   -> nullable-integer group
  is_hv        boolean [T, F, NA]                   -> nullable-boolean group
  gene_name    str     ['g0','g1','g2']             -> plain string dataset
  var_idx      int64   [7, 8, 9]                    -> name clashes with the synthetic index (var_idx_)
  sym          string  ['S0', NA, 'S2']              -> categorical with nullable-string-array categories;
                                                      used as var_name_column for anndata_scan_x
layers: counts_u32 CSR uint32 with values above 2^31 (4000000000, 4294967295, 2147483648)
uns: big_u64 = 2^64-1 (uint64 scalar), u64_arr = [2^64-1, 5]

No obsm/varm/obsp/varp/raw are written. The nullable-string columns need
anndata.settings.allow_write_nullable_strings = True (anndata >= 0.11).
"""

import os

import anndata as ad
import numpy as np
import pandas as pd
from scipy import sparse

N_OBS = 6
N_VARS = 3
OBS_NAMES = [f'c{i}' for i in range(N_OBS)]
VAR_NAMES = [f'g{i}' for i in range(N_VARS)]

# obs column definitions, in the order they should appear in the DataFrame
OBS_N_INT = [10, None, 30, None, 50, 60]
OBS_FLAG = [True, None, False, True, None, False]
OBS_SMALL_INT = [1, 2, None, 4, 5, 6]
OBS_SCORE = [1.5, np.nan, 2.5, 3.5, np.nan, 4.5]
OBS_CELL_TYPE = ['A', 'B', 'A', 'B', 'A', 'B']
OBS_OBS_IDX = [100, 101, 102, 103, 104, 105]  # a user column literally named obs_idx
OBS_U8 = [200, None, 255, 0, 1, 2]
OBS_U16 = [60000, 65535, None, 1, 2, 3]
OBS_U32 = [4000000000, None, 4294967295, 1, 2, 3]
OBS_U64 = [2**64 - 1, None, 5, 1, 2, 3]
OBS_NSTR = ['p', None, 'q', 'p', 'q', 'r']
OBS_NSTR_UNIQ = ['s0', None, 's2', 's3', 's4', 's5']

# var column definitions
VAR_N_CELLS_EXPR = [5, None, 3]
VAR_IS_HV = [True, False, None]
VAR_VAR_IDX = [7, 8, 9]  # a user column literally named var_idx
VAR_SYM = ['S0', None, 'S2']  # nullable string, used as the X column-name column in tests


def main():
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(output_dir, exist_ok=True)

    # Dense, deterministic X: row-major 0, 1, 2, ... as float32
    X = np.arange(N_OBS * N_VARS, dtype=np.float32).reshape(N_OBS, N_VARS)

    obs = pd.DataFrame(
        {
            'n_int': pd.array(OBS_N_INT, dtype='Int64'),
            'flag': pd.array(OBS_FLAG, dtype='boolean'),
            'small_int': pd.array(OBS_SMALL_INT, dtype='Int32'),
            'score': np.array(OBS_SCORE, dtype=np.float64),
            'cell_type': pd.Categorical(OBS_CELL_TYPE),
            'label': OBS_NAMES,
            'obs_idx': np.array(OBS_OBS_IDX, dtype=np.int64),
            'u8': pd.array(OBS_U8, dtype='UInt8'),
            'u16': pd.array(OBS_U16, dtype='UInt16'),
            'u32': pd.array(OBS_U32, dtype='UInt32'),
            'u64': pd.array(OBS_U64, dtype='UInt64'),
            'nstr': pd.array(OBS_NSTR, dtype='string'),
            'nstr_uniq': pd.array(OBS_NSTR_UNIQ, dtype='string'),
        },
        index=OBS_NAMES,
    )

    var = pd.DataFrame(
        {
            'n_cells_expr': pd.array(VAR_N_CELLS_EXPR, dtype='Int64'),
            'is_hv': pd.array(VAR_IS_HV, dtype='boolean'),
            'gene_name': VAR_NAMES,
            'var_idx': np.array(VAR_VAR_IDX, dtype=np.int64),
            'sym': pd.array(VAR_SYM, dtype='string'),
        },
        index=VAR_NAMES,
    )

    adata = ad.AnnData(X=X, obs=obs, var=var)
    # A CSR layer of uint32 counts with values beyond the signed 32-bit range, and a uint64 uns scalar
    adata.layers['counts_u32'] = sparse.csr_matrix(
        np.array([[0, 4000000000, 4294967295], [0, 0, 3], [2147483648, 0, 0], [0, 0, 0], [0, 0, 0], [0, 7, 0]], dtype=np.uint32)
    )
    adata.uns['big_u64'] = np.uint64(2**64 - 1)
    adata.uns['u64_arr'] = np.array([2**64 - 1, 5], dtype=np.uint64)

    # pandas 'string' columns with NA are only writable with this setting; repeated values become a
    # categorical whose categories are a nullable-string-array group, unique values a plain
    # nullable-string-array group.
    ad.settings.allow_write_nullable_strings = True
    output_path = os.path.join(output_dir, 'test_nullable.h5ad')
    adata.write_h5ad(output_path)

    # Plain-typed twin for multi-file harmonization tests
    plain_obs = pd.DataFrame(
        {
            'n_int': np.arange(1, 7, dtype=np.int64),
            'flag': np.array([True, False, True, False, True, False]),
            'small_int': np.arange(7, 13, dtype=np.int32),
            'u8': np.arange(200, 206, dtype=np.uint8),
            'score': np.arange(6, dtype=np.float64) + 0.5,
            'cell_type': pd.Categorical(['B', 'B', 'A', 'A', 'B', 'A']),
            'label': [f'p{i}' for i in range(N_OBS)],
            'obs_idx': np.arange(900, 906, dtype=np.int64),
        },
        index=[f'q{i}' for i in range(N_OBS)],
    )
    plain = ad.AnnData(X=X, obs=plain_obs, var=pd.DataFrame({'gene_name': VAR_NAMES}, index=VAR_NAMES))
    plain_path = os.path.join(output_dir, 'test_nullable_plain.h5ad')
    plain.write_h5ad(plain_path)
    print(f"Created {plain_path} (plain-typed twin)")

    # A deliberately malformed file: a group that claims encoding-type nullable-integer but has no
    # values dataset, sitting between two good columns. Discovery must keep the later column and
    # the broken one must read as NULL rather than failing the scan.
    import h5py

    broken_path = os.path.join(output_dir, 'test_malformed_group.h5ad')
    ad.AnnData(
        X=X,
        obs=pd.DataFrame({'a_ok': np.arange(N_OBS, dtype=np.int64), 'zz_last': OBS_NAMES}, index=OBS_NAMES),
        var=pd.DataFrame({'gene_name': VAR_NAMES}, index=VAR_NAMES),
    ).write_h5ad(broken_path)
    with h5py.File(broken_path, 'a') as f:
        g = f['obs'].create_group('m_broken')
        g.attrs['encoding-type'] = 'nullable-integer'
        g.attrs['encoding-version'] = '0.1.0'
        g.create_dataset('mask', data=np.zeros(N_OBS, dtype=bool))
        f['obs'].attrs['column-order'] = np.array(['a_ok', 'm_broken', 'zz_last'], dtype=object)
    print(f"Created {broken_path} (malformed nullable group)")

    print(f"Created {output_path}")
    print(f"  n_obs: {adata.n_obs}, n_vars: {adata.n_vars}")
    print(f"  obs dtypes:\n{adata.obs.dtypes.to_string()}")
    print(f"  var dtypes:\n{adata.var.dtypes.to_string()}")

    print("\n" + "=" * 60)
    print("Nullable-column test file created!")
    print("=" * 60)
    print("\nExpected results:")
    print("  obs.n_int     : 10, NULL, 30, NULL, 50, 60   (BIGINT)")
    print("  obs.flag      : true, NULL, false, true, NULL, false   (BOOLEAN)")
    print("  obs.small_int : 1, 2, NULL, 4, 5, 6   (INTEGER)")
    print("  obs.score     : 1.5, nan, 2.5, 3.5, nan, 4.5   (DOUBLE, NaN not NULL)")
    print("  var.n_cells_expr : 5, NULL, 3   (BIGINT)")
    print("  var.is_hv        : true, false, NULL   (BOOLEAN)")
    print("  obs.obs_idx_     : 100..105 (user column), obs_idx: 0..5 (synthetic index)")
    print("  var.var_idx_     : 7, 8, 9 (user column), var_idx: 0..2 (synthetic index)")
    print("  obs.u8/u16/u32/u64: UTINYINT/USMALLINT/UINTEGER/UBIGINT with NULLs, values unclamped")
    print("  obs.nstr         : p, NULL, q, p, q, r (VARCHAR); obs.nstr_uniq: s0, NULL, s2, s3, s4, s5")


if __name__ == '__main__':
    main()
