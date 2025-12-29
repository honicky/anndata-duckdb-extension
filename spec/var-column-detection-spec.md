# VAR Column Auto-Detection and User Specification

## Overview

Allow users to specify which var columns contain gene names and IDs via ATTACH options, and auto-detect them using heuristics when not specified.

## Requirements

1. **User specification**:
   ```sql
   ATTACH 'file.h5ad' AS ad (
       TYPE ANNDATA,
       VAR_NAME_COLUMN = 'gene_symbols',
       VAR_ID_COLUMN = 'ensembl_id'
   );
   ```
2. **Auto-detection**: When not specified, scan var columns and pick the best matches
3. **User feedback**: Print to stderr indicating which columns were selected

---

## Current State

| Component | Status | Location |
|-----------|--------|----------|
| Table function param | Exists | `anndata_scan_x(file, var_col)` in scanner.cpp:228-231 |
| View generation | Exists | `GenerateViewSQL()` in storage.cpp:26-53 |
| AttachOptions parsing | Not implemented | storage.cpp:187 (param ignored) |
| Auto-detection | Not implemented | - |

---

## Implementation Plan

### Step 1: Parse ATTACH Options

**File**: `src/anndata_storage.cpp`

Extract `VAR_NAME_COLUMN` and `VAR_ID_COLUMN` from AttachOptions in `AnndataStorageAttach()`.

### Step 2: Store Columns in View Metadata

**File**: `src/include/anndata_storage.hpp`

Update `TableViewInfo` struct to include:
- `var_name_column` - Column for gene names (e.g., "gene_symbols")
- `var_id_column` - Column for gene IDs (e.g., "ensembl_id")

### Step 3: Update View SQL Generation

**File**: `src/anndata_storage.cpp`

Update `GenerateViewSQL()` to pass var_name_column to X and layer functions:

```sql
-- For X table:
-- Before: anndata_scan_x('file.h5ad')
-- After:  anndata_scan_x('file.h5ad', 'gene_symbols')

-- For layers:
-- Before: anndata_scan_layers('file.h5ad', 'raw')
-- After:  anndata_scan_layers('file.h5ad', 'raw', 'gene_symbols')
```

### Step 4: Implement Auto-Detection

**File**: `src/h5_reader_multithreaded.cpp`

Add `DetectVarColumns()` method that returns both name and ID columns.

#### Phase 1: Check Known Column Names

Search for columns matching common names (case-insensitive):

**For gene names** (priority order):
1. gene_symbols, gene_symbol
2. gene_names, gene_name
3. symbol, symbols
4. feature_name
5. name, names

**For gene IDs** (priority order):
1. gene_ids, gene_id
2. ensembl_id, ensembl
3. feature_id
4. id, ids

#### Phase 2: Score Columns by Content

If no known column names found, sample values from each column and score them:

| Pattern | Type | Examples | Score |
|---------|------|----------|-------|
| `^[A-Z][A-Z0-9-]{1,12}$` | Gene symbol | TP53, BRCA1, CD4, HLA-DRB1 | 2 |
| `^ENS[A-Z]*G[0-9]+` | Ensembl gene | ENSG00000141510, ENSMUSG00000059552 | 1 |
| `^[0-9]+$` | Numeric (avoid) | 0, 1, 2... | 0 |

Priority: Gene symbols > Ensembl IDs > _index

#### Phase 3: Fallback

If no suitable column found, default to `_index`.

### Step 5: Print User Feedback

**File**: `src/anndata_storage.cpp`

When auto-detecting, print informational message to stderr:

```
Note: Using var_name='gene_symbols', var_id='ensembl_id'. Override with VAR_NAME_COLUMN/VAR_ID_COLUMN options.
```

---

## Critical Files

| File | Changes |
|------|---------|
| `src/anndata_storage.cpp` | Parse options, call auto-detect, print message, update view SQL |
| `src/include/anndata_storage.hpp` | Add var_name_column and var_id_column to TableViewInfo |
| `src/h5_reader_multithreaded.cpp` | Add DetectVarColumns() method with heuristics |
| `src/include/h5_reader_multithreaded.hpp` | Declare VarColumnDetection struct and DetectVarColumns() |

---

## Usage Examples

```sql
-- Explicit specification (both columns)
ATTACH 'test.h5ad' AS ad (
    TYPE ANNDATA,
    VAR_NAME_COLUMN = 'gene_symbols',
    VAR_ID_COLUMN = 'ensembl_id'
);
SELECT * FROM ad._info WHERE property LIKE 'var_%';

-- Auto-detection (prints note to stderr)
ATTACH 'test.h5ad' AS ad2 (TYPE ANNDATA);
-- stderr: Note: Using var_name='gene_symbols', var_id='ensembl_id'. Override with...

-- Verify X matrix uses correct gene names
SELECT * FROM ad.X LIMIT 5;

-- Override just one column (other auto-detected)
ATTACH 'test.h5ad' AS ad3 (TYPE ANNDATA, VAR_NAME_COLUMN = 'custom_names');
```

---

## Notes

- If auto-detection picks wrong column, user can always override with explicit option
- `_info` table will show the detected/specified columns for transparency
- Both columns default to `_index` if no suitable column found
