#!/bin/bash
# Test script to measure actual query timings for the remote demo
# Run this before generating the VHS tape to get accurate Sleep durations

echo "==================================================================="
echo "Testing AnnData Remote File Query Timings"
echo "==================================================================="
echo ""
echo "This script will:"
echo "  1. Install required extensions"
echo "  2. Attach remote files from HTTP and S3"
echo "  3. Run queries and show timing for each operation"
echo ""
echo "Use these timings to adjust Sleep durations in demo-remote.tape"
echo "Add 1-2 seconds buffer to each timing for the demo"
echo "==================================================================="
echo ""

# Create a temp SQL file
cat > /tmp/anndata_timing_test.sql << 'EOF'
.timer on
.echo on

-- STEP 1: Install extensions
INSTALL httpfs;
LOAD httpfs;
INSTALL anndata FROM community;

-- STEP 2: Attach HTTP file (PBMC blood cells ~24MB)
ATTACH 'https://raw.githubusercontent.com/chanzuckerberg/cellxgene/main/example-dataset/pbmc3k.h5ad'
  AS pbmc (TYPE ANNDATA);

-- STEP 3: Show tables
SHOW ALL TABLES;

-- STEP 4: Query cell count
SELECT COUNT(*) AS total_cells FROM pbmc.obs;

-- STEP 5: Query cell types (pbmc uses 'louvain' column)
SELECT louvain, COUNT(*) as count
FROM pbmc.obs
GROUP BY louvain
ORDER BY count DESC;

-- STEP 6: Attach S3 file (lung cells ~20MB, us-west-2)
SET s3_region = 'us-west-2';
ATTACH 's3://cellxgene-annotation-public/cell_type/tutorial/minilung.h5ad'
  AS lung (TYPE ANNDATA);

-- STEP 7: Show databases
CALL duckdb_databases();

-- STEP 8: Compare counts (blood vs lung)
SELECT 'pbmc (blood)' as tissue, COUNT(*) as cells
FROM pbmc.obs
UNION ALL
SELECT 'lung' as tissue, COUNT(*) as cells
FROM lung.obs;

-- STEP 9: Explore lung cell types
SELECT cell_type, COUNT(*) FROM lung.obs GROUP BY 1;

-- STEP 10: Check var table
SELECT * FROM pbmc.var LIMIT 10;

.quit
EOF

echo "Running DuckDB with timing enabled..."
echo ""

duckdb < /tmp/anndata_timing_test.sql

echo ""
echo "==================================================================="
echo "Timing Test Complete!"
echo "==================================================================="
echo ""
echo "Next steps:"
echo "  1. Review the 'Run Time' for each query above"
echo "  2. Add 1-2 seconds buffer to each timing"
echo "  3. Update the Sleep durations in demo-remote.tape"
echo "  4. Run: vhs demo/demo-remote.tape"
echo ""
echo "Example: If ATTACH took 3.5 seconds, use Sleep 5s or Sleep 6s"
echo "==================================================================="

rm /tmp/anndata_timing_test.sql
