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
-- Adjust Sleep after line 26 based on this timing
INSTALL httpfs;
LOAD httpfs;
INSTALL anndata FROM community;

-- STEP 2: Attach HTTP file (~20MB)
-- Adjust Sleep after line 47 based on this timing
ATTACH 'https://cellxgene-annotation-public.s3.us-west-2.amazonaws.com/cell_type/tutorial/minilung.h5ad'
  AS lung (TYPE ANNDATA);

-- STEP 3: Show tables
-- Adjust Sleep after line 54 based on this timing
SHOW ALL TABLES;

-- STEP 4: Query cell count
-- Adjust Sleep after line 61 based on this timing
SELECT COUNT(*) AS total_cells FROM lung.obs;

-- STEP 5: Query cell types
-- Adjust Sleep after line 73 based on this timing
SELECT cell_type, COUNT(*) as count
FROM lung.obs
GROUP BY cell_type
ORDER BY count DESC;

-- STEP 6: Attach S3 file
-- Adjust Sleep after line 82 based on this timing
ATTACH 's3://openproblems-bio/resources_test/common/pancreas/dataset.h5ad'
  AS pancreas (TYPE ANNDATA);

-- STEP 7: Show databases
-- Adjust Sleep after line 89 based on this timing
CALL duckdb_databases();

-- STEP 8: Compare counts
-- Adjust Sleep after line 108 based on this timing
SELECT 'lung' as dataset, COUNT(*) as cells
FROM lung.obs
UNION ALL
SELECT 'pancreas' as dataset, COUNT(*) as cells
FROM pancreas.obs;

-- STEP 9: Explore pancreas obs
-- Adjust Sleep after line 115 based on this timing
SELECT * FROM pancreas.obs LIMIT 5;

-- STEP 10: Check var table
-- Adjust Sleep after line 122 based on this timing
SELECT * FROM lung.var LIMIT 10;

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
