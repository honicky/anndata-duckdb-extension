# AnnData DuckDB Extension Demo - Remote Files (HTTP & S3)
# Run with: vhs demo/demo-remote.tape
#
# IMPORTANT: Test this locally first! Adjust Sleep timings based on:
# - Your internet connection speed
# - Actual query execution times (use .timer on to measure)
# - Network latency to HTTP/S3 endpoints

Output demo-remote.mp4

# Settings for readability on LinkedIn
Set Shell bash
Set FontSize 20
Set Width 1200
Set Height 1500
Set Theme "Dracula"
Set TypingSpeed 15ms
Set Padding 20

# Start DuckDB
Type "duckdb"
Enter
Sleep 2s

# Install extensions
Type "-- Install required extensions"
Enter
Type "INSTALL anndata FROM community;"
Enter
Sleep 1s
Type "INSTALL httpfs;"
Enter
Sleep 1s
Type "LOAD httpfs;"
Enter
Sleep 1s

# Enable timing to show query performance
Type "-- Enable timing to see query performance"
Enter
Type ".timer on"
Enter
Sleep 1s

# Attach HTTP file
Type "-- Attach AnnData file directly from HTTP (~20MB)"
Enter
Type@5ms "ATTACH 'https://cellxgene-annotation-public.s3.us-west-2.amazonaws.com/cell_type/tutorial/minilung.h5ad'"
Enter
Type "  AS lung (TYPE ANNDATA);"
Enter
Sleep 10s

# Show tables
Type "-- See what tables are available"
Enter
Type "SHOW ALL TABLES;"
Enter
Sleep 3s

# Query cell metadata with timing
Type "-- Query cell metadata (notice the timing!)"
Enter
Type "SELECT COUNT(*) AS total_cells FROM lung.obs;"
Enter
Sleep 4s

Type "-- What cell types are present?"
Enter
Type "SELECT cell_type, COUNT(*) as count"
Enter
Type "FROM lung.obs"
Enter
Type "GROUP BY cell_type"
Enter
Type "ORDER BY count DESC;"
Enter
Sleep 4s

# Attach S3 file
Type "-- Now attach another file from public S3"
Enter
Type@5ms "ATTACH 's3://openproblems-bio/resources_test/common/pancreas/dataset.h5ad'"
Enter
Type "  AS pancreas (TYPE ANNDATA);"
Enter
Sleep 8s

# Show all databases
Type "-- We now have two AnnData files attached"
Enter
Type "CALL duckdb_databases();"
Enter
Sleep 3s

# Query across both
Type "-- Compare cell counts across datasets"
Enter
Type "SELECT 'lung' as dataset,"
Enter
Type "       COUNT(*) as cells"
Enter
Type "FROM lung.obs"
Enter
Type "UNION ALL"
Enter
Type "SELECT 'pancreas' as dataset,"
Enter
Type "       COUNT(*) as cells"
Enter
Type "FROM pancreas.obs;"
Enter
Sleep 4s

# Explore pancreas obs
Type "-- Explore pancreas cell metadata"
Enter
Type "SELECT * FROM pancreas.obs LIMIT 5;"
Enter
Sleep 3s

# Gene expression query
Type "-- Query gene expression"
Enter
Type "SELECT * FROM lung.var LIMIT 10;"
Enter
Sleep 3s

# Exit
Type "-- That's it! AnnData files from HTTP and S3!"
Enter
Sleep 2s
Type ".quit"
Enter
Sleep 1s
