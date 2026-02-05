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
Set Height 1000
Set Theme "Dracula"
Set TypingSpeed 15ms
Set Padding 20

# Start DuckDB
Type "duckdb"
Enter
Sleep 2s

# Install extensions
Type "-- Install required extensions (only needed the first time)"
Enter
Type "INSTALL anndata FROM community;"
Enter
Type "INSTALL httpfs;"
Enter
Type "LOAD httpfs;"
Enter
Sleep 1s

# Enable timing to show query performance
Type "-- Enable timing to see query performance"
Enter
Type ".timer on"
Enter
Sleep 1s

# Attach HTTP file (PBMC blood cells from GitHub)
Type "-- Attach AnnData file from HTTP (PBMC ~24MB)"
Enter
Type@5ms "ATTACH 'https://raw.githubusercontent.com/chanzuckerberg/cellxgene/main/example-dataset/pbmc3k.h5ad'"
Enter
Type "  AS pbmc (TYPE ANNDATA);"
Enter
Sleep 3s

# Show tables
Type "-- See what tables are available"
Enter
Type "SHOW ALL TABLES;"
Enter
Sleep 2s

# Query cell metadata with timing
Type "-- Query cell metadata (notice the timing!)"
Enter
Type "SELECT COUNT(*) AS total_cells FROM pbmc.obs;"
Enter
Sleep 2s

Type "-- What cell types are present?"
Enter
Type "SELECT louvain, COUNT(*) as count"
Enter
Type "FROM pbmc.obs"
Enter
Type "GROUP BY louvain"
Enter
Type "ORDER BY count DESC;"
Enter
Sleep 2s

# Attach S3 file (lung cells from cellxgene S3)
Type "-- Now attach lung data from S3"
Enter
Type "SET s3_region = 'us-west-2';"
Enter
Sleep 1s
Type@5ms "ATTACH 's3://cellxgene-annotation-public/cell_type/tutorial/minilung.h5ad'"
Enter
Type "  AS lung (TYPE ANNDATA);"
Enter
Sleep 4s

# Show all databases
Type "-- We now have blood (HTTP) and lung (S3) attached"
Enter
Type "CALL duckdb_databases();"
Enter
Sleep 2s

# Query across both
Type "-- Compare cell counts: blood vs lung"
Enter
Type "SELECT 'pbmc (blood)' as tissue,"
Enter
Type "       COUNT(*) as cells"
Enter
Type "FROM pbmc.obs"
Enter
Type "UNION ALL"
Enter
Type "SELECT 'lung' as tissue,"
Enter
Type "       COUNT(*) as cells"
Enter
Type "FROM lung.obs;"
Enter
Sleep 2s

# Exit
Type "-- That's it! Blood (HTTP) + Lung (S3) in one query!"
Enter
Sleep 2s
Type ".quit"
Enter
Sleep 1s
