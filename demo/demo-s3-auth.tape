# AnnData DuckDB Extension Demo - S3 Authentication
# Run with: vhs demo/demo-s3-auth.tape
#
# IMPORTANT: Set these environment variables BEFORE running this demo:
#   export AWS_ACCESS_KEY_ID="your-key-id"
#   export AWS_SECRET_ACCESS_KEY="your-secret-key"
#   export AWS_DEFAULT_REGION="us-west-2"  # optional
#
# The actual credentials will NOT appear in the video - only the variable names

Output demo-s3-auth.mp4

# Settings for readability
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
Type "-- Install required extensions"
Enter
Type "INSTALL httpfs;"
Enter
Sleep 1s
Type "LOAD httpfs;"
Enter
Sleep 1s
Type "INSTALL anndata FROM community;"
Enter
Sleep 1s

# Create S3 secret using environment variables
Type "-- Create S3 secret using environment variables"
Enter
Type "-- (Your actual keys won't be shown on screen!)"
Enter
Type "CREATE SECRET my_s3 ("
Enter
Type "    TYPE S3,"
Enter
Type "    KEY_ID '\${AWS_ACCESS_KEY_ID}',"
Enter
Type "    SECRET '\${AWS_SECRET_ACCESS_KEY}',"
Enter
Type "    REGION 'us-west-2'"
Enter
Type ");"
Enter
Sleep 2s

# Show that the secret was created
Type "-- Verify the secret was created"
Enter
Type "SELECT name, type FROM duckdb_secrets();"
Enter
Sleep 3s

# Now attach a private S3 file
Type "-- Now attach a private S3 file (~42MB)"
Enter
Type@5ms "ATTACH 's3://pythiomicsdata/cellxgene_v2/test_v1/17e9d436-a264-4c94-a42d-48c8daf6fdd9_subset.h5ad'"
Enter
Type "  AS cells (TYPE ANNDATA);"
Enter
Sleep 10s

# Query the private data
Type "-- Query the private data"
Enter
Type "SELECT COUNT(*) AS total_cells FROM cells.obs;"
Enter
Sleep 4s

# Show cell metadata
Type "-- Explore cell metadata"
Enter
Type "SELECT * FROM cells.obs LIMIT 5;"
Enter
Sleep 4s

# Show tables
Type "-- See all available tables"
Enter
Type "SHOW ALL TABLES;"
Enter
Sleep 3s

# Clean up
Type "-- Clean up"
Enter
Type "DROP SECRET my_s3;"
Enter
Sleep 1s

# Exit
Type "-- That's it! Private S3 data, credentials stayed hidden."
Enter
Sleep 2s
Type ".quit"
Enter
Sleep 1s
