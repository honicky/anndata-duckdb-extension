# AnnData DuckDB Extension Demo - S3 Authentication
# Run with: vhs demo/demo-s3-auth.tape
#
# IMPORTANT: Set AWS_PROFILE environment variable BEFORE running:
#   export AWS_PROFILE="your-profile-name"
#
# Your profile name will appear in the video, but NOT your credentials

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
Type "LOAD httpfs;"
Enter
Type "INSTALL anndata FROM community;"
Enter
Sleep 1s

# Create S3 secret using AWS profile
Type "-- Create S3 secret using AWS profile from ~/.aws/credentials"
Enter
Type "CREATE SECRET my_s3 ("
Enter
Type "    TYPE S3,"
Enter
Type "    PROVIDER credential_chain,"
Enter
Type "    CHAIN 'config',"
Enter
Type "    PROFILE 'miratyper',   -- optional, will use 'default'"
Enter
Type "    REGION 'us-west-2'     -- optional, will use profile default"
Enter
Type ");"
Enter
Sleep 2s

# Now attach a private S3 file
Type "-- Now attach a private S3 file (~42MB)"
Enter
Type@5ms "ATTACH 's3://pythiomicsdata/cellxgene_v2/test_v1/17e9d436-a264-4c94-a42d-48c8daf6fdd9_subset.h5ad'"
Enter
Type "  AS cells (TYPE ANNDATA);"
Enter
Sleep 4s

# Query the private data
Type "-- Query the private data"
Enter
Type "SELECT COUNT(*) AS total_cells FROM cells.obs;"
Enter
Sleep 2s

# Show cell metadata
Type "-- Explore cell metadata"
Enter
Type "SELECT * FROM cells.obs LIMIT 5;"
Enter
Sleep 2s

# Show tables
Type "-- See all available tables"
Enter
Type "SHOW ALL TABLES;"
Enter
Sleep 2s

# Exit
Type ".quit"
Enter
Sleep 1s
