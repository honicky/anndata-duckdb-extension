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
Type "    SECRET '\${AWS_SECRET_ACCESS_KEY}'"
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
Type "-- Now you can attach private S3 files"
Enter
Type "-- Example: s3://your-bucket/your-file.h5ad"
Enter
Sleep 2s

# Alternative: Attach with inline credentials (also using env vars)
Type "-- Alternative: Inline credentials (still using env vars)"
Enter
Type "ATTACH 's3://your-bucket/data.h5ad'"
Enter
Type "  AS mydata (TYPE ANNDATA,"
Enter
Type "             S3_ACCESS_KEY_ID '\${AWS_ACCESS_KEY_ID}',"
Enter
Type "             S3_SECRET_ACCESS_KEY '\${AWS_SECRET_ACCESS_KEY}');"
Enter
Sleep 2s

# Show public S3 access (no auth needed)
Type "-- For public S3 buckets, no auth needed:"
Enter
Type@5ms "ATTACH 's3://openproblems-bio/resources_test/common/pancreas/dataset.h5ad'"
Enter
Type "  AS public_data (TYPE ANNDATA);"
Enter
Sleep 5s

Type "-- Query the public data"
Enter
Type "SELECT COUNT(*) FROM public_data.obs;"
Enter
Sleep 3s

# Regional endpoints
Type "-- For specific regions, specify the endpoint:"
Enter
Type "CREATE SECRET my_s3_region ("
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

# Clean up
Type "-- Clean up secrets"
Enter
Type "DROP SECRET my_s3;"
Enter
Sleep 1s
Type "DROP SECRET my_s3_region;"
Enter
Sleep 1s

# Exit
Type "-- That's it! Your credentials stayed private."
Enter
Sleep 2s
Type ".quit"
Enter
Sleep 1s
