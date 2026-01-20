# AnnData DuckDB Extension Demo
# Run with: vhs demo/demo.tape

Output demo.mp4

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

# Install extension
Type "-- Only INSTALL the first time you use the extension"
Enter
Type "INSTALL anndata FROM community;"
Enter
Sleep 1s

# Attach file
Type "-- Attach an AnnData file like a database"
Enter
Type@5ms "ATTACH 'anndata:5a6c74b9-da1c-4cef-8fdc-07c7a90089d7.h5ad'"
Enter
Type "  AS ad;"
Enter
Sleep 3s

# Show tables
Type "-- See what tables are available"
Enter
Type "SHOW ALL TABLES;"
Enter
Sleep 3s

# Describe obs
Type "-- Explore cell metadata (obs)"
Enter
Type "DESC ad.obs;"
Enter
Sleep 4s

# Query cell types
Type "-- What cell types are in this dataset?"
Enter
Type "SELECT DISTINCT author_celltype"
Enter
Type "FROM ad.obs;"
Enter
Sleep 4s

# Count
Type "-- How many cells total?"
Enter
Type "SELECT COUNT(*) FROM ad.obs;"
Enter
Sleep 2s

# Describe X matrix
Type "-- The expression matrix has genes as columns"
Enter
Type "DESC ad.X;"
Enter
Sleep 4s

# Query gene expression
Type "-- Query gene expression directly"
Enter
Type "SELECT mean(Xkr4) FROM ad.X;"
Enter
Type "-- This is slow because we are need to scan the whole sparse CSR matrix"
Enter
Sleep 10s

# External data join demo
Type "-- Join with external data sources"
Enter
Type "INSTALL httpfs;"
Enter
Type "LOAD httpfs;"
Enter

Type "-- Load CSV mapping table of mouse genes to human orthologs"
Enter
Type "CREATE TABLE orth AS"
Enter
Type@5ms "  SELECT * FROM read_csv('https://raw.githubusercontent.com/AllenInstitute/GeneOrthology/refs/heads/main/csv/mouse_human_marmoset_macaque_orthologs_20231113.csv');"
Enter
Sleep 2s

Type "-- Join AnnData var with ortholog table"
Enter
Type "SELECT orth.mouse_EnsemblID,"
Enter
Type "       orth.human_EnsemblID"
Enter
Type "FROM orth"
Enter
Type "JOIN ad.var"
Enter
Type "  ON ad.var._index = orth.mouse_EnsemblID"
Enter
Type "LIMIT 5;"
Enter
Sleep 2s

# Exit
Type ".quit"
Enter
Sleep 1s
