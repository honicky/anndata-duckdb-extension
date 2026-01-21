# Demo Videos

This directory contains VHS tape scripts for generating demo videos showcasing the AnnData DuckDB extension.

## Prerequisites

- [VHS](https://github.com/charmbracelet/vhs) installed (`brew install vhs`)
- DuckDB installed and in PATH

## Available Demos

### 1. Local File Demo (`demo-local.tape`)

Demonstrates working with local AnnData files:
- Installing the anndata extension
- Attaching a local .h5ad file
- Exploring tables and schemas
- Querying cell metadata and gene expression
- Joining with external CSV data from HTTP

**Prerequisites:**
- A local h5ad file at `/Users/rj/personal/GenePT-tools/data/5a6c74b9-da1c-4cef-8fdc-07c7a90089d7.h5ad`

**Generate:**
```bash
cd demo
vhs demo-local.tape
```

This will create `demo-local.mp4`.

### 2. Remote File Demo (`demo-remote.tape`)

Demonstrates querying AnnData files directly from HTTP and S3:
- Installing httpfs extension for remote file access
- Using DuckDB's `.timer` to measure query performance
- Attaching .h5ad files from HTTP URLs
- Attaching .h5ad files from S3 buckets
- Querying multiple remote datasets simultaneously
- Comparing data across different sources

**Prerequisites:**
- Internet connection (no local files needed!)
- Files used:
  - HTTP: `minilung.h5ad` from CELLxGENE
  - S3: `pancreas/dataset.h5ad` from OpenProblems test data

**Generate:**
```bash
cd demo
vhs demo-remote.tape
```

This will create `demo-remote.mp4`.

**⚠️ IMPORTANT: Test timing first!**

Before generating the final video, run the timing test script:
```bash
cd demo
./test-timing.sh
```

This will run all queries with `.timer on` and show execution times. Add 1-2 seconds buffer to each timing, then update the `Sleep` durations in `demo-remote.tape` accordingly.

Alternatively, test manually:
```bash
duckdb
> .timer on
> INSTALL httpfs; LOAD httpfs; INSTALL anndata FROM community;
> ATTACH 'https://cellxgene-annotation-public.s3.us-west-2.amazonaws.com/cell_type/tutorial/minilung.h5ad' AS lung (TYPE ANNDATA);
> SELECT COUNT(*) FROM lung.obs;
```

### 3. S3 Authentication Demo (`demo-s3-auth.tape`)

Demonstrates how to authenticate with private S3 buckets **without exposing credentials**:
- Creating S3 secrets using environment variables
- Using `${VAR}` syntax to reference credentials
- Verifying secrets were created
- Attaching private S3 files with authentication
- Inline credential specification
- Regional S3 endpoints

**Prerequisites:**
- AWS credentials set as environment variables BEFORE running VHS
- Internet connection

**Generate:**
```bash
# 1. Set your AWS credentials as environment variables
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-west-2"  # optional

# 2. Run VHS - credentials won't appear in the video!
cd demo
vhs demo-s3-auth.tape
```

This will create `demo-s3-auth.mp4`.

**🔒 Security Note:** The video will only show `${AWS_ACCESS_KEY_ID}` and `${AWS_SECRET_ACCESS_KEY}` - the actual credential values are never displayed on screen. DuckDB interpolates the environment variables at runtime.

## Customization

Edit the `.tape` files to adjust:
- `Set FontSize` - text size
- `Set Width/Height` - video dimensions
- `Sleep` durations - pause between commands
- `Type@<speed>` - typing speed for specific commands

## Remote File URLs Used

The remote demo uses these publicly accessible files:

**HTTP:**
- CELLxGENE minilung: https://cellxgene-annotation-public.s3.us-west-2.amazonaws.com/cell_type/tutorial/minilung.h5ad (~20MB)

**S3:**
- OpenProblems pancreas test data: s3://openproblems-bio/resources_test/common/pancreas/dataset.h5ad (small test file)

These files are intentionally small to keep the demo fast and accessible.
