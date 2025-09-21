# AWS S3 Deployment Setup

This guide explains how to set up AWS S3 deployment for the AnnData DuckDB extension, enabling users to install directly from your S3 bucket.

## Prerequisites

1. An AWS account with S3 access
2. An S3 bucket for hosting extensions
3. AWS IAM user with appropriate permissions

## Step 1: Create an S3 Bucket

```bash
aws s3 mb s3://your-extension-bucket --region us-east-1
```

Enable public read access for the bucket (extensions need to be publicly downloadable):

```bash
aws s3api put-bucket-acl --bucket your-extension-bucket --acl public-read
```

## Step 2: Create IAM User and Policy

Create an IAM user with permissions to upload to the bucket:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-extension-bucket",
        "arn:aws:s3:::your-extension-bucket/*"
      ]
    }
  ]
}
```

## Step 3: Configure GitHub Secrets

Add the following secrets to your GitHub repository (Settings → Secrets and variables → Actions):

- `S3_DEPLOY_ID`: Your AWS Access Key ID
- `S3_DEPLOY_KEY`: Your AWS Secret Access Key
- `S3_REGION`: AWS region (e.g., `us-east-1`)
- `S3_BUCKET`: Your bucket name (e.g., `your-extension-bucket`)

Optional for signed extensions:
- `EXTENSION_SIGNING_PK`: Private key for signing extensions (leave empty for unsigned)

## Step 4: Deploy

The extension automatically deploys to S3 when:
- You bump the version (in VERSION file) and push to main
- The build workflow will create a tag and deploy if AWS credentials are configured

No manual deployment is needed - deployments only happen on version bumps to ensure consistency.

## Step 5: Usage

Once deployed, users can install your extension with:

```sql
-- Set your S3 bucket as the extension repository
SET custom_extension_repository='s3://your-extension-bucket';

-- Install and load
INSTALL anndata;
LOAD anndata;

-- Or as a one-liner
INSTALL anndata FROM 's3://your-extension-bucket';
```

## Directory Structure in S3

The deployment creates the following structure:
```
s3://your-extension-bucket/
├── anndata/
│   └── <extension_version>/
│       └── v1.3.2/
│           ├── linux_amd64/anndata.duckdb_extension.gz
│           ├── linux_arm64/anndata.duckdb_extension.gz
│           ├── osx_amd64/anndata.duckdb_extension.gz
│           ├── osx_arm64/anndata.duckdb_extension.gz
│           └── windows_amd64/anndata.duckdb_extension.gz
```

## Alternative: CloudFlare R2

CloudFlare R2 is S3-compatible and offers free egress. You can use it instead of AWS S3:

1. Create an R2 bucket
2. Generate S3-compatible credentials
3. Use the same GitHub secrets but with R2 credentials
4. Set `S3_REGION` to `auto`
5. Users install with: `INSTALL anndata FROM 'https://<your-r2-domain>.r2.dev'`

## Testing Deployment

To test without actually deploying, the workflow runs in dry-run mode by default. To deploy for real:
1. Set the `DUCKDB_DEPLOY_SCRIPT_MODE` environment variable to `for_real` in the workflow
2. Or manually edit the Deploy workflow when needed