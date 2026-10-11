"""Populate a running moto_server with the remote-access test data.

Used by scripts/start-test-server.sh (local development) and by the "Remote Access Tests" job in
.github/workflows/MainDistributionPipeline.yml.

Creates a bucket, uploads every test/data/*.h5ad into it, and either

  --auth   creates an IAM user with an S3 policy and turns on moto's signature checking, so a request signed with
           the wrong credentials is refused with 403 (test/sql/remote/s3_error_messages.test relies on that), or
  (default) leaves the server open and uploads the files with a public-read ACL, so anonymous HEAD, GET and Range
           requests work (test/sql/remote/http_basic.test). moto refuses anonymous reads of private objects with 403.

moto cannot do both on one server: with signature checking on, an anonymous GET fails with a 500 because moto reads
the Authorization header unconditionally. Hence two servers.

With --auth the generated credentials are printed as KEY=VALUE lines on stdout.
"""

import argparse
import glob
import json
import os
import sys

import boto3
import requests

REGION = "us-east-1"
USER = "anndata-test"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--endpoint", required=True, help="e.g. http://127.0.0.1:9000")
    parser.add_argument("--bucket", default="data")
    parser.add_argument(
        "--data-dir", required=True, help="directory holding the .h5ad files to upload"
    )
    parser.add_argument(
        "--auth",
        action="store_true",
        help="create an IAM user and enforce signature checking",
    )
    args = parser.parse_args()

    # While signature checking is off (moto's default) any credentials are accepted
    client_kwargs = dict(
        endpoint_url=args.endpoint,
        region_name=REGION,
        aws_access_key_id="bootstrap",
        aws_secret_access_key="bootstrap",
    )
    s3 = boto3.client("s3", **client_kwargs)
    s3.create_bucket(Bucket=args.bucket)

    files = sorted(glob.glob(os.path.join(args.data_dir, "*.h5ad")))
    if not files:
        print(f"error: no .h5ad files in {args.data_dir}", file=sys.stderr)
        return 1
    extra_args = {} if args.auth else {"ACL": "public-read"}
    for path in files:
        s3.upload_file(path, args.bucket, os.path.basename(path), ExtraArgs=extra_args)
    print(f"uploaded {len(files)} files to s3://{args.bucket}/", file=sys.stderr)

    if not args.auth:
        return 0

    iam = boto3.client("iam", **client_kwargs)
    iam.create_user(UserName=USER)
    iam.put_user_policy(
        UserName=USER,
        PolicyName="s3-all",
        PolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}],
            }
        ),
    )
    key = iam.create_access_key(UserName=USER)["AccessKey"]

    # Every request from here on must be signed by a known key. The count is the number of requests that are still
    # let through unchecked, so 0 means all of them are checked.
    response = requests.post(
        args.endpoint + "/moto-api/reset-auth", data=b"0", timeout=30
    )
    response.raise_for_status()

    print(f"ANNDATA_TEST_S3_ACCESS_KEY={key['AccessKeyId']}")
    print(f"ANNDATA_TEST_S3_SECRET_KEY={key['SecretAccessKey']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
