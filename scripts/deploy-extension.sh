#!/bin/bash

# Custom deploy script without ACL requirements
# Based on duckdb/scripts/extension-upload-single.sh but without --acl flag

# Usage: ./deploy-extension.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned>

set -e

if [ -z "$8" ]; then
    BASE_EXT_DIR="/tmp/extension"
else
    BASE_EXT_DIR="$8"
fi

if [[ $4 == wasm* ]]; then
  ext="$BASE_EXT_DIR/$1.duckdb_extension.wasm"
else
  ext="$BASE_EXT_DIR/$1.duckdb_extension"
fi

script_dir="$(dirname "$(readlink -f "$0")")"

# calculate SHA256 hash of extension binary
cat $ext > $ext.append

( command -v truncate && truncate -s -256 $ext.append ) || ( command -v gtruncate && gtruncate -s -256 $ext.append ) || exit 1

# No signing for community extensions (just add 256 zeros)
dd if=/dev/zero of=$ext.sign bs=256 count=1

# append signature to extension binary
cat $ext.sign >> $ext.append

# compress extension binary
if [[ $4 == wasm_* ]]; then
  brotli < $ext.append > "$ext.compressed"
else
  gzip < $ext.append > "$ext.compressed"
fi

set -e

# Abort if AWS key is not set
if [ -z "$AWS_ACCESS_KEY_ID" ]; then
    echo "No AWS key found, skipping.."
    exit 0
fi

# upload versioned version
if [[ $7 = 'true' ]]; then
  if [ -z "$3" ]; then
    echo "deploy-extension.sh called with upload_versioned=true but no extension version was passed"
    exit 1
  fi

  if [[ $4 == wasm* ]]; then
    echo "Uploading to: s3://$5/anndata/$2/$3/$4/$1.duckdb_extension.wasm"
    aws s3 cp $ext.compressed s3://$5/anndata/$2/$3/$4/$1.duckdb_extension.wasm --content-encoding br --content-type="application/wasm"
  else
    echo "Uploading to: s3://$5/anndata/$2/$3/$4/$1.duckdb_extension.gz"
    aws s3 cp $ext.compressed s3://$5/anndata/$2/$3/$4/$1.duckdb_extension.gz
  fi
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  if [[ $4 == wasm* ]]; then
    echo "Uploading to: s3://$5/$3/$4/$1.duckdb_extension.wasm"
    aws s3 cp $ext.compressed s3://$5/$3/$4/$1.duckdb_extension.wasm --content-encoding br --content-type="application/wasm"
  else
    echo "Uploading to: s3://$5/$3/$4/$1.duckdb_extension.gz"
    aws s3 cp $ext.compressed s3://$5/$3/$4/$1.duckdb_extension.gz
  fi
fi

echo "✅ Successfully uploaded $1 for $4"