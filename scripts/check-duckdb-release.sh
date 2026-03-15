#!/usr/bin/env bash
#
# check-duckdb-release.sh - Check for new DuckDB releases and compatibility status
#
# Usage:
#   ./scripts/check-duckdb-release.sh          # Check current vs latest
#   ./scripts/check-duckdb-release.sh --update  # Print commands to update to latest
#
set -euo pipefail

WORKFLOW_FILE=".github/workflows/MainDistributionPipeline.yml"
NEXT_WORKFLOW_FILE=".github/workflows/DuckDBNextBuild.yml"

# Extract current DuckDB version from the main CI workflow
CURRENT_VERSION=$(grep -m1 'duckdb_version:' "$WORKFLOW_FILE" | sed 's/.*duckdb_version: *//' | tr -d "'\"")
echo "Current DuckDB version (CI): ${CURRENT_VERSION}"

# Check DuckDB submodule version
if [ -d "duckdb" ] && [ -f "duckdb/.git" ]; then
    SUBMODULE_REF=$(cd duckdb && git describe --tags --exact-match 2>/dev/null || cd duckdb && git log -1 --format=%h)
    echo "DuckDB submodule ref:        ${SUBMODULE_REF}"
fi

# Get latest release from GitHub API
if command -v gh &> /dev/null; then
    LATEST_VERSION=$(gh api repos/duckdb/duckdb/releases/latest --jq '.tag_name' 2>/dev/null || echo "unknown")
elif command -v curl &> /dev/null; then
    LATEST_VERSION=$(curl -s https://api.github.com/repos/duckdb/duckdb/releases/latest | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'])" 2>/dev/null || echo "unknown")
else
    echo "Error: Neither 'gh' nor 'curl' found. Cannot check latest release."
    exit 1
fi

echo "Latest DuckDB release:       ${LATEST_VERSION}"
echo ""

if [ "${CURRENT_VERSION}" = "${LATEST_VERSION}" ]; then
    echo "✓ Already on the latest DuckDB release."
else
    echo "⚠ Update available: ${CURRENT_VERSION} → ${LATEST_VERSION}"

    if [ "${1:-}" = "--update" ]; then
        echo ""
        echo "To update, run these commands:"
        echo ""
        echo "  # 1. Update DuckDB submodule"
        echo "  cd duckdb && git fetch origin tag ${LATEST_VERSION} && git checkout ${LATEST_VERSION} && cd .."
        echo ""
        echo "  # 2. Update extension-ci-tools submodule"
        echo "  cd extension-ci-tools && git fetch origin && git checkout origin/main && cd .."
        echo ""
        echo "  # 3. Update version references in CI workflows"
        echo "  sed -i 's/duckdb_version: ${CURRENT_VERSION}/duckdb_version: ${LATEST_VERSION}/g' ${WORKFLOW_FILE}"
        echo "  sed -i 's/${CURRENT_VERSION}/${LATEST_VERSION}/g' ${WORKFLOW_FILE}"
        echo ""
        echo "  # 4. Clean build and test"
        echo "  rm -rf build/release"
        echo "  uv run make"
        echo ""
        echo "  # 5. Run quality checks"
        echo "  uv run make format-fix"
        echo "  uv run make tidy-check"
        echo ""
        echo "  # 6. Commit"
        echo "  git add duckdb extension-ci-tools ${WORKFLOW_FILE}"
        echo "  git commit -m 'chore: upgrade DuckDB to ${LATEST_VERSION}'"
    fi
fi

# Check if duckdb-next-build is passing (if gh is available)
if command -v gh &> /dev/null; then
    echo ""
    echo "--- DuckDB Next Build Status ---"
    # Check the most recent scheduled workflow run
    NEXT_STATUS=$(gh run list --workflow=DuckDBNextBuild.yml --limit=1 --json conclusion,createdAt --jq '.[0] | "\(.conclusion // "in_progress") (\(.createdAt))"' 2>/dev/null || echo "no runs found")
    echo "Latest next-build run: ${NEXT_STATUS}"
fi
