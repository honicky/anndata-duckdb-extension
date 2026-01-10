#!/bin/bash
#
# Start MinIO server for local development and testing of remote file access.
# Uses Docker for consistency with CI.
#
# Usage:
#   ./scripts/start-test-server.sh        # Start MinIO and upload test files
#   ./scripts/start-test-server.sh stop   # Stop MinIO
#   ./scripts/start-test-server.sh env    # Print environment variables to export
#
# After starting, export the environment variables printed by this script,
# then run the tests with: make test
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CONTAINER_NAME="anndata-minio"
MINIO_PORT=9000
MINIO_CONSOLE_PORT=9001
BUCKET_NAME="data"

# MinIO credentials
MINIO_ROOT_USER="minioadmin"
MINIO_ROOT_PASSWORD="minioadmin"

print_env() {
    echo ""
    echo "# Export these environment variables to enable remote tests:"
    echo "export ANNDATA_TEST_S3_ENDPOINT=localhost:${MINIO_PORT}"
    echo "export ANNDATA_TEST_S3_ACCESS_KEY=${MINIO_ROOT_USER}"
    echo "export ANNDATA_TEST_S3_SECRET_KEY=${MINIO_ROOT_PASSWORD}"
    echo "export ANNDATA_TEST_HTTP_ENDPOINT=http://localhost:${MINIO_PORT}/${BUCKET_NAME}"
    echo ""
}

check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed."
        echo ""
        echo "Install Docker:"
        echo "  https://docs.docker.com/get-docker/"
        echo ""
        exit 1
    fi

    if ! docker info &> /dev/null; then
        echo "Error: Docker daemon is not running."
        exit 1
    fi
}

stop_minio() {
    if docker ps -q -f name="${CONTAINER_NAME}" | grep -q .; then
        echo "Stopping MinIO container..."
        docker stop "${CONTAINER_NAME}" > /dev/null
        docker rm "${CONTAINER_NAME}" > /dev/null
        echo "MinIO stopped."
    elif docker ps -aq -f name="${CONTAINER_NAME}" | grep -q .; then
        echo "Removing stopped MinIO container..."
        docker rm "${CONTAINER_NAME}" > /dev/null
        echo "MinIO container removed."
    else
        echo "MinIO container not running."
    fi
}

start_minio() {
    check_docker

    # Stop any existing instance
    stop_minio 2>/dev/null || true

    echo "Starting MinIO container..."
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -p ${MINIO_PORT}:9000 \
        -p ${MINIO_CONSOLE_PORT}:9001 \
        -e MINIO_ROOT_USER="${MINIO_ROOT_USER}" \
        -e MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD}" \
        minio/minio server /data --console-address ":9001" \
        > /dev/null

    # Wait for MinIO to start
    echo "Waiting for MinIO to start..."
    for i in {1..30}; do
        if curl -sf "http://localhost:${MINIO_PORT}/minio/health/live" > /dev/null 2>&1; then
            echo "MinIO started successfully!"
            break
        fi
        sleep 0.5
    done

    if ! curl -sf "http://localhost:${MINIO_PORT}/minio/health/live" > /dev/null 2>&1; then
        echo "Error: MinIO failed to start. Check: docker logs ${CONTAINER_NAME}"
        exit 1
    fi

    # Create bucket and upload test files
    setup_bucket_and_files
}

setup_bucket_and_files() {
    echo "Creating bucket and uploading test files..."

    # Use mc with shell entrypoint to run multiple commands
    docker run --rm --network host --entrypoint sh \
        -v "${PROJECT_DIR}/test/data:/testdata:ro" \
        minio/mc -c "
            mc alias set local http://localhost:${MINIO_PORT} ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD}
            mc mb local/${BUCKET_NAME} 2>/dev/null || true
            mc anonymous set download local/${BUCKET_NAME}
            mc cp --recursive /testdata/*.h5ad local/${BUCKET_NAME}/
        "

    echo "Test files available at: s3://${BUCKET_NAME}/"
}

# Main
case "${1:-start}" in
    start)
        start_minio
        print_env
        echo "MinIO is running. Console: http://localhost:${MINIO_CONSOLE_PORT}"
        echo "Stop with: $0 stop"
        ;;
    stop)
        stop_minio
        ;;
    env)
        print_env
        ;;
    *)
        echo "Usage: $0 [start|stop|env]"
        exit 1
        ;;
esac
