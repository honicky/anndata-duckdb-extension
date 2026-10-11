#!/bin/bash
#
# Start local S3 servers (moto) for development and testing of remote file access.
# CI uses the same script, so a local run reproduces it.
#
# Two servers are started, because moto cannot serve authenticated and anonymous requests from one server:
#   127.0.0.1:9000  checks request signatures; used by the s3://data/... tests (wrong credentials get 403)
#   127.0.0.1:9001  open; used by the http://.../data/... tests (anonymous GET with Range)
# Both hold bucket "data" with every test/data/*.h5ad in it.
#
# Usage:
#   ./scripts/start-test-server.sh        # Start both servers and upload the test files
#   ./scripts/start-test-server.sh stop   # Stop them
#   ./scripts/start-test-server.sh env    # Print the environment variables to export
#
# After starting, export the environment variables printed by this script,
# then run the tests with: make test
#
# Needs uv, or python3 with the venv module; the moto virtualenv lives in build/moto-test-server/.
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
STATE_DIR="${PROJECT_DIR}/build/moto-test-server"
VENV_DIR="${STATE_DIR}/venv"
REQUIREMENTS="${SCRIPT_DIR}/requirements-test-server.txt"
CREDENTIALS_FILE="${STATE_DIR}/credentials.env"
PYTHON="${PYTHON:-python3}"

HOST=127.0.0.1
S3_PORT=9000
HTTP_PORT=9001
BUCKET_NAME="data"

print_env() {
    if [ ! -f "${CREDENTIALS_FILE}" ]; then
        echo "Error: no credentials found; start the servers first: $0 start" >&2
        exit 1
    fi
    echo ""
    echo "# Export these environment variables to enable remote tests:"
    echo "export ANNDATA_TEST_S3_ENDPOINT=${HOST}:${S3_PORT}"
    sed 's/^/export /' "${CREDENTIALS_FILE}"
    echo "export ANNDATA_TEST_HTTP_ENDPOINT=http://${HOST}:${HTTP_PORT}/${BUCKET_NAME}"
    echo ""
}

ensure_venv() {
    if [ ! -x "${VENV_DIR}/bin/moto_server" ] || ! cmp -s "${REQUIREMENTS}" "${VENV_DIR}/requirements.installed"; then
        echo "Installing moto into ${VENV_DIR}..."
        rm -rf "${VENV_DIR}"
        if command -v uv &> /dev/null; then
            uv venv --quiet "${VENV_DIR}"
            uv pip install --quiet --python "${VENV_DIR}/bin/python" -r "${REQUIREMENTS}"
        else
            "${PYTHON}" -m venv "${VENV_DIR}"
            "${VENV_DIR}/bin/python" -m pip install --quiet --disable-pip-version-check -r "${REQUIREMENTS}"
        fi
        cp "${REQUIREMENTS}" "${VENV_DIR}/requirements.installed"
    fi
}

stop_server() {
    local name=$1
    local pid_file="${STATE_DIR}/${name}.pid"
    if [ -f "${pid_file}" ]; then
        if kill -0 "$(cat "${pid_file}")" 2>/dev/null; then
            kill "$(cat "${pid_file}")" 2>/dev/null || true
            echo "Stopped ${name} server."
        fi
        rm -f "${pid_file}"
    fi
}

start_server() {
    local name=$1
    local port=$2
    local log_file="${STATE_DIR}/${name}.log"

    nohup "${VENV_DIR}/bin/moto_server" -H "${HOST}" -p "${port}" > "${log_file}" 2>&1 < /dev/null &
    echo $! > "${STATE_DIR}/${name}.pid"

    for _ in {1..60}; do
        if curl -sf "http://${HOST}:${port}/moto-api/" > /dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo "Error: the ${name} server did not start on port ${port}. Log: ${log_file}" >&2
    tail -5 "${log_file}" >&2
    exit 1
}

bootstrap() {
    "${VENV_DIR}/bin/python" "${SCRIPT_DIR}/moto_bootstrap.py" \
        --data-dir "${PROJECT_DIR}/test/data" --bucket "${BUCKET_NAME}" "$@"
}

start_servers() {
    mkdir -p "${STATE_DIR}"
    ensure_venv

    # Stop any existing instance
    stop_server s3
    stop_server http

    echo "Starting moto servers..."
    start_server http "${HTTP_PORT}"
    start_server s3 "${S3_PORT}"

    echo "Uploading test files..."
    bootstrap --endpoint "http://${HOST}:${HTTP_PORT}"
    bootstrap --endpoint "http://${HOST}:${S3_PORT}" --auth > "${CREDENTIALS_FILE}"

    echo "Test files available at: s3://${BUCKET_NAME}/ and http://${HOST}:${HTTP_PORT}/${BUCKET_NAME}/"
}

# Main
case "${1:-start}" in
    start)
        start_servers
        print_env
        echo "Servers are running. Stop with: $0 stop"
        ;;
    stop)
        stop_server s3
        stop_server http
        ;;
    env)
        print_env
        ;;
    *)
        echo "Usage: $0 [start|stop|env]"
        exit 1
        ;;
esac
