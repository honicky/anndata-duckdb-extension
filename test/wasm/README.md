# WASM tests

Two CI gates protect the wasm builds (`wasm_mvp`, `wasm_eh`):

1. **Symbol contract** (`scripts/wasm_symbol_check.py`) - static proof that the
   side module's imports resolve against the duckdb-wasm main module. The wasm
   link (`emcc -sSIDE_MODULE=2`) does NOT fail on unresolved symbols, so a
   green build can ship an artifact that dies at `LOAD` - which is exactly what
   happened for every wasm artifact published before this gate (issue #24).
   `allowed_undefined.txt` is the reviewed allowlist for deliberate exceptions.

2. **Load smoke** (`run_node.mjs`) - boots `@duckdb/duckdb-wasm` in Node,
   serves the built artifact from a localhost extension repository, and
   asserts INSTALL + LOAD + `anndata_version()` + function registration.

Run locally (after a `make wasm_eh` build with emsdk active):

    cd test/wasm && npm install
    HOME="$(mktemp -d)" node run_node.mjs \
        ../../build/wasm_eh/extension/anndata/anndata.duckdb_extension.wasm \
        wasm_eh v1.5.5 "$(cat ../../VERSION)"

**Version pins**: `@duckdb/duckdb-wasm` in `package.json` must carry the same
DuckDB version as `duckdb_version:` in `MainDistributionPipeline.yml`
(1.33.1-dev64.0 -> v1.5.5). The harness asserts this and fails loudly on
drift. `web-worker` stays pinned at 1.2.0.

Not covered here: `wasm_threads` (no Node COI worker exists - browser tier
only) and any query that opens an .h5ad file (file access needs the
`duckdb::FileSystem` VFD port - see `spec/wasm-support-spec.md`, Phase 2+).
