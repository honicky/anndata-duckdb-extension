# In-browser demo: query .h5ad files with SQL, entirely client-side

A terminal-style page that boots DuckDB-WASM, loads the anndata extension,
and queries AnnData files **without any server-side compute** - the file
never leaves the browser.

![demo](../demo.mp4)

## Run it

```bash
# 1. Build the wasm extension (once; needs emsdk - see spec/wasm-support-spec.md)
make wasm_eh

# 2. Serve the demo (serves the page + the locally built extension)
python3 demo/browser/serve.py       # -> http://127.0.0.1:8110/
```

Then: click **Load sample .h5ad** (registers `test/data/test_small.h5ad`), or
drag & drop any `.h5ad`, and query:

```sql
SELECT * FROM anndata_scan_obs('test_small.h5ad') LIMIT 5;
SELECT * FROM anndata_info('test_small.h5ad');
ATTACH 'test_small.h5ad' AS d (TYPE ANNDATA);
SHOW ALL TABLES;
SELECT cell_type, count(*) FROM d.obs GROUP BY 1;
```

## How it works

- `@duckdb/duckdb-wasm` is loaded from jsDelivr, **pinned to `1.33.1-dev64.0`**,
  the build that carries DuckDB v1.5.5 - the same version the extension is
  built against. The pin must move in lockstep with `duckdb_version`
  (`test/wasm/README.md`).
- The extension is served by `serve.py` from your local `build/<arch>/repository/`
  at `/extension/v1.5.5/<arch>/anndata.duckdb_extension.wasm`, and loaded via
  `SET custom_extension_repository` + `INSTALL anndata; LOAD anndata`
  (`allowUnsignedExtensions` - local builds are unsigned).
- Files are registered with `registerFileBuffer` and opened by the extension
  through DuckDB's filesystem as an in-memory HDF5 CORE image
  (`src/wasm_file_image.cpp`).

## Limitations

- **Whole file in memory** (CORE image; wasm32 address space caps practical
  file size - the demo refuses files > 500 MB). The ranged-read VFD
  (spec Phase 3) lifts this.
- `.open <url>` requires CORS on the remote server and downloads the whole file.
- Browsers pick the `eh` bundle; on very old browsers (mvp) error *messages*
  can be cryptic (upstream duckdb-wasm limitation on side-module throws) -
  successful queries are unaffected.
