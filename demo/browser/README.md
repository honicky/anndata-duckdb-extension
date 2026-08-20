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
- Files are opened by the extension through a ranged HDF5 driver backed by
  `duckdb::FileSystem` (`src/vfd/h5fd_duckdb_fs.cpp`): every browser byte
  source - dropped files, buffers, HTTP/S3 URLs - reaches HDF5 through one
  lazy, positional-read path.

## Remote files (HTTP/S3)

`.open <url>` registers the URL as a **lazy handle** (`registerFileURL`):
queries issue HTTP range requests for exactly the byte ranges they touch.
Measured on a 110 MB file: schema + a 150k-row aggregate = 1 HEAD + ~50 range
requests, ~12 MB transferred, no full download.

Server requirements: `Range` support (206), `Content-Length`, and in a browser
CORS (`Access-Control-Allow-Origin`, allow the `Range` request header, expose
`Content-Range`/`Content-Length`). Servers without Range support fall back to
a whole-file download. The demo opens the database with
`filesystem: { reliableHeadRequests: true, forceFullHTTPReads: false }` -
without these flags this duckdb-wasm build downloads whole files.

The browser fetches remote URLs **directly** - `serve.py` serves only static
files and is never in the data path. That means the remote host must send
CORS headers (and allow the `Range` request header in the preflight): a
browser platform rule that binds every in-browser tool equally.

Known-good: the **public CellxGene Census S3 bucket** - thousands of `.h5ad`
files with correct CORS, verified live here up to **14.6 GB** (schema in
~7 s, direct browser-to-S3 range requests):

```
https://cellxgene-census-public-us-west-2.s3.us-west-2.amazonaws.com/cell-census/2023-07-25/h5ads/<dataset-id>.h5ad
```

Hosts that do not send CORS (e.g. `datasets.cellxgene.cziscience.com` -
preflight returns 403; also `raw.githubusercontent.com`, which sends
`Access-Control-Allow-Origin` but 403s the preflight) **cannot be read by any
in-browser tool**. Options there: the terminal extension (native tools have
no same-origin policy), download once and drag & drop, or ask the host to
enable CORS - it is two headers.

S3 uses the same machinery with SigV4 signing:

```sql
SET s3_region='us-west-2'; SET s3_access_key_id='...'; SET s3_secret_access_key='...';
SELECT * FROM anndata_scan_obs('s3://bucket/file.h5ad') LIMIT 5;
```

(CORS must be configured on the bucket. Verified path: HTTP; S3 shares the
same code path in duckdb-wasm but is not exercised by this repo's tests.)

## Limitations

- Local drag & drop files are **lazy** (`registerFileHandle` + FileReaderSync):
  no size cap - file size is bounded by what queries touch, not memory.
  The "Load sample" button uses `registerFileBuffer` (in-memory) instead.
- CSR expression matrices: selecting one gene still reads most of `X`
  (row-major layout - a file-format property, not an I/O one). Metadata,
  `obs`, `var`, `obsm` queries are the sweet spot for large remote files.
- Browsers pick the `eh` bundle; on very old browsers (mvp) error *messages*
  can be cryptic (upstream duckdb-wasm limitation on side-module throws) -
  successful queries are unaffected.
