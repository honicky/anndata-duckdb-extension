#!/usr/bin/env python3
"""Static file server for the in-browser AnnData demo. Nothing dynamic.

Serves this directory, plus two mapped routes (still just files on disk):
  /extension/<duckdb_ver>/<arch>/anndata.duckdb_extension.wasm
      -> ../../build/<arch>/repository/<duckdb_ver>/<arch>/... (local wasm build)
  /data/<name>.h5ad
      -> ../../test/data/<name>.h5ad (sample files for the "load sample" button)

Any static host (GitHub Pages, S3, ...) can replace this script by serving the
same files. Remote .h5ad URLs are fetched by the BROWSER directly, which works
when the host sends CORS headers (e.g. the public CellxGene Census S3 bucket);
hosts without CORS cannot be read by any in-browser tool - use the terminal
extension for those.

No COOP/COEP headers are needed: the demo uses the mvp/eh duckdb-wasm bundles,
never the COI (threads) one.

Usage:  python3 serve.py [port]     (default 8110)
"""
import http.server
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))


class Handler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".wasm": "application/wasm",
        ".mjs": "text/javascript",
        ".h5ad": "application/octet-stream",
    }

    def translate_path(self, path):
        path = path.split("?", 1)[0].split("#", 1)[0]
        m = re.match(r"^/extension/(v[\w.]+)/(wasm_\w+)/(anndata\.duckdb_extension\.wasm)$", path)
        if m:
            ver, arch, name = m.groups()
            return os.path.join(REPO, "build", arch, "repository", ver, arch, name)
        m = re.match(r"^/data/([\w.-]+\.h5ad)$", path)
        if m:
            return os.path.join(REPO, "test", "data", m.group(1))
        # default: serve demo/browser/
        rel = path.lstrip("/") or "index.html"
        return os.path.join(HERE, rel)

    def end_headers(self):
        # The extension artifact changes on every rebuild - never cache it.
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8110
    ext = os.path.join(REPO, "build", "wasm_eh", "repository")
    if not os.path.isdir(ext):
        print("WARNING: no wasm build found at build/wasm_eh/repository/")
        print("         build it first:  make wasm_eh   (with emsdk active)")
    print(f"AnnData in-browser demo:  http://127.0.0.1:{port}/")
    http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
