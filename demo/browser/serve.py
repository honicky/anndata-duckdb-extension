#!/usr/bin/env python3
"""Static server for the in-browser AnnData demo.

Serves this directory, plus two mapped routes:
  /extension/<duckdb_ver>/<arch>/anndata.duckdb_extension.wasm
      -> ../../build/<arch>/repository/<duckdb_ver>/<arch>/... (local wasm build)
  /data/<name>.h5ad
      -> ../../test/data/<name>.h5ad (sample files for the "load sample" button)

No COOP/COEP headers are needed: the demo uses the mvp/eh duckdb-wasm bundles,
never the COI (threads) one.

Usage:  python3 serve.py [port]     (default 8110)
"""
import http.server
import os
import re
import sys
import urllib.parse
import urllib.request

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

    # ---- same-origin range relay: /proxy/<percent-encoded-url> -------------
    #
    # Browsers enforce the same-origin policy: a page cannot read a remote
    # file unless the SERVER opts in with CORS headers, and many public data
    # hosts (e.g. datasets.cellxgene.cziscience.com) do not - their preflight
    # even returns 403. Native tools (curl, the terminal extension) have no
    # such rule, which is why the same URL works there. This relay forwards
    # Range requests from the demo's own origin, so no CORS is involved, and
    # laziness is preserved: only the ranges queries touch flow through.
    #
    # It also normalizes one probe quirk: duckdb-wasm's ranged mode requires
    # HEAD-with-Range to return 206 + Content-Length, but e.g. CloudFront
    # answers 200 - without normalization duckdb-wasm would fall back to
    # downloading the whole file.
    #
    # Local demo tool only (binds 127.0.0.1); do not deploy as an open proxy.

    PROXY_RE = re.compile(r"^/proxy/(.+)$")

    def _proxy(self, head_only):
        url = urllib.parse.unquote(self.PROXY_RE.match(self.path).group(1))
        if not re.match(r"^https?://", url):
            self.send_error(400, "proxy target must be http(s)")
            return
        req = urllib.request.Request(url, method="HEAD" if head_only else "GET")
        rng = self.headers.get("Range")
        if rng:
            req.add_header("Range", rng)
        try:
            resp = urllib.request.urlopen(req, timeout=60)
        except urllib.error.HTTPError as e:
            resp = e  # pass upstream errors through with their status
        except Exception as e:
            self.send_error(502, f"upstream fetch failed: {e}")
            return
        status = resp.status
        clen = resp.headers.get("Content-Length")
        crange = resp.headers.get("Content-Range")
        # normalize: HEAD + Range answered 200 -> synthesize the 206 that
        # duckdb-wasm's range-mode probe requires
        if head_only and rng and status == 200 and clen and not crange:
            status = 206
            crange = f"bytes 0-{int(clen) - 1}/{clen}"
        sys.stderr.write(f"proxy {'HEAD' if head_only else 'GET '} {rng or '-':>24} -> {status} {clen or '?'}b {url[:80]}\n")
        self.send_response(status)
        if clen:
            self.send_header("Content-Length", clen)
        if crange:
            self.send_header("Content-Range", crange)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Type", resp.headers.get("Content-Type", "application/octet-stream"))
        self.end_headers()
        if not head_only:
            while True:
                chunk = resp.read(1 << 16)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_HEAD(self):
        if self.PROXY_RE.match(self.path):
            self._proxy(True)
            return
        super().do_HEAD()

    def do_GET(self):
        if self.PROXY_RE.match(self.path):
            self._proxy(False)
            return
        super().do_GET()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8110
    ext = os.path.join(REPO, "build", "wasm_eh", "repository")
    if not os.path.isdir(ext):
        print("WARNING: no wasm build found at build/wasm_eh/repository/")
        print("         build it first:  make wasm_eh   (with emsdk active)")
    print(f"AnnData in-browser demo:  http://127.0.0.1:{port}/")
    http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()
