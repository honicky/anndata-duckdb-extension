// Tier 1 wasm load smoke test.
//
// Boots @duckdb/duckdb-wasm in Node, serves the locally built
// anndata.duckdb_extension.wasm from a throwaway localhost repository,
// INSTALLs + LOADs it, and asserts the extension is actually functional
// (version scalar, table functions registered).
//
// This is the test that would have caught the bug where every published wasm
// artifact failed at dlopen with unresolved H5* symbols (issue #24).
//
// Usage: node run_node.mjs <path-to-extension.wasm> <arch: wasm_mvp|wasm_eh> \
//            <expected-duckdb-version> <expected-extension-version> [fixture.h5ad]
//
// With the optional 5th argument, the fixture is registered via
// registerFileBuffer and actually queried - covering the wasm file-access
// path (CORE file image, see src/wasm_file_image.cpp), not just LOAD.
//
// IMPORTANT (CI): run under a fresh HOME (HOME="$(mktemp -d)") - the duckdb-wasm
// Node loader caches HTTP response bodies (including error bodies) under
// ~/.duckdb/extensions/ and will reuse a stale or poisoned cache entry.
//
// Version pins (package.json) move in lockstep with duckdb_version in
// .github/workflows/MainDistributionPipeline.yml:
//   @duckdb/duckdb-wasm 1.33.1-dev64.0  ->  DuckDB v1.5.5
//   web-worker pinned 1.2.0 (1.5.x cannot load the CJS worker bundles)

import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import Worker from "web-worker";
import * as duckdb from "@duckdb/duckdb-wasm";

const require = createRequire(import.meta.url);

const [extPath, arch, expectDuckdb, expectExt, fixturePath] = process.argv.slice(2);
if (!extPath || !arch || !expectDuckdb || !expectExt) {
  console.error("usage: run_node.mjs <extension.wasm> <wasm_mvp|wasm_eh> <duckdb-version> <ext-version>");
  process.exit(2);
}

const BUNDLES = {
  wasm_mvp: { main: "duckdb-mvp.wasm", worker: "duckdb-node-mvp.worker.cjs" },
  wasm_eh: { main: "duckdb-eh.wasm", worker: "duckdb-node-eh.worker.cjs" },
  // wasm_threads needs a COI worker, which @duckdb/duckdb-wasm does not ship
  // for Node - it is untestable here (browser tier only).
};
const bundle = BUNDLES[arch];
if (!bundle) {
  console.error(`unsupported arch for the Node harness: ${arch}`);
  process.exit(2);
}

const dist = path.dirname(require.resolve("@duckdb/duckdb-wasm"));
const extBytes = readFileSync(extPath);

let failures = 0;
function check(ok, label, detail = "") {
  console.log(`  [${ok ? "ok  " : "FAIL"}] ${label}${detail ? ": " + detail : ""}`);
  if (!ok) failures++;
}

// Serve the artifact at the exact URL layout the extension loader requests:
//   <repo>/<duckdb_version>/<platform>/<name>.duckdb_extension.wasm
const server = createServer((req, res) => {
  const expected = `/${expectDuckdb}/${arch}/anndata.duckdb_extension.wasm`;
  if (req.url === expected) {
    res.writeHead(200, { "Content-Type": "application/wasm", "Content-Length": extBytes.length });
    res.end(extBytes);
  } else {
    console.log(`  (repo server: unexpected request ${req.url})`);
    res.writeHead(404);
    res.end("not found");
  }
});
await new Promise((r) => server.listen(0, "127.0.0.1", r));
const repo = `http://127.0.0.1:${server.address().port}`;

// Watchdog: a wedged worker (query promise that never settles) would otherwise
// hang the top-level await - and the recovery path too, since conn.close()
// round-trips through the same worker. unref'd so it never delays a clean exit.
const watchdog = setTimeout(() => {
  console.error("FAIL: watchdog timeout (5 min) - worker wedged");
  process.exit(1);
}, 300_000);
watchdog.unref?.();

const logger = new duckdb.VoidLogger();
const worker = new Worker(path.join(dist, bundle.worker));
const db = new duckdb.AsyncDuckDB(logger, worker);

let conn = null;
try {
  // second arg (pthread worker) must be null for mvp/eh
  await db.instantiate(path.join(dist, bundle.main), null);
  await db.open({ path: ":memory:", allowUnsignedExtensions: true });
  conn = await db.connect();

  const one = async (sql) => {
    const t = await conn.query(sql);
    return t.getChildAt(0)?.get(0);
  };

  const duckVer = String(await one("SELECT version()"));
  check(duckVer === expectDuckdb, `duckdb-wasm engine is ${expectDuckdb}`,
    duckVer === expectDuckdb ? "" : `engine is ${duckVer} - the npm pin in test/wasm/package.json must move in lockstep with duckdb_version in the CI workflow`);

  await conn.query(`SET custom_extension_repository='${repo}'`);
  await conn.query("INSTALL anndata");
  await conn.query("LOAD anndata");
  check(true, "LOAD anndata succeeded");

  const ver = String(await one("SELECT anndata_version()"));
  check(ver === expectExt, `anndata_version() == ${expectExt}`, ver);

  const loaded = await one(
    "SELECT loaded FROM duckdb_extensions() WHERE extension_name='anndata'");
  check(Boolean(loaded), "duckdb_extensions() reports loaded");

  const fnTable = await conn.query(
    "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'anndata%' ORDER BY 1");
  const fns = fnTable.getChildAt(0)?.toArray().map(String) ?? [];
  for (const required of ["anndata_version", "anndata_scan_obs", "anndata_scan_var", "anndata_scan_x", "anndata_info"]) {
    check(fns.includes(required), `function registered: ${required}`);
  }
  console.log(`  (functions: ${fns.join(", ")})`);

  if (fixturePath) {
    // ---- file-access phase: register a real .h5ad and query it ----
    await db.registerFileBuffer("fixture.h5ad", new Uint8Array(readFileSync(fixturePath)));

    const nObs = Number(await one("SELECT count(*) FROM anndata_scan_obs('fixture.h5ad')"));
    check(nObs > 0, "anndata_scan_obs returns rows", `${nObs} obs`);
    const nVar = Number(await one("SELECT count(*) FROM anndata_scan_var('fixture.h5ad')"));
    check(nVar > 0, "anndata_scan_var returns rows", `${nVar} var`);

    // A value-level assertion, not just counts: X must sum to a finite number
    const xCols = await conn.query("SELECT * FROM anndata_scan_x('fixture.h5ad') LIMIT 1");
    check(xCols.numCols > 1, "anndata_scan_x has gene columns", `${xCols.numCols} cols`);

    // ATTACH path exercises the storage extension end to end.
    // (duckdb_tables() intentionally not used: anndata's catalog does not
    // populate it - native behaves identically. SHOW ALL TABLES does.)
    await conn.query("ATTACH 'fixture.h5ad' AS fx (TYPE ANNDATA)");
    const nTables = Number(await one(
      "SELECT count(*) FROM (SHOW ALL TABLES) WHERE database='fx'"));
    check(nTables > 0, "ATTACH (TYPE ANNDATA) exposes tables", `${nTables} tables`);
    const nObsAttach = Number(await one("SELECT count(*) FROM fx.obs"));
    check(nObsAttach === nObs, "ATTACH obs count matches scan", `${nObsAttach}`);
    await conn.query("DETACH fx");

    // Unregistered path must fail with the actionable registerFileBuffer hint.
    // wasm_eh only: on wasm_mvp a side-module C++ throw dies in the dynamic
    // loader's invoke wrappers with `_setThrew is not defined` (duckdb-wasm's
    // main-module JS does not alias its setThrew export for side modules) -
    // an upstream limitation; happy paths are unaffected.
    if (arch === "wasm_eh") {
      let missingMsg = "";
      try {
        await conn.query("SELECT count(*) FROM anndata_scan_obs('nope.h5ad')");
      } catch (e) {
        missingMsg = String(e?.message ?? e);
      }
      check(missingMsg.includes("registerFileBuffer"),
        "unregistered file error mentions registerFileBuffer", missingMsg.slice(0, 140));
    }
  } else {
    console.log("  (no fixture argument - file-access phase skipped)");
  }
} catch (e) {
  check(false, "harness completed without an exception", String(e?.message ?? e));
} finally {
  try { if (conn) await conn.close(); await db.terminate(); } catch {}
  server.close();
}

console.log(failures ? `\nFAIL (${failures} checks)` : "\nPASS");
process.exit(failures ? 1 : 0);
