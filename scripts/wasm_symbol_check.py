#!/usr/bin/env python3
"""Link-level symbol contract check for DuckDB-WASM extension artifacts.

A DuckDB wasm extension is an Emscripten side module (-sSIDE_MODULE=2). The
side-module link does NOT fail on unresolved symbols, so a green build can
produce an artifact that fails at LOAD time inside duckdb-wasm with e.g.:

    Error: bad export type for 'H5T_NATIVE_INT32_g': undefined

(which is exactly what happened for every anndata wasm artifact published
before this check existed - see issue #24 and spec/wasm-support-spec.md).

This script proves, without running anything, that the side module can be
instantiated against a given duckdb-wasm main module:

    imports(side) - exports(side) - exports(main) - allowlist == empty set

It is a pure-Python wasm binary parser: no wabt / llvm tooling required.

Usage:
  wasm_symbol_check.py side.wasm --main duckdb-eh.wasm \
      [--allowlist test/wasm/allowed_undefined.txt] \
      [--expect-init anndata_duckdb_cpp_init] \
      [--expect-platform wasm_eh] [--expect-duckdb v1.5.5] \
      [--min-size 2000000] [--max-size 12000000]

Exit code 0 = pass, 1 = fail (with per-check diagnostics).
"""

import argparse
import fnmatch
import sys

# ---------------------------------------------------------------- wasm parser


def leb_u(buf, i):
    result = 0
    shift = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return result, i


def parse_name(buf, i):
    n, i = leb_u(buf, i)
    return buf[i : i + n].decode("utf-8", "replace"), i + n


def parse_limits(buf, i):
    flags, i = leb_u(buf, i)
    _min, i = leb_u(buf, i)
    if flags & 0x1:
        _max, i = leb_u(buf, i)
    return i


class WasmModule:
    def __init__(self, path):
        self.path = path
        with open(path, "rb") as f:
            self.data = f.read()
        if self.data[:4] != b"\x00asm":
            raise ValueError(f"{path}: not a wasm module (bad magic)")
        self.sections = []  # (id, name_or_None, payload)
        i = 8
        d = self.data
        while i < len(d):
            sid, i = leb_u(d, i)
            size, i = leb_u(d, i)
            payload = d[i : i + size]
            name = None
            if sid == 0:
                name, j = parse_name(payload, 0)
                payload = payload[j:]
            self.sections.append((sid, name, payload))
            i += size

    def imports(self):
        """Return list of (module, field, kind)."""
        out = []
        for sid, _, p in self.sections:
            if sid != 2:
                continue
            count, i = leb_u(p, 0)
            for _ in range(count):
                mod, i = parse_name(p, i)
                field, i = parse_name(p, i)
                kind = p[i]
                i += 1
                if kind == 0x00:  # func: typeidx
                    _, i = leb_u(p, i)
                elif kind == 0x01:  # table: reftype + limits
                    i += 1
                    i = parse_limits(p, i)
                elif kind == 0x02:  # memory: limits
                    i = parse_limits(p, i)
                elif kind == 0x03:  # global: valtype + mutability
                    i += 2
                elif kind == 0x04:  # tag (exception handling): attr + typeidx
                    i += 1
                    _, i = leb_u(p, i)
                else:
                    raise ValueError(
                        f"{self.path}: unknown import kind 0x{kind:02x} for "
                        f"{mod}.{field} - parser needs updating"
                    )
                out.append((mod, field, kind))
        return out

    def exports(self):
        out = set()
        for sid, _, p in self.sections:
            if sid != 7:
                continue
            count, i = leb_u(p, 0)
            for _ in range(count):
                name, i = parse_name(p, i)
                i += 1  # kind
                _, i = leb_u(p, i)  # index
                out.add(name)
        return out

    def custom_section(self, name):
        for sid, sname, p in self.sections:
            if sid == 0 and sname == name:
                return p
        return None

    def metadata_fields(self):
        """Parse duckdb_signature: 2-byte LEB(512) + fields 8..1 (32B each,
        appended in reverse) + 256B signature. See duckdb/scripts/append_metadata.cmake."""
        p = self.custom_section("duckdb_signature")
        if p is None:
            return None
        _, i = leb_u(p, 0)  # payload length (512)
        fields_rev = [p[i + k * 32 : i + (k + 1) * 32].rstrip(b"\x00").decode("utf-8", "replace") for k in range(8)]
        # file order is METADATA8..METADATA1; flip to 1..8
        f = list(reversed(fields_rev))
        return {
            "meta1": f[0],
            "platform": f[1],
            "duckdb_version": f[2],
            "extension_version": f[3],
            "abi_type": f[4],
        }


# ------------------------------------------------------------------- checks

# Symbols the Emscripten dynamic loader itself supplies to every side module.
LOADER_SUPPLIED = {
    "__memory_base",
    "__stack_pointer",
    "__table_base",
    "__table_base32",
    "memory",
    "__indirect_function_table",
    "__cpp_exception",
    "__c_longjmp",
    "setTempRet0",
    "getTempRet0",
    "__tls_base",
    "__tls_size",
    "__tls_align",
    "__wasm_init_tls",
    "emscripten_stack_set_limits",
}

# Prefixes that indicate the HDF5 dependency stack failed to link statically.
DEPENDENCY_PREFIXES = ("H5", "inflate", "deflate", "compress", "uncompress", "crc32", "adler32", "SZ_", "aec_")


def load_allowlist(path):
    pats = []
    if path:
        with open(path) as f:
            for line in f:
                line = line.split("#", 1)[0].strip()
                if line:
                    pats.append(line)
    return pats


def allowed(name, patterns):
    return any(fnmatch.fnmatchcase(name, p) for p in patterns)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("side", help="the extension .duckdb_extension.wasm to check")
    ap.add_argument("--main", required=True, help="duckdb-wasm main module (duckdb-mvp/eh/coi.wasm)")
    ap.add_argument("--allowlist", help="file of allowed-unresolved symbol patterns")
    ap.add_argument("--expect-init", default="anndata_duckdb_cpp_init")
    ap.add_argument("--expect-platform", help="expected metadata platform field, e.g. wasm_eh")
    ap.add_argument("--expect-duckdb", help="expected metadata duckdb_version field, e.g. v1.5.5")
    ap.add_argument("--min-size", type=int, default=2_000_000,
                    help="size floor: HDF5 statically linked cannot be smaller (default 2MB)")
    ap.add_argument("--max-size", type=int, default=12_000_000,
                    help="size ceiling: browser delivery budget (default 12MB)")
    args = ap.parse_args()

    failures = []

    def check(ok, label, detail=""):
        mark = "ok  " if ok else "FAIL"
        print(f"  [{mark}] {label}" + (f": {detail}" if detail else ""))
        if not ok:
            failures.append(label)

    side = WasmModule(args.side)
    main_mod = WasmModule(args.main)

    size = len(side.data)
    print(f"{args.side}: {size:,} bytes, vs main module {args.main}")

    # 1. structure
    check(side.custom_section("dylink.0") is not None, "dylink.0 custom section present")
    sig_idx = [k for k, (sid, n, _) in enumerate(side.sections) if sid == 0 and n == "duckdb_signature"]
    check(bool(sig_idx), "duckdb_signature custom section present")
    if sig_idx:
        check(sig_idx[0] == len(side.sections) - 1, "duckdb_signature is the last section")

    # 2. init export
    exports = side.exports()
    check(args.expect_init in exports, f"exports {args.expect_init}")

    # 3. size budget
    check(size >= args.min_size, f"size >= {args.min_size:,}",
          f"{size:,} bytes - below the floor means static deps (HDF5 et al.) are missing from the link")
    check(size <= args.max_size, f"size <= {args.max_size:,}", f"{size:,} bytes")

    # 4. symbol resolution
    allow = load_allowlist(args.allowlist)
    imports = side.imports()
    main_exports = main_mod.exports()
    # The emscripten dynamic loader resolves a side module's env imports against
    # the main module's EXPORTS and also against the main module's own JS-library
    # IMPORTS (wasmImports) - e.g. wasm_mvp's JS-based exception path imports
    # env.__cxa_throw, which duckdb-mvp.wasm itself imports from the JS library
    # rather than exporting. Anything the main module imports, the JS runtime
    # supplies, so the side module can import it too.
    main_imports = {field for _, field, _ in main_mod.imports()}
    unresolved = sorted(
        {field for _, field, _ in imports}
        - exports          # vague-linkage / GOT self-references resolve within the module
        - main_exports     # supplied by the duckdb-wasm main module
        - main_imports     # supplied by the JS runtime to the main module (wasmImports)
        - LOADER_SUPPLIED  # supplied by the emscripten dynamic loader
    )
    unresolved = [s for s in unresolved if not allowed(s, allow)]

    dep_unresolved = [s for s in unresolved if s.startswith(DEPENDENCY_PREFIXES)]
    check(not dep_unresolved, "no unresolved HDF5/zlib/szip/aec symbols",
          f"{len(dep_unresolved)} found e.g. {dep_unresolved[:8]} - the static dependency "
          "archives are not fully linked into the side module (LINKED_LIBS)")
    other = [s for s in unresolved if s not in dep_unresolved]
    check(not other, "no other unresolved imports",
          f"{len(other)} found e.g. {other[:8]} - either add to --allowlist deliberately "
          "or fix the link")

    # 5. metadata
    meta = side.metadata_fields()
    check(meta is not None, "metadata parseable")
    if meta:
        print(f"       metadata: platform={meta['platform']!r} duckdb={meta['duckdb_version']!r} "
              f"ext={meta['extension_version']!r} abi={meta['abi_type']!r}")
        if args.expect_platform:
            check(meta["platform"] == args.expect_platform,
                  f"metadata platform == {args.expect_platform}", repr(meta["platform"]))
        if args.expect_duckdb:
            check(meta["duckdb_version"] == args.expect_duckdb,
                  f"metadata duckdb_version == {args.expect_duckdb}", repr(meta["duckdb_version"]))

    stats = (f"imports={len(imports)} side_exports={len(exports)} "
             f"main_exports={len(main_exports)} unresolved={len(unresolved)}")
    if failures:
        print(f"\nFAIL ({len(failures)} checks): {', '.join(failures)}\n  {stats}")
        return 1
    print(f"\nPASS  {stats}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
