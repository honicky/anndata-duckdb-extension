# WASM Support Specification and Implementation Plan

## Overview

This document specifies what it would take to support DuckDB-WASM properly, and plans the
work. It originates from [issue #24](https://github.com/honicky/anndata-duckdb-extension/issues/24),
which proposed marking the extension wasm-excluded.

The investigation found something the issue did not: **the wasm artifacts this project has been
building and publishing since its first week do not load.** They have never loaded. CI is green
because the wasm link step tolerates undefined symbols, and no test has ever executed a wasm build.

The plan therefore has two distinct halves that should not be conflated:

1. **A repair** — days of work — that makes the artifact loadable and puts a gate in place so it
   can never silently break again.
2. **A port** — weeks of work — that makes file access go through DuckDB's `FileSystem`, without
   which a loadable extension still cannot open a single file in a browser.

Evidence classes used throughout: **EXECUTED** (a command was run and the output observed in
preparing this document), **VERIFIED** (source read directly), **INFERRED** (reasoned, with
confidence), **UNKNOWN**.

---

## Current State

### The published artifact cannot load

EXECUTED. The `wasm_eh` artifact from the last fully green CI run (31567104870) was downloaded and
its import table compared against duckdb-wasm's main module (`@duckdb/duckdb-wasm`,
`dist/duckdb-eh.wasm`):

| measurement | value |
|---|---|
| side-module imports | 605 |
| resolvable from `duckdb-eh.wasm` | 329 |
| self-resolving (`GOT.mem` vague-linkage) | 207 |
| **genuinely unresolved** | **69** |
| — of which `H5*` | **63** |
| — of which standard loader-supplied | 6 |
| `H5*` symbols exported by `duckdb-eh.wasm` | **0** |

The 63 are `H5Fopen`, `H5Dread`, `H5Zregister`, `H5Pcreate`, `H5T_NATIVE_INT32_g` and similar.
`H5T_NATIVE_INT32_g` is a `GOT.mem` **data** import, resolved eagerly by Emscripten's dynamic
linker, so the module aborts during `dlopen` before any extension code runs. The failure is not
avoidable by deferring HDF5 calls.

An independent run under `@duckdb/duckdb-wasm` in Node reproduced the user-visible error:

```
IO Error: Extension "anndata.duckdb_extension.wasm" could not be loaded:
          Could not load dynamic lib: anndata
Error: bad export type for 'H5T_NATIVE_INT32_g': undefined
```

Corroborating signal: the wasm artifacts are 98–116 KB compressed / 446 KB raw, against 11–17 MB
for native builds. A module containing a statically linked HDF5 cannot be that small — for
comparison, `spatial.duckdb_extension.wasm` is 6.3 MB.

### Root cause

VERIFIED at `duckdb/extension/extension_build_tools.cmake:182-198`. Under Emscripten the real
`.wasm` is not produced by the CMake target at all. The target is built as a `STATIC` library
(`:115-119`), and the actual module comes from a separate `POST_BUILD` shell-out:

```cmake
COMMAND emcc $<TARGET_FILE:${TARGET_NAME}> -o $<TARGET_FILE:${TARGET_NAME}>.wasm
        -O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="${EXPORTED_FUNCTIONS}"
        ${WASM_THREAD_FLAGS} ${TO_BE_LINKED}
```

`TO_BE_LINKED` is populated **only** from `DUCKDB_EXTENSION_<NAME>_LINKED_LIBS` (`:186`), which is
set **only** by the `LINKED_LIBS` argument of `duckdb_extension_load()`. Our `extension_config.cmake`
does not pass it, so `TO_BE_LINKED` is empty, and the `target_link_libraries(${LOADABLE_EXTENSION_NAME}
${HDF5_LIBRARIES})` at `CMakeLists.txt:151` is bypassed entirely. A CMake `STATIC` target's
`target_link_libraries` is a usage requirement, not an archiving step, so nothing else rescues it.

`-sSIDE_MODULE=2` does not error on unresolved symbols — this is documented Emscripten behaviour
("we don't check if any symbols remain unresolved"). Hence a green build that produces a
non-loadable artifact.

### Green CI proves linking, not loading

VERIFIED. There is no wasm test anywhere in the toolchain:

- `extension-ci-tools/scripts/ci_phase.py:468-492` — the `test()` phase's wasm branch prints
  *"The Wasm distribution job has no test target."* and returns.
- No `test_wasm_*` target exists in `extension-ci-tools/makefiles/duckdb_extension.Makefile`.
- Grep for `playwright|npm|npx|node --|duckdb-wasm` across `extension-ci-tools`: no hits.
- `duckdb/.github/workflows/NightlyTests.yml:565` (`linux-wasm-experimental`) is guarded `if: false`.

`RELEASE.md:45-47` lists WebAssembly under "platforms built" and says to "test the extension on
each platform." That is not true and is part of why this went unnoticed for eleven months.

---

## Issue #24 Scorecard

| Claim | Verdict |
|---|---|
| **(a) `LINKED_LIBS` needed for HDF5 symbols** | **Correct, and it is the live bug.** The single reason the artifact is broken. |
| **(b) `std::ifstream` won't work** | **Half wrong, half understated.** It *links* — `basic_ifstream` is exported by duckdb-wasm's main module. But it reads Emscripten MEMFS, which duckdb-wasm never populates, so the probes always report "missing". More importantly `ifstream` is a symptom: deleting all four probe sites still leaves `H5Fopen` on the POSIX (sec2) driver, which is the actual blocker. |
| **(c) HDF5-in-wasm is a deep rabbit hole** | **Stale.** It was solved in this repo's first week. `CMakeLists.txt:32-48` FetchContents `usnistgov/libhdf5-wasm` v0.4.6_3.1.68 (HDF5 1.14.6), which builds and compiles cleanly. |

Net: the issue identified one real, unfixed, currently-shipping bug and two non-blockers. The
recommendation to exclude was reasonable given the information available, but exclusion should be
an interim posture, not the endpoint.

---

## Blockers

Ordered by severity.

### B1 — HDF5/zlib/szip/aec not linked into the side module

Shipping broken today. Breaks `LOAD anndata` on all three wasm arches, all published versions.

**The fix is confirmed to work.** EXECUTED, using emsdk 3.1.71 (the version CI pins at
`_extension_distribution.yml:691-693`) against the actual `libhdf5-wasm` tarball: a stub exercising
`H5open`/`H5Fopen`/`H5Dread`/`H5Zfilter_avail` was compiled `-fPIC` and linked
`-sSIDE_MODULE=2` against all four archives.

| link configuration | result | size |
|---|---|---|
| `libhdf5.a libsz.a libaec.a libz.a` | **0 genuinely unresolved** | 2.63 MB |
| reversed archive order | **0 genuinely unresolved** | 2.63 MB |
| `libhdf5.a` alone | **8 unresolved**: `inflate`, `inflateInit_`, `inflateEnd`, `compressBound`, `compress2`, `SZ_encoder_enabled`, … | 2.53 MB |
| *(today: nothing passed)* | 69 unresolved (63 `H5*`) | 0.45 MB |

Three consequences that the fix must respect:

1. **All four archives are required.** `CMakeLists.txt:42-44` currently sets `HDF5_LIBRARIES` to
   `libhdf5.a` only. Passing just that reproduces the same class of silently-broken artifact —
   green CI, 8 unresolved symbols, dead on `dlopen`. `libz.a`, `libsz.a` and `libaec.a` ship in the
   same tarball and must be named. Szip cannot be avoided by "not using szip": `H5Z.c`'s static
   filter table references `SZ_*` unconditionally when `H5_HAVE_FILTER_SZIP` is set, and it is.
2. **Archive order does not matter.** EXECUTED — reversed order also yields zero unresolved.
   `wasm-ld` performs full archive resolution, unlike classic `ld`. Advice to the contrary is wrong.
3. **`LINKED_LIBS` is a `oneValueArgs` parameter** (`extension_build_tools.cmake:417`). It must be
   a single quoted, space-separated string; unquoted multiple paths are silently dropped into
   `UNPARSED_ARGUMENTS`. It works only because `separate_arguments()` re-splits it at `:187`.

**Where to set it.** Not in `extension_config.cmake` — the FetchContent source dir is not known
there. `build_loadable_extension` is a CMake *function* reading the caller's directory scope, and
it is invoked at `CMakeLists.txt:147`, after `FetchContent_MakeAvailable` at `:40`. So setting
`DUCKDB_EXTENSION_ANNDATA_LINKED_LIBS` anywhere between `:48` and `:146` in our own `CMakeLists.txt`
is ordering-safe. (This is the approach `teaguesterling/duckdb_webbed` uses after hitting the same
trap.)

**Also fix while here:** `CMakeLists.txt:38` FetchContents a GitHub release tarball with no
`URL_HASH`, inside the community-extensions build. Add the hash — one line, and it is both a
supply-chain and a reproducibility fix.

### B2 — All HDF5 I/O bypasses `duckdb::FileSystem`

Blocks every user file in a browser, and is the reason B1 alone is not sufficient.

Every open is `H5Fopen(path, H5F_ACC_RDONLY, fapl)` on HDF5's default sec2 (POSIX) driver
(`src/include/h5_file_cache.hpp:153-164`), preceded by `stat` / `S_ISDIR` / `ifstream` probes
(`:135`, `:141`, `:146`). Under Emscripten those resolve to MEMFS. duckdb-wasm's
`registerFileBuffer` / `registerFileURL` / `registerFileHandle` register into `WebFileSystem`,
a `duckdb::FileSystem` subclass. **The two filesystems are disjoint**, so every path a duckdb-wasm
user can produce will report "File not found."

Perverse corollary: `src/glob_handler.cpp:28,72` *does* use `FileSystem::GetFileSystem(context)`,
so a wildcard query can successfully list files the reader then cannot open.

Also affected natively-shaped: on wasm, `s3://` and `https://` fall through the
`#ifndef DUCKDB_NO_REMOTE_VFD` guard into the local branch and die with a misleading
`File not found: https://…`.

### B3 — X/layers/raw_X emit one DuckDB column per gene

A wasm32 address-space hazard and a native usability problem.

`anndata_scan_x` returns wide format — one column per gene. A 30k-gene file yields a 30,001-column
table; a single `DataChunk` is 30,001 × 2048 × 8 B ≈ 491 MB, allocated by DuckDB before extension
code runs. duckdb-wasm sets `MAXIMUM_MEMORY=4GB` with `use_temporary_directory = false`, so there
is no spill — an over-budget query simply dies. Practical per-tab ceiling is often ~2 GB.

Additionally, `size_t` is 32 bits on wasm32 (`H5_SIZEOF_SIZE_T 4`, VERIFIED from the prebuilt's
`H5pubconf.h`). Buffer-size computations such as `h5_reader_multithreaded.cpp:1925`
(`std::vector<double> buffer(row_count*col_count)`) and `:3794` (`values.resize(obs_count*var_count)`)
compute in `size_t` and can **silently wrap** rather than merely OOM. This is a wrong-answer class,
not just a memory class, and auditing every `n*m` size computation in the reader is its own task.

**Important correction to a natural assumption:** projection pushdown exists
(`anndata_scanner.cpp:842-888`) and does reduce DuckDB-side column count and memory, but it does
**not** reduce HDF5 I/O for CSR matrices. Measured below.

### B4 — `wasm_threads` is high-risk, low-value

- `extension-ci-tools/makefiles/duckdb_extension.Makefile:231` references
  `WASM_COMPILE_TIME_EH_FLAGS`, which is **never defined** (the real variable is
  `WASM_CXX_EH_FLAGS` at `:230`). So wasm_threads extensions build without `-fwasm-exceptions`
  while duckdb-wasm's threads main module is built with it — an exception-ABI mismatch. Upstream bug.
- `libhdf5.a` is built without `-pthread`; `wasm_threads` sets `-sSHARED_MEMORY=1`. wasm-ld may
  refuse to mix these (INFERRED ~70%; not yet executed — see Q3).
- `usnistgov/libhdf5-wasm#1` reports HDF5's mere presence causing segfaults in multi-threaded
  Emscripten builds.
- It is untestable in Node: `@duckdb/duckdb-wasm/dist` ships no COI worker.
- It is rarely selected in practice — `getJsDelivrBundles()` ships no `coi` entry, by design.

Recommendation: ship `wasm_mvp` + `wasm_eh`; treat `wasm_threads` as excluded until someone has a
reason to want it.

### B5 — Exception handling on `wasm_mvp`

`WASM_CXX_MVP_FLAGS` is empty and `duckdb/CMakeLists.txt:258-261` gates `-fexceptions` behind
`EXPLICIT_EXCEPTIONS` (default off). Emscripten's default `DISABLE_EXCEPTION_CATCHING=1` compiles
with `-fignore-exceptions`, removing catch handlers. The compiled sources contain 91 `throw` and
108 `catch (` sites. This matters specifically for the VFD design in B2: a C++ exception must never
escape an HDF5 C callback, and on `wasm_mvp` the catch that is supposed to contain it may be elided.

Resolution: write the VFD in a no-throw style — every callback returns an HDF5 error code, with
error detail stashed in a side channel — rather than relying on catch. This is good practice
regardless of platform, so it costs nothing to adopt.

### B6 — Unbounded eager work at ATTACH time

`GetUnsKeys()` (`h5_reader_multithreaded.cpp:2951`) reads every array in `/uns` in full, as strings,
and runs at ATTACH (`anndata_storage.cpp:278`). `/uns` routinely holds full-length per-cell colour
maps and `rank_genes_groups` tables. Independent of wasm; wasm makes it fatal.

### B7 — Console noise

`src/anndata_extension.cpp:62` prints to `std::cout` on every `LOAD`. In a browser that goes into
the host page's console. Six `fprintf(stderr, …)` in `anndata_storage.cpp:161-181` and a
non-threadsafe-HDF5 warning at `h5_reader_multithreaded.cpp:35` (which will fire on every wasm
session, since the prebuilt is `#undef H5_HAVE_THREADSAFE`) compound it. Route through DuckDB's
logging; suppress the threadsafe warning when `MaxThreads() == 1`.

### B8 — Dead code inflating the apparent surface

`src/h5_reader.cpp` (2,641 lines, HDF5 C++ API) is absent from `EXTENSION_SOURCES` and compiled on
no platform. `src/include/h5_handles.hpp:79` `H5FileHandle(path)` has zero live call sites. Between
them they account for two of the six `H5Fopen`/`ifstream` sites that get counted when scoping this
work. Delete before scoping.

---

## Measurements

### Threading

VERIFIED by exhaustive grep over `src/` excluding `src/lzf/`: **the extension spawns zero threads.**
No `std::thread`, `std::async`, `pthread_*`, `TaskScheduler`, `condition_variable`, or `std::atomic`.
`src/h5_reader_multithreaded.cpp` is a misnomer — it means "safe to call *from* DuckDB's threads."
`AnndataGlobalState::MaxThreads()` returns 1 (`anndata_scanner.hpp:166-168`).

The synchronisation that exists (`H5GlobalLock`'s function-local `recursive_mutex`,
`H5FileCache::mu_`, one `std::once_flag`) compiles to stubs without pthreads. The only
`thread_local` is in `h5fd_http.cpp:39-40`, which is compiled out on wasm.

**Conclusion: the pthread half of the issue's concern does not apply. `wasm_mvp` and `wasm_eh` are
fine.** The prebuilt HDF5 is non-threadsafe, the extension already detects this
(`h5_reader_multithreaded.cpp:32` → `H5is_library_threadsafe()`) and falls back to its own global
mutex — the same path the Windows build already uses.

### libhdf5-wasm 1.14.6 configuration

VERIFIED from the extracted tarball's `H5pubconf.h`:

| flag | value | consequence |
|---|---|---|
| `H5_HAVE_THREADSAFE` | **absent** | serialize via the existing global mutex, as on Windows |
| `H5_HAVE_ROS3_VFD` | **absent** | no built-in S3; remote must go through a custom VFD |
| `H5_HAVE_LIBCURL` | **absent** | confirms the above |
| `H5_HAVE_FILTER_DEFLATE` | 1 | gzip `.h5ad` works, given `libz.a` is linked |
| `H5_HAVE_FILTER_SZIP` | 1 | requires `libsz.a` + `libaec.a` |
| `H5_SIZEOF_SIZE_T` | 4 | wasm32 — see B3 |
| `H5_SIZEOF_OFF_T` | 8 | files > 4 GB are addressable |

CORE VFD and `H5FDdevelop.h` are both shipped, so both the whole-file-image approach and a custom
VFD are available. Note `lib/libhdf5.settings` reports `Optimization Level: OFF` — the prebuilt is
`-O0` codegen, which is why rebuilding HDF5 ourselves is worth evaluating (Q4).

### duckdb-wasm main module exports the API a VFD needs

EXECUTED against `duckdb-eh.wasm` (78,053 exports):

| symbol | present |
|---|---|
| `FileSystem::GetFileSystem(ClientContext&)` | yes |
| `FileSystem::OpenFile` | yes |
| `FileHandle::Read(void*, uint64, uint64)` (positional) | yes |
| `FileHandle::Seek`, `GetFileSize` | yes |
| `FileSystem::Glob`, `FileExists` | yes |
| `VirtualFileSystem` | yes (58 symbols) |
| `SecretManager` | yes (48 symbols) |
| httpfs (`HTTPWasmClient`) | yes (12 symbols) |

Positional random-access reads — exactly what an HDF5 VFD issues — are supported, synchronously,
with no ASYNCIFY/JSPI required. HTTP/S3 is available through the main module rather than curl.
**This is the decisive architectural finding: it is why the VFD must go through `duckdb::FileSystem`
rather than calling `fetch()` itself.**

### Read amplification

EXECUTED against an instrumented range-request HTTP server, using the current native build and its
existing HTTP VFD, on a 109 MB gzip-compressed `.h5ad` (150,000 obs × 3,000 var, CSR, 3% density):

| query | requests | bytes | % of file |
|---|---|---|---|
| `scan_var` (3,000 genes, metadata only) | 20 | 19.1 MB | 17.5% |
| `scan_obs` count | 25 | 24.4 MB | 22.3% |
| `obs` cell_type histogram | 25 | 24.4 MB | 22.3% |
| `ATTACH` only | 22 | 21.2 MB | 19.4% |
| `obsm_X_pca LIMIT 5` | 24 | 23.3 MB | 21.3% |
| **`X`: 1 gene column** | **78** | **80.0 MB** | **73.1%** |
| **`X`: 10 gene columns** | 78 | 80.0 MB | 73.1% |
| **`X`: all 3,000 columns** | 78 | 80.0 MB | 73.1% |

Two findings:

1. **Metadata access is browser-viable.** Twenty-odd requests, not thousands. HDF5's small random
   reads are absorbed by the VFD's 1 MB LRU block cache (`h5fd_http.cpp:256-382`). Most of the
   volume is a **hardcoded 16 MB eager prefetch on open** (`:1010`, `config.prefetch_size`), which
   has no user-facing setting and issues 16 sequential 1 MB round-trips. That should become a
   tunable, with a smaller wasm default.
2. **Gene selection on CSR reads the whole matrix.** Identical cost for 1 gene and 3,000, because
   CSR is row-major and extracting a column requires scanning all of `indices` and `data`.
   Projection pushdown reduces DuckDB columns and memory but **not** I/O. For a browser this, not
   HDF5 granularity, is the binding performance constraint. CSC-stored files would not have this
   problem; the extension already supports CSC.

Note the existing VFD leaves easy wins on the table: `query` is `nullptr` (`:936`) so feature flags
are zero, `read_vector` is `nullptr` (`:946`), and `config_.cache_size` is never read (`:391`
hardcodes `BlockCache(1 MB, 64)`).

---

## Design

### Recommended: one `duckdb::FileSystem`-backed HDF5 VFD, on every platform

Replace both the sec2 driver and the curl VFD with a single read-only HDF5 virtual file driver whose
backend is `duckdb::FileHandle` positional reads.

This is chosen over a wasm-only driver because the code is the same either way, and scoping it to
wasm keeps two remote implementations alive forever. It buys, in one move:

- Local wasm files work (`registerFileBuffer`, `registerFileHandle`, OPFS, Node FS).
- Remote wasm files work through duckdb-wasm's HTTP/S3 backends — the only legal browser network path.
- Deletes `src/vfd/h5fd_http.cpp` (1,254 lines), a hand-rolled AWS SigV4, a three-tier HTTP
  size-discovery ladder, and the `curl` + `OpenSSL` dependencies.
- Deletes the `DUCKDB_NO_REMOTE_VFD` fork — one code path everywhere.
- Deletes all four `stat`/`ifstream` probe sites.
- Fixes the dead `cache_size` config, the 16-RTT prefetch, and the missing `query` callback for free.
- Exercised by the **entire existing native test suite** with no new harness, because it replaces
  sec2 natively too.

Roughly 460 lines of `h5fd_http.cpp` survive nearly verbatim — the `BlockCache` (`:256-382`) is
fully transport-agnostic, as are `Read`, `Prefetch`, the `H5FD_class_t` initialiser, and the
close/get_eoa/set_eoa/get_eof callbacks. The genuinely new work:

1. **FAPL must stay POD.** `fapl_copy` is `nullptr` with `fapl_size > 0`, and HDF5 does a raw
   `memcpy` of the driver info. No `string`, `shared_ptr` or `FileHandle` by value. Open the
   `duckdb::FileHandle` *before* `H5Fopen`, where a context is in scope, and pass a raw
   caller-owned `FileHandle*` through the FAPL. The VFD's `open` must compare the requested name
   against the stashed handle's path and hard-error on mismatch, or external links and `H5Freopen`
   will silently read the wrong file.
2. **Plumb the filesystem into `H5FileCache::Open`.** Use `DatabaseInstance::GetFileSystem()` plus
   an optional `FileOpener*` for secrets, **not** a stored `ClientContext&` — an ATTACH-scoped
   catalog is database-scoped while a context is per-connection, so a stored context dangles when
   ATTACH happens on one connection and the query on another.
3. **No-throw error propagation** across the C boundary (see B5). Drop the `thread_local` error
   state at `h5fd_http.cpp:39-40`.
4. **Cache lifetime.** `H5FileCache` is a function-local `static` (`h5_file_cache.hpp:203-205`)
   holding an 8-entry LRU of open files that outlive DETACH. Once `H5Fclose` transitively closes a
   `duckdb::FileHandle`, static destruction order (database first, function-local static last)
   becomes a **use-after-free at shutdown on every platform**. The cache must move to
   `ObjectCache`/`DatabaseInstance` scope. This is a singleton refactor touching every call site,
   and it is the most under-estimated item in this plan.
5. **Invalidation.** duckdb-wasm's `dropFile()` / re-`registerFileBuffer()` with the same name are
   ordinary operations. The cache is keyed on path string alone (`:91-97`), so a re-registered name
   silently serves stale bytes — already a latent bug for S3 with differing credentials.
6. **A non-null `FileOpener` on every `OpenFile`.** `VirtualFileSystem::FindFileSystem` autoloads
   httpfs only via `FileOpener::TryGetDatabase(opener)`; with `opener == nullptr` (the default)
   remote paths fall through to `LocalFileSystem` and produce a bogus local-file error.
   `src/glob_handler.cpp:28,72` already makes exactly this mistake. Use `DatabaseFileOpener` —
   secrets resolve without a `ClientContext` via `CatalogTransaction::GetSystemTransaction`.

### Cache policy must be per-backend, not universal

This is the single most important native-safety requirement, and getting it wrong is the largest
regression risk in the plan. Local and remote differ by five orders of magnitude in per-read
latency (a warm `pread` is 340–400 ns; an HTTP range request is ~50 ms). One policy cannot serve both.

**Local: no block cache, no prefetch.** A block cache in front of a local file is read
amplification, not latency hiding. It also duplicates two caches that already sit above and below
the VFD — the OS page cache, and HDF5's own metadata cache (32 MiB default) and chunk cache (1 MiB /
521 slots). DuckDB encodes the same judgement: `CachingMode::CACHE_REMOTE_ONLY` exists precisely so
local files bypass its external file cache. `FileOpenFlags::caching_mode` already defaults to
`NO_CACHING`, so the default is correct — the risk is someone "helpfully" setting `ALWAYS_CACHE`.

**Remote: keep our own aligned block cache. Do not delegate it.** Verified against the pinned
v1.5.5 submodule: DuckDB's `ExternalFileCache` caches *exact requested ranges*
(`caching_file_system.cpp:141-190`), widening only to fill a gap ≤ 1 MiB that is also ≤ the current
request length. It is a cache, not a coalescer — a 4 KB HDF5 read fetches 4 KB. httpfs's own
buffering is a single unaligned sliding window, and it has been **deleted outright on
`duckdb-httpfs@main`**. Porting `BlockCache` (`h5fd_http.cpp:256-382`) verbatim is therefore
required, not optional; delegating would turn ~20 requests per query into something on the order of
HDF5's raw read count.

**Detection** is `FileSystem::IsRemoteFile(path)` — static, no I/O, no throw, prefix match over
`http/https/s3/s3a/s3n/gs/gcs/r2/hf`. This is what DuckDB itself uses for `CACHE_REMOTE_ONLY`.
Because the prefix heuristic is wrong for network-mounted local paths (NFS, SMB, gcsfuse), expose
`anndata_vfd_cache_mode = 'auto'|'off'|'on'`, plus `anndata_vfd_block_size`,
`anndata_vfd_cache_size`, `anndata_vfd_prefetch_size`. That also finally makes `cache_size` live.

**Block size: keep ~1 MiB for remote. The current default is right; only its unconditional
application is wrong.** Measured (HDF5 log VFD, composite ATTACH-like workload = enumerate + read
all obs/var columns), on a 110 MB file and a real 1.19 GB CELLxGENE file:

| block size | 110 MB: fetches / bytes / amp | 1.19 GB: fetches / bytes / amp |
|---|---|---|
| none (raw) | 448 reads / 6.50 MB / 1.00× | 1369 reads / 12.79 MB / 1.00× |
| 64 KiB | 116 / 7.6 MB / 1.17× | 232 / 15.2 MB / 1.19× |
| 256 KiB | 41 / 10.7 MB / 1.65× | 84 / 22.0 MB / 1.72× |
| 1 MiB (today) | 23 / 24.1 MB / 3.71× | 42 / 44.0 MB / 3.44× |
| 4 MiB | 10 / 41.9 MB / 6.44× | 31 / 130.0 MB / 10.16× |

Byte amplification alone is the wrong objective — requests are weighted by RTT, and HDF5's VFD
interface is synchronous, so requests are serial. Modelling `t = requests × RTT + bytes / bandwidth`
on the 1.19 GB workload:

| block size | local / buffer | LAN, 5 ms / 125 MB/s | CDN, 50 ms / 12.5 MB/s | mobile, 150 ms / 5 MB/s |
|---|---|---|---|---|
| none | **3 ms** | 6947 ms | 69473 ms | 207908 ms |
| 64 KiB | 3 ms | 1282 ms | 12816 ms | 37840 ms |
| 256 KiB | 4 ms | 596 ms | 5960 ms | 17000 ms |
| 1 MiB | 9 ms | **562 ms** | **5620 ms** | **15100 ms** |
| 4 MiB | 26 ms | 1195 ms | 11950 ms | 30650 ms |

The optimum is 256 KiB–1 MiB at every non-zero latency, and *no cache* at zero latency. 64 KiB is
2–4× worse than 1 MiB on any real network despite winning on bytes. This is precisely the
single-metric trap the performance gates below warn about, so the block size must be chosen against
a latency-weighted model and the gates must report requests, bytes and wall-clock together.

Expose the size as a setting anyway: 4 MiB is already past the knee, and a bandwidth-constrained
link shifts the optimum down toward 256 KiB.

**Amplification is scale-invariant.** Note the ratio columns above are nearly identical across a
10× file-size range (1.17 vs 1.19; 1.65 vs 1.72; 3.71 vs 3.44). Blocks touched and raw bytes both
scale with dataset dimensions, so the ratio is a property of HDF5's layout density and the chosen
block size — not of file size. Ratios quoted for single micro-workloads (e.g. 448× for reading one
categorical column from the 1.19 GB file) are artifacts of a tiny denominator; the absolute overhead
there is bounded at single-digit MB, because metadata stays clustered in a handful of blocks
regardless of how large the file grows.

### Feature flags: advertise `DATA_SIEVE`, and only that

sec2 advertises seven `H5FD_FEAT_*` flags. Traced through the HDF5 1.14.6 consumers, for a
**read-only** workload exactly one matters:

- `H5FD_FEAT_DATA_SIEVE` — **the only read-path flag.** One consumer, `H5D__contig_readvv`
  (`H5Dcontig.c:1233`), 64 KiB default buffer. Applies to **contiguous** datasets only;
  `H5Dchunk.c` has zero sieve references. Worth 4–32× fewer VFD reads on uncompressed files.
- `AGGREGATE_METADATA`, `AGGREGATE_SMALLDATA` — write-side only (`H5MFaggr.c`). No read effect.
- `ACCUMULATE_METADATA` — **dead on read-only files.** `H5F__accum_read` gates on the flag but the
  hit branch also requires `H5_addr_defined(accum->loc)`, and every assignment to `accum->loc` is
  inside `H5F__accum_write`. Open initializes it to `HADDR_UNDEF`. It never coalesces reads.
  *(An earlier draft of this document named this flag as the one at risk. That was wrong.)*
- `POSIX_COMPAT_HANDLE` — **must not be advertised.** It promises a real `int` fd, which a
  `FileHandle`-backed VFD cannot honestly provide. Its only consumer is symlink-name cosmetics in
  `H5F__build_actual_name`, which falls back gracefully.

Because the sieve only helps contiguous datasets and real `.h5ad` files are overwhelmingly chunked
(the 1.19 GB reference file is 89 chunked / 6 contiguous, and all 6 contiguous are scalar `uns/`
strings), a missing `DATA_SIEVE` is invisible on compressed fixtures. **The gate for it must run on
an uncompressed fixture or it will not detect the regression at all.**

Also worth implementing, and a win the current VFD does not have: `read_vector` (currently
`nullptr`), which lets HDF5 hand scattered chunk reads to the driver in one call for merge-and-batch.

### Stepping stone: `H5FD_CORE` with a whole-file image

Read the file via `duckdb::FileSystem` into one buffer and hand it to HDF5's CORE driver. The CORE
VFD is confirmed present in the prebuilt.

This is more attractive than it first appears, because in the dominant browser flow
(`registerFileBuffer`) the bytes are *already* resident in wasm memory — no new download. It gives
correct duckdb-wasm integration with no VFD to write, and the same code path native and wasm. Use
`H5Pset_file_image_callbacks` with a no-copy op, or it is 2× file size resident.

It cannot do ranged reads, and it is capped by 32-bit `size_t`. Ship it as a size-gated path that
the VFD later supersedes, and keep it as the fast path for small files.

### Rejected: MEMFS staging

Writing the file into Emscripten MEMFS via `Module.FS` and letting sec2 work unchanged (what h5wasm
does) requires users to reach around DuckDB into the Emscripten runtime. Not a shippable API, and if
it lands in the test suite it bakes itself in. Useful only as a throwaway spike.

---

## Impact on Native Platforms

Phase 3 changes the I/O path everywhere, so it must be justified on native terms too, not only as
wasm enablement.

**Per-read overhead is a non-issue.** Measured in the cross-shared-library shape a loadable
extension actually has, `duckdb::FileHandle::Read` costs **+0.212 ns per read** over a direct
`pread`, against a warm-page-cache `pread` of 340–400 ns and a syscall floor of 104.5 ns. On the
heaviest measured query (one gene column from the 1.19 GB file: 49,650 reads, 8.85 s wall) the total
added dispatch tax is ~10.5 µs, or 0.0001% of wall time. The path does no allocation, takes no lock,
and does no path parsing per read; `VirtualFileSystem` protocol dispatch is per-open only.

**Local is a wash** provided the two rules above hold (no local block cache; advertise `DATA_SIEVE`).
Windows may get slightly faster: HDF5's sec2 falls back to `lseek`+`read` (two syscalls) where
`H5_HAVE_PREADWRITE` is undefined, which MSVC fails, while `LocalFileSystem::Read` issues one
`ReadFile` with `OVERLAPPED`.

**Remote is a robustness win and a wall-clock wash.** Both designs are libcurl with one keep-alive
connection per open file, so there is no transport win — everything is caching policy. What is
gained: retries with exponential backoff and `Retry-After` (the current VFD has **zero** retries),
ETag validation, credential refresh for expiring STS credentials, and the full secret surface
(`s3`/`r2`/`gcs`/`aws`, `url_style`, `requester_pays`, bearer tokens) against the current
hand-rolled six-key subset — note `test/sql/remote/s3_error_messages.test:31` already sets
`URL_STYLE 'path'`, which the current reader ignores and which works only by an endpoint heuristic.

**Build and size are unambiguously better.** Measured by relinking without
`libcurl.a`/`libssl.a`/`libcrypto.a`/`h5fd_http.cpp.o` on osx_arm64: **−5.39 MB raw / −2.59 MB
gzipped, a 21.9% cut in the user-facing download** (11.81 → 9.22 MB gz). Similar on every native
arch except mingw. In CI, openssl + curl cost ~695 s of runner wall time per full matrix, and the
vcpkg binary cache restores zero packages on macOS/Windows because `vcpkg-configuration.json` pins
its own baseline — on osx_arm64 those two ports are 49% of the build step. The shipped binaries also
currently statically embed OpenSSL 3.5.2 and curl 8.16.0; that CVE surface moves to httpfs.

**mingw gains remote support for free.** `windows_amd64_mingw` today has none — `CMakeLists.txt:89-90`
excludes curl/OpenSSL there because OpenSSL fails to build in vcpkg on that platform, so `s3://`
falls into the local branch and dies with `File not found: s3://…`.

**Two behaviour changes to document, neither performance:**

- HDF5's sec2 takes `flock(LOCK_SH|LOCK_NB)` on read-only open; `LocalFileSystem` with
  `FILE_FLAGS_READ` takes no lock. Concurrent-writer detection disappears.
- `http(s)://` becomes a hard dependency on httpfs being loaded. It autoloads in stock DuckDB, but
  offline, air-gapped, `autoload_known_extensions=false`, or self-built-without-autoloading users
  regress from working to `MissingExtensionException`. Note `s3://` already requires httpfs today
  (secret types and `s3_*` settings are registered by it), so only bare `http(s)://` is newly
  dependent.

---

## Implementation Plan

> **Status (2026-08-19):** Phase 0 landed in PR #34 (exclusion, deploy-matrix trim, docs).
> Phase 1 landed with the `LINKED_LIBS` fix plus the Tier 0 symbol gate and Tier 1 Node load
> smoke in CI (`wasm-checks` job); `wasm_mvp`/`wasm_eh` build and load (~3.0 MB artifacts).
> Phase 2 landed beyond the minimal path: the **ranged `duckdb::FileSystem` VFD**
> (`src/vfd/h5fd_duckdb_fs.cpp`, wasm-only scope for now - the native swap still waits on the
> performance gates) makes every duckdb-wasm byte source queryable lazily: drag & drop files
> (FileReaderSync), buffers, and HTTP/S3 range requests (measured in Chrome: 110 MB file,
> schema + 150k-row aggregate = ~50 range requests / ~12 MB). Scan functions and `ATTACH`
> verified on both arches; real-fixture phase in the CI load smoke; in-browser demo
> (`demo/browser/`). duckdb-wasm quirks worth recording: ranged HTTP requires
> `filesystem: {reliableHeadRequests: true, forceFullHTTPReads: false}` at `db.open` (this
> build otherwise force-downloads whole files), and its Node build cannot open `http://` paths
> at all (no XHR in the worker). The un-exclusion criteria below are MET (Tier 1 + the first
> file-access assertions are green in CI), so `wasm_mvp`/`wasm_eh` ship with the next
> community-extensions release; `wasm_threads` stays excluded (B4). One upstream limitation found:
> on `wasm_mvp` a side-module C++ throw dies in the loader's invoke wrappers
> (`_setThrew is not defined`) - happy paths are unaffected, error messages are eh-clean only.
> Still open from Phase 0: the upstream community-extensions `excluded_platforms` PR (Monitor 3
> nags until it lands) and retraction of historical wasm objects from the S3 channel.


### Phase 0 — Stop shipping a broken artifact (about a week)

1. Land `scripts/wasm_symbol_check.py` (Tier 0 below) **first, red**, against the current artifact,
   so the bug is documented by a failing test with a precise symbol list.
2. Exclude wasm. This is **three** edits, not two:
   - `exclude_archs: 'wasm_mvp;wasm_eh;wasm_threads'` in `MainDistributionPipeline.yml`'s
     `duckdb-stable-build`;
   - **trim the deploy matrix at `:189`** — it hardcodes the three wasm arches and
     `download-artifact` at `:202-205` will fail for them otherwise;
   - `excluded_platforms: "wasm_mvp;wasm_eh;wasm_threads"` in
     `duckdb/community-extensions/extensions/anndata/description.yml` (upstream PR; precedents
     exist: `zarr`, `hdfs`, `orc`, `pdf`).
3. Retract the second channel. `scripts/deploy-extension.sh` also publishes wasm to
   `s3://software-releasers…`, which `excluded_platforms` does not touch, and `README.md:24` points
   users at it. Remove the wasm objects, or they stay fetchable forever.
4. Honesty fixes: `RELEASE.md:45-47` (claims wasm is tested), `README.md:214` ("thread-safe on all
   platforms"), `README.md:9-15` (the unqualified quick-start is the repro).
5. Extend `UpcomingDuckdbPipeline.yml` Monitor 3 to compare `excluded_platforms`, not just `version`
   and `ref` — otherwise the exclusion can silently disappear upstream.

### Phase 1 — Make it load (about a week)

6. Set `DUCKDB_EXTENSION_ANNDATA_LINKED_LIBS` in `CMakeLists.txt` with all four archives as one
   quoted string. Add `URL_HASH` to the FetchContent.
7. Turn the Tier 0 gate green. Expect ~446 KB → ~3 MB.
8. Land Tier 1 (Node load smoke). Do **not** un-exclude yet — a module that loads and then answers
   "File not found" for every path reads to users as "broken with my file", which is worse than
   "unsupported".

### Phase 2 — Make it work for registered buffers (2–3 weeks)

9. `H5FD_CORE` + whole-file image via `duckdb::FileSystem`, size-gated.
10. Tier 2 golden sqllogictest replay for the fixture-reading tests.
11. Un-exclude `wasm_mvp` + `wasm_eh` once Tier 1 and the first four Tier 2 tests pass. This is the
    gating criterion; make it explicit in the descriptor PR.

### Phase 3 — The port proper (6–10 weeks)

11a. **Land the performance gates first, against the current sec2/curl build, and record the
    baselines.** They are cheap (the benchmark harness is already wired — see Performance gates) and
    they are worthless if written after the change, because there would be nothing to compare to.
    No VFD work should merge to the default path until these are green.

12. `src/vfd/h5fd_duckdb.cpp` — the `duckdb::FileSystem`-backed VFD, general from the start, with
    per-backend cache policy from day one (see Cache policy must be per-backend).
13. The `H5FileCache` lifetime/scope refactor (item 4 above) and the `H5ReaderMultithreaded`
    constructor change. Budget these separately; they are not glue.
14. Land the VFD behind the existing dispatch, prove parity against the native remote suite and the
    MinIO job, **then** delete `h5fd_http.cpp` and drop curl/OpenSSL in a follow-up PR. Do not
    combine.

### Phase 4 — Performance and ergonomics (unscheduled)

15. B3: a long/tidy `(obs_idx, var_id, value)` output mode for X/layers, plus the 32-bit overflow
    audit of the reader.
16. B6: lazy `/uns` key enumeration.
17. Make prefetch size and cache size real settings; implement `query` and evaluate `read_vector`.
18. `ReadSparseMatrixCSC` (`h5_reader_multithreaded.cpp:3585-3696`) reads the full matrix per
    2048-row chunk; obsp/varp (`:3348-3366`) does two 1-element `H5Dread`s per non-zero. Separate
    perf workstream.

---

## Testing

Unit tests are part of each module above, not a separate phase. Tiers 0 and 1 would each have caught
the current bug; nothing resembling them existed.

### Tier −1 — native VFD tests (free)

Once the `duckdb::FileSystem` VFD replaces sec2 natively, the **entire existing native suite**
exercises it with no new harness: 20 `test/sql/*.test` plus 5 `test/sql/remote/*.test`. This is the
highest-value tier in the plan and costs nothing, and it is the strongest argument for making the
VFD general rather than wasm-only.

These are correctness tests only. They will stay green through a 2× read-count regression, which is
why the performance gates below are a separate, blocking requirement.

### Tier 0 — link-level symbol contract (half a day, zero flakiness, ~200 ms)

`scripts/wasm_symbol_check.py` — a pure-Python wasm section parser, no wabt or llvm-nm needed
(it must handle import kind `0x04` and exception tags or it crashes on `wasm_eh`). Assertions, each
individually diagnosing:

1. `\x00asm` magic; `dylink.0` and `duckdb_signature` custom sections present, signature last.
2. Exports contain `anndata_duckdb_cpp_init`.
3. `imports(side) − exports(side) − exports(main) − allowlist == ∅`, across all import kinds and
   modules (`env`, `GOT.func`, `GOT.mem`). **Subtracting the side module's own exports is essential** —
   without it, 207 vague-linkage symbols and ~956 `GOT.mem` self-references read as false failures.
4. Redundant but legible: no unresolved `H5*`, `inflate*`, `deflate*`, `SZ_*`, `aec_*`. This is what
   turns a 300-line symbol diff into a one-line message naming the actual bug — and it is exactly
   the check that catches the `libhdf5.a`-alone near-miss.
5. Size floor (> 2 MB) and ceiling (a growth budget). HDF5 linked cannot yield 446 KB.
6. `duckdb_signature` metadata: `duckdb_version` matches the pin, `platform` matches the arch.

`test/wasm/allowed_undefined.txt` is a reviewed, comment-annotated allowlist.

**This gate is version-specific, not a fixed contract.** `GOT.mem` entries resolve eagerly at
`dlopen`, so a duckdb-wasm bump can break `LOAD` with no change on our side. The pin-lockstep rule
in `CLAUDE.md`'s "Upgrading DuckDB Version" checklist needs a third entry: duckdb-wasm npm version ↔
`duckdb_version:` ↔ the allowlist.

### Tier 1 — Node load and smoke (1–2 days, `wasm_mvp` + `wasm_eh` only)

`@duckdb/duckdb-wasm` pinned to the dev-channel build carrying our DuckDB version (npm `latest`
lags and will fail with a metadata error), plus `web-worker` pinned to 1.2.0. Boot `AsyncDuckDB`
with a worker; `db.instantiate(mainModule, null)` — the second argument is the COI pthread worker
and must be `null` for eh/mvp. Open with `allowUnsignedExtensions: true`.

Serve the locally built artifact from a throwaway `http.createServer` on `127.0.0.1:0` at
`/v<version>/<arch>/anndata.duckdb_extension.wasm`, then
`SET custom_extension_repository='http://127.0.0.1:<port>'` and `LOAD anndata`. Note `LOAD
'/abs/path.wasm'` is not an alternative in Node — the full-path branch still goes through the
fetch machinery.

**Operational rule that will otherwise cause chronic flakiness:** the Node loader caches non-200
bodies. A 404 body gets written into `$HOME/.duckdb/extensions/…` and reused. Every invocation must
run under a fresh `HOME` (`export HOME="$(mktemp -d)"` in the npm script, so it survives crashes).

Assertions, increasing depth: `LOAD` succeeds → `anndata_version()` matches `VERSION` →
`duckdb_extensions()` shows it loaded → all 13 table functions and 2 scalars appear in
`duckdb_functions()` → a real query against a registered fixture → `ATTACH … (TYPE ANNDATA)` and
`SHOW TABLES` lists `obs`/`var`/`X`.

Also assert **request count and peak memory** here (`HEAP8.byteLength`). Both are the properties
most likely to regress silently and both are nearly free in this harness.

### Tier 2 — golden sqllogictest replay (the regression net)

Reuse `test/sql/*.test` verbatim — one file, two engines — so "identical results native and wasm" is
enforced by construction. That is the only thing that catches 32-bit width and alignment bugs. Our
files use only `query`, `statement`, `require`, `require-env`; no `loop`/`foreach`/`restart`, a
strict subset of what existing prior art handles.

Prior art worth copying rather than reinventing:
[`Query-farm-haybarn/haybarn-extension-wasm-tester`](https://github.com/Query-farm-haybarn/haybarn-extension-wasm-tester)
(MIT) ran 11,219 sqllogictest records across 124 community extensions, 57% passing. Steal its
normalisation trick — wrap each query as `SELECT COLUMNS(*)::VARCHAR FROM (<query>)` so DuckDB
itself owns boolean/float/NULL formatting, sidestepping JS formatting mismatches.

Manifest `test/wasm/wasm_suite.txt` with a one-line reason per exclusion, and **ratchet**: CI fails
if the passing count drops. Start with the four that need only a fixture read (`anndata_basic`,
`anndata_sparse`, `anndata_sparse_csc`, `obs_categorical`). Plan for coverage that grows; 57% is the
realistic prior, not 100%.

Note `statement error` messages are native-path-shaped and will need per-file exceptions — expect
some hand-maintained state despite the ratchet.

### Tier 3 — headless browser (3–5 days, nightly, not a PR gate)

Justified only by what Node cannot reach: COI/`SharedArrayBuffer`; `FileReaderSync` and OPFS
`FileSystemSyncAccessHandle`, both worker-only by spec; the browser XHR branch, which unlike Node's
checks `status != 200`; real range behaviour against a CORS origin. Serve COOP `same-origin` +
COEP `require-corp` on **every** response including the wasm and worker JS — one missing header
silently drops `crossOriginIsolated`.

### Fixtures

21 committed `.h5ad` totalling 1.5 MB, largest 201 KB — all trivially wasm-resident. The 13
`test/python/create_test_*.py` generators are wired into nothing (no references in `Makefile`,
`scripts/`, or `.github/`). Consolidate into `test/python/make_fixtures.py` with `--out-dir`,
`--only`, seeded RNG, and a `make fixtures` target — this is a **build dependency** for the new
fixtures below, not cleanup.

Generate the same content **both gzip-compressed and uncompressed**: that pair is the direct
functional test that `libz.a` is actually linked. Add an szip fixture as a positive test that the
multi-archive link works. Variable-length strings are the most VFD-sensitive read pattern (global
heap objects live far from the dataset; a broken seek shows there first).

Do not gate CI on byte-identical regeneration until `anndata`/`h5py` are pinned exactly
(`pyproject.toml` currently has `anndata>=0.12.2`, a floor). Assert on query results, never hashes.

### Negative tests

Each asserting a specific error substring, so "clear error" degrading to "hang / crash / wrong
answer" is caught:

- unregistered path → `File not found`
- non-HDF5 buffer → `not a valid HDF5 file`
- valid HDF5 that is not AnnData
- LZF filter registration under `-sSIDE_MODULE=2` — currently **unknown**; `src/lzf/*.c` is not
  excluded on Emscripten and `anndata_extension.cpp:36-40` only warns to stderr on failure. Test
  both directions.
- szip fixture should **succeed**
- OOM → clean error, not a worker abort
- `ATTACH 'https://…'` on wasm → today falls through the `DUCKDB_NO_REMOTE_VFD` guard and emits a
  misleading `File not found: https://…`. Add an explicit guard with a purpose-built message; the
  test forces the fix.
- 404 extension repo → fails cleanly and leaves no poisoned cache
- **register → attach → detach → drop → re-register** → the lifetime/invalidation class the VFD
  introduces (design items 4 and 5). Currently uncovered by any tier.
- a native stub `FileSystem` that short-reads without throwing, to cover `WebFileSystem::Read`
  semantics — cheap fault injection, native, no browser needed.

---

## Metrics

Collected per CI run and tracked over time, so regressions are visible rather than inferred:

| metric | source | why |
|---|---|---|
| artifact size per arch, raw and brotli | Tier 0 | browser delivery cost; also the cheapest tripwire for a broken link |
| genuinely-unresolved symbol count | Tier 0 | the load-correctness oracle |
| HTTP request count per canonical query | Tier −1 (MinIO), Tier 1 | the dominant remote cost |
| bytes transferred per canonical query | same | distinguishes request count from volume |
| wall-clock per canonical query | same | the number users feel; requests × RTT |
| peak wasm heap (`HEAP8.byteLength`) | Tier 1 | the wasm32 ceiling in B3 |
| Tier 2 passing-test count | Tier 2 | the ratchet |

Two comparisons are worth plotting rather than tabulating, because the shape carries the argument:

1. **Requests and bytes vs VFD block size** (64 KiB / 256 KiB / 1 MiB / 4 MiB) per workload class —
   metadata, obs scan, X scan. There is a knee, and it is not in the same place for metadata as for
   raw data; a single block size is likely wrong. This directly sizes the cache and is the main
   tuning decision in the VFD.
2. **Before/after request count per query, native**, when the VFD replaces sec2 — the native
   regression risk in Tier −1.

Note the selection-vs-diagnostic distinction: block size must be **chosen** on the metadata and obs
workloads and then **reported** on X scans, not tuned on whichever workload flatters it. The 73%
CSR result above shows the two classes behave completely differently.

### Performance gates — blocking, not advisory

**Today the repo has none.** Verified: no `bench`/`timing`/`.timer` reference in any CI workflow, no
timing assertion in `test/`, and `Makefile` has only version and release targets.
`demo/test-timing.sh` is not a benchmark — its own header says it exists to calibrate `Sleep`
durations for a VHS demo recording; it hits live external URLs, uses whatever `duckdb` is on `PATH`,
asserts nothing, and is not in CI.

Most of the harness already exists and is simply unused:

- `extension-ci-tools/makefiles/duckdb_extension.Makefile:126` already passes
  `-DBENCHMARK_ROOT_DIRECTORY="$(PROJ_DIR)"` — DuckDB's `benchmark_runner` is **already pointed at
  this repo**. It has no `.benchmark` files to find.
- The same Makefile (`:138-139`) supports `BUILD_BENCHMARK=1`.
- `duckdb/scripts/regression/test_runner.py` does A/B comparison — `--old <runner> --new <runner>
  --benchmarks <csv>` with a percentage threshold, a `--regression-threshold-seconds` floor for fast
  benchmarks, repeated runs, and geomean reporting. DuckDB's own CI uses it
  (`ExtendedTests.yml:107-132`). Crucially it runs **both binaries now, on the same machine**, which
  is what makes a gate usable on noisy GitHub runners.

These gates must exist and be green **before the VFD becomes the default path**, not after.

**A. Local** — baseline is the current sec2 build. Count syscalls with an `LD_PRELOAD` / `dyld`
interpose shim on `pread`, or `strace -c -e pread64` on Linux.

| id | workload | gate |
|---|---|---|
| A1 | `ATTACH` alone, 1.19 GB file | ≤ 1.05× baseline on **both** read count and bytes |
| A2 | `ATTACH` + `count(*) FROM obs` on an **uncompressed** fixture | ≤ 1.05× read count — **this is the `DATA_SIEVE` gate; it is silent on gzip fixtures** |
| A3 | same on the gzip-chunked 1.19 GB file | ≤ 1.05× count, ≤ 1.10× wall |
| A4 | `sum(<one gene>)` from CSR X | ≤ 1.02× wall — the dispatch-tax sanity check |
| A5 | 100 × ATTACH/DETACH of a small fixture | ≤ baseline; should *improve* (4 fewer probes per open) |
| A6 | peak RSS on A3 and A4 | ≤ 1.05× — catches an accidental local block cache |

**B. Remote** — against the instrumented localhost range server, run twice: at 0 ms and with an
injected 50 ms per-request delay.

| id | workload | gate |
|---|---|---|
| B1 | `ATTACH` alone | **≤ 3 requests** — today's ~16 is the serial prefetch; this must improve |
| B2 | `ATTACH` + var / obs / obsm reads | ≤ 1.25× requests **and** ≤ 1.10× bytes vs the 19–25 request baseline |
| B3 | one-gene column from CSR X | ≤ 1.25× requests, ≤ 1.10× bytes vs the 78 request / 73% baseline |
| B4 | B2 then B3 in the **same** session | incremental requests for B3 strictly less than a cold B3 — proves the cross-query cache survived |
| B5 | forced metadata↔data↔metadata interleaving | catches sliding-window thrash if httpfs buffering is relied on |
| B6 | B3 with the server injecting one 503 | must succeed — the retry gate |

**Report all three of requests / bytes / wall for every remote row.** Any single-metric gate is
gameable in both directions: exact-range caching lowers bytes while exploding requests, and a larger
prefetch does the reverse.

---

## Open Questions

Each materially changes the plan; each has a cheap decisive experiment. Q1 and Q2 from the original
investigation are now closed by execution and recorded above.

**Q3 — Does `wasm_threads` link at all once HDF5 is in?** `libhdf5.a` has no `-pthread`;
`wasm_threads` sets `-sSHARED_MEMORY=1`. *Experiment*: repeat the Phase 1 link with
`-pthread -sSHARED_MEMORY=1`. Ten minutes, and it decides whether `wasm_threads` is excluded
permanently or merely deferred.

**Q4 — Should we build HDF5 for wasm ourselves?** The prebuilt is `-O0`, unpinned by hash, and
carries features we do not need. Building it (`-O2 -ffunction-sections`, no hl/cxx/tools, szip
decision, pthread decision) addresses size, supply chain and Q3 at once. *Experiment*: check
whether vcpkg can target `wasm32-emscripten` — note `vcpkg.json` marks **every** dependency
`"platform": "!emscripten"` today, so the wasm leg currently resolves to an empty install set, and
our `vcpkg-overlay/ports/hdf5` carries a `ros3` feature that must be disabled for wasm.

**Q5 — Does `WebFileSystem::Glob` cover `BUFFER`-protocol registered files?** Determines whether
wildcard queries can ever work in wasm. `glob_handler.cpp:80-85` currently throws telling the user
to load httpfs, which is unactionable in duckdb-wasm. *Experiment*: register three fixtures in the
Tier 1 harness and run `glob('*.h5ad')`.

**Q6 — What does `read_vector` buy?** Currently `nullptr`, so HDF5 falls back to a serial loop and
the driver never sees coalescing opportunities. *Experiment*: instrument the new VFD with a call
counter and compare paths on the obs-scan workload. Make it a task, not an assumption.

**Q7 — Is remote `.h5ad` in a browser a product or a demo?** HDF5's VFD API is synchronous and
duckdb-wasm's HTTP is synchronous XHR: no pipelining, no cancellation, worker blocked. Even the good
case above (~20 requests) is 1–3 s at typical RTT; a CSR gene query at 78 requests is far worse.
Overlapping requests would require ASYNCIFY/JSPI, which this design deliberately avoids.
*Experiment*: wall-clock the Tier 1 harness against a throttled origin before promising remote
support in docs. This is a positioning decision, not just a perf number.

**Q8 — Does the LZF filter register under `-sSIDE_MODULE=2`?** Unknown; currently fails silently to
stderr. Covered by the negative tests above.

---

## Documentation

For a browser port the docs are part of the feature. `grep -i wasm README.md docs/*.md` currently
returns zero hits. Required, and not optional:

- A platform-support matrix stating exactly which arches are supported and what "supported" means.
- A browser section with `registerFileBuffer` / `registerFileURL` examples, since a duckdb-wasm user
  cannot otherwise discover that `ATTACH 'x.h5ad'` needs a registered file.
- The `directIO=false` guidance for `registerFileHandle` — `directIO=true` bypasses duckdb-wasm's
  16 KB page cache, which for HDF5 turns every 48-byte superblock read into a `Blob.slice`.
- A shell.duckdb.org snippet. It is the cheapest possible proof of value and the natural demo;
  `demo/` currently has no browser story and hardcodes an absolute local path.
- CHANGELOG entries at each phase, and a note that routing HDF5 I/O through `duckdb::FileSystem`
  makes `https://`/`s3://` a hard dependency on httpfs being loaded — a **behaviour change** for
  native remote users, with different error text and secret resolution. The existing remote tests
  cannot catch this because they `INSTALL httpfs; LOAD httpfs;` unconditionally.

---

## Files to Create and Modify

### New

| path | purpose |
|---|---|
| `scripts/wasm_symbol_check.py` | Tier 0 gate |
| `scripts/repro_wasm_load.mjs` | committed reproduction of the load failure |
| `test/wasm/allowed_undefined.txt` | reviewed allowlist |
| `test/wasm/wasm_suite.txt` | Tier 2 manifest with per-exclusion reasons |
| `test/wasm/run_node.mjs` | Tier 1 harness |
| `test/wasm/sqllogic_replay.mjs` | Tier 2 replay |
| `test/python/make_fixtures.py` | consolidates the 13 orphaned generators |
| `src/vfd/h5fd_duckdb.cpp` | the `duckdb::FileSystem` VFD |

### Modified

| path | change |
|---|---|
| `CMakeLists.txt` | `DUCKDB_EXTENSION_ANNDATA_LINKED_LIBS`, `URL_HASH`, all four archives |
| `.github/workflows/MainDistributionPipeline.yml` | `exclude_archs`, deploy matrix, wasm test jobs |
| `.github/workflows/UpcomingDuckdbPipeline.yml` | Monitor 3 checks `excluded_platforms` |
| `src/include/h5_file_cache.hpp` | scope refactor, filesystem plumbing, drop POSIX probes |
| `src/include/h5_reader_multithreaded.hpp` | constructor signature |
| `src/anndata_scanner.cpp` | drop `ifstream` probes at `:96`, `:132` |
| `src/anndata_extension.cpp` | remove unconditional `std::cout` |
| `src/glob_handler.cpp` | wasm-appropriate error for remote globs |
| `scripts/deploy-extension.sh` | wasm retraction |
| `README.md`, `RELEASE.md`, `CHANGELOG.md`, `CLAUDE.md` | see Documentation |
| `src/h5_reader.cpp`, `src/include/h5_handles.hpp` | **delete** (dead) |

---

## Risks

| risk | likelihood | mitigation |
|---|---|---|
| **Remote loses aligned block coalescing** — caching delegated to httpfs or DuckDB's EFC, neither of which coalesces | **high** | port `BlockCache` verbatim; gates B1–B3. ~20 requests → HDF5's raw read count would be minutes instead of seconds at 50 ms RTT |
| `H5FileCache` lifetime refactor is larger than scoped | high | treat as its own PR with its own tests; do not bundle with the VFD |
| A block cache gets applied to **local** files | medium | assert `IsRemoteFile == false ⇒ cache off` in a unit test; gates A1/A6 |
| `query = nullptr` copied over from `h5fd_http.cpp:936` | medium | gate A2 **on an uncompressed fixture** — invisible on gzip |
| `opener == nullptr` passed to `OpenFile` | medium | `s3://` silently routes to `LocalFileSystem`; test that a bad bucket errors as HTTP, not "File not found" |
| 16-serial-RTT prefetch carried forward unchanged | medium | gate B1 (≤ 3 requests for bare ATTACH) |
| Error-text contract break in `test/sql/remote/s3_error_messages.test` | medium | httpfs 403/404 text differs; re-wrap `IOException` at open |
| duckdb-wasm bump breaks `LOAD` with no change on our side | medium | Tier 0 is per-version; add the npm pin to the upgrade checklist |
| Remote `.h5ad` in browser is too slow to be useful | medium | Q7 — measure before documenting it as supported |
| `wasm_threads` never works | medium | exclude it; it is near-zero value (no `coi` bundle is published) |
| CSR gene queries dominate real usage and stay slow | medium | document CSC as the recommended layout; B3/Phase 4 |
| Exclusion silently disappears upstream | low | Monitor 3 extension in Phase 0 |
