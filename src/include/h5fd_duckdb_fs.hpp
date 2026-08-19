#pragma once
//===----------------------------------------------------------------------===//
// HDF5 virtual file driver backed by duckdb::FileSystem (wasm builds)
//
// In DuckDB-WASM every byte source a browser has - files registered from drag
// & drop (registerFileHandle / FileReaderSync), OPFS, in-memory buffers
// (registerFileBuffer), and HTTP/S3 via the browser network stack - is exposed
// as one duckdb::FileSystem (WebFileSystem) with synchronous positional reads.
// HDF5's default sec2 driver reads Emscripten MEMFS instead, which duckdb-wasm
// never populates, so none of those sources are reachable through it.
//
// This driver maps HDF5's VFD callbacks onto duckdb::FileHandle reads, giving
// HDF5 lazy ranged access to all of them at once: only the byte ranges a query
// touches are read - opening a multi-GB .h5ad costs a few hundred bytes of
// metadata, not the file. It replaces the earlier whole-file CORE-image bridge
// and removes its wasm32 file-size ceiling.
//
// Remote handles (http/https/s3 - FileSystem::IsRemoteFile) get a per-file
// 1 MiB-block LRU cache to coalesce HDF5's many small metadata reads into few
// range requests; local-ish handles read directly (a block cache in front of a
// local source is amplification, not latency hiding -
// spec/wasm-support-spec.md, "Cache policy must be per-backend").
//
// Wasm-only by scope, not by design: the same driver is the endpoint for
// native too (spec Phase 3), but swapping the native path waits on the
// performance gates. The DatabaseInstance is stashed at extension load (one
// instance per wasm worker; the instance owns the extension, so it cannot
// dangle while extension code runs).
//===----------------------------------------------------------------------===//

#ifdef __EMSCRIPTEN__

#include "hdf5.h"
#include <string>

namespace duckdb {

class DatabaseInstance;

//! Called once at extension load.
void SetWasmDatabaseInstance(DatabaseInstance *db);

//! Open `path` through duckdb::FileSystem with the ranged VFD and return the
//! HDF5 file handle. Throws std::runtime_error with an actionable message
//! (mentioning registerFileBuffer/registerFileHandle) when the file is not
//! registered, and "not a valid HDF5 file" when the bytes are not HDF5.
//! (The throw happens here, outside HDF5's C callbacks - the VFD callbacks
//! themselves are strictly no-throw and report through an error slot.)
hid_t AnndataOpenViaDuckdbFS(const std::string &path);

} // namespace duckdb

#endif // __EMSCRIPTEN__
