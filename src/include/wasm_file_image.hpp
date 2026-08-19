#pragma once
//===----------------------------------------------------------------------===//
// WASM file access via HDF5 CORE-driver file images
//
// In DuckDB-WASM, files registered by the user (registerFileBuffer,
// registerFileHandle, OPFS, HTTP) live in duckdb-wasm's WebFileSystem - a
// duckdb::FileSystem implementation. HDF5's default sec2 driver, by contrast,
// reads through Emscripten's MEMFS, which duckdb-wasm never populates. The two
// filesystems are disjoint, so H5Fopen(path) can never see a registered file.
//
// This is the minimal bridge (spec/wasm-support-spec.md, "Stepping stone:
// H5FD_CORE with a whole-file image"): read the entire file through
// duckdb::FileSystem and hand the bytes to HDF5's CORE driver as a file image.
// In the dominant browser flow (registerFileBuffer) the bytes are already
// resident in wasm memory, so this costs one additional copy held by the CORE
// driver for the lifetime of the open file. Whole-file resident means this is
// bounded by the wasm32 address space; the ranged-read VFD (spec Phase 3)
// supersedes this for large/remote files.
//
// The DatabaseInstance is stashed at extension load rather than plumbed
// through every reader constructor: a wasm worker hosts exactly one
// DatabaseInstance, and the instance owns the extension, so it cannot dangle
// while extension code runs. This shortcut is wasm-only by construction.
//===----------------------------------------------------------------------===//

#ifdef __EMSCRIPTEN__

#include "hdf5.h"
#include <string>

namespace duckdb {

class DatabaseInstance;

//! Called once at extension load.
void SetWasmDatabaseInstance(DatabaseInstance *db);

//! Open `path` through duckdb::FileSystem and return an HDF5 file handle
//! backed by an in-memory CORE file image. Throws std::runtime_error with an
//! actionable message (mentioning registerFileBuffer) when the file is not
//! registered, and "not a valid HDF5 file" when the bytes are not HDF5.
hid_t AnndataOpenFileImage(const std::string &path);

} // namespace duckdb

#endif // __EMSCRIPTEN__
