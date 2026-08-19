// See wasm_file_image.hpp for why this exists. Compiled to (almost) nothing
// outside Emscripten builds.

#ifdef __EMSCRIPTEN__

#include "wasm_file_image.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"

#include <vector>

namespace duckdb {

static DatabaseInstance *wasm_db_instance = nullptr;

void SetWasmDatabaseInstance(DatabaseInstance *db) {
	wasm_db_instance = db;
}

hid_t AnndataOpenFileImage(const std::string &path) {
	if (!wasm_db_instance) {
		throw std::runtime_error("AnnData wasm file access is not initialized (extension not loaded?)");
	}
	auto &fs = wasm_db_instance->GetFileSystem();

	unique_ptr<FileHandle> handle;
	try {
		handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	} catch (std::exception &ex) {
		// The most common cause in a browser: the file was never registered.
		auto slash = path.find_last_of('/');
		std::string name = slash == std::string::npos ? path : path.substr(slash + 1);
		throw std::runtime_error("File not found: " + path +
		                         ". In DuckDB-WASM a file must be registered before use, e.g. "
		                         "db.registerFileBuffer('" +
		                         name + "', new Uint8Array(bytes)). (" + ex.what() + ")");
	}

	auto file_size = handle->GetFileSize();
	if (file_size <= 0) {
		// duckdb-wasm's WebFileSystem auto-creates an empty entry when an
		// unknown path is opened, so "empty" here almost always means "never
		// registered" - give the actionable hint rather than "File is empty".
		auto slash = path.find_last_of('/');
		std::string name = slash == std::string::npos ? path : path.substr(slash + 1);
		throw std::runtime_error("File not found (or empty): " + path +
		                         ". In DuckDB-WASM a file must be registered before use, e.g. "
		                         "db.registerFileBuffer('" +
		                         name + "', new Uint8Array(bytes)).");
	}

	// Read the whole file. Use the streaming Read (returns bytes read) in a
	// loop: WebFileSystem backends may short-read without throwing, so the
	// byte count must be verified here.
	std::vector<uint8_t> image(static_cast<size_t>(file_size));
	idx_t offset = 0;
	while (offset < static_cast<idx_t>(file_size)) {
		auto n = handle->Read(image.data() + offset, static_cast<idx_t>(file_size) - offset);
		if (n <= 0) {
			throw std::runtime_error("Short read at byte " + std::to_string(offset) + " of " +
			                         std::to_string(file_size) + ": " + path);
		}
		offset += static_cast<idx_t>(n);
	}
	handle.reset(); // release the duckdb handle immediately; HDF5 owns a copy

	// Hand the bytes to HDF5's CORE driver as a read-only file image.
	hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
	if (fapl < 0) {
		throw std::runtime_error("H5Pcreate failed");
	}
	H5Pset_fclose_degree(fapl, H5F_CLOSE_SEMI);
	// increment=1MB is irrelevant for read-only; backing_store=false: never
	// write anything back to a filesystem.
	if (H5Pset_fapl_core(fapl, 1 << 20, false) < 0) {
		H5Pclose(fapl);
		throw std::runtime_error("H5Pset_fapl_core failed");
	}
	// H5Pset_file_image copies the buffer into the property list; the CORE
	// driver copies it again on open. Closing the fapl below frees the plist
	// copy, and `image` frees on return, so steady state is one copy owned by
	// the open HDF5 file. (H5Pset_file_image_callbacks could eliminate the
	// transient copies; not worth it until large files matter here.)
	if (H5Pset_file_image(fapl, image.data(), image.size()) < 0) {
		H5Pclose(fapl);
		throw std::runtime_error("H5Pset_file_image failed");
	}

	hid_t file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
	H5Pclose(fapl);
	if (file < 0) {
		throw std::runtime_error("File is not a valid HDF5 file: " + path);
	}
	return file;
}

} // namespace duckdb

#else

// Avoid an empty translation unit on non-wasm builds.
namespace duckdb {
void AnndataWasmFileImageUnused() {
}
} // namespace duckdb

#endif // __EMSCRIPTEN__
