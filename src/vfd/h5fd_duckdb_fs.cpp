// HDF5 VFD backed by duckdb::FileSystem - see h5fd_duckdb_fs.hpp for design.
// Compiled to (almost) nothing outside Emscripten builds.

#ifdef __EMSCRIPTEN__

#include "h5fd_duckdb_fs.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"

#include <cstring>
#include <list>
#include <map>
#include <memory>
#include <vector>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Extension-load state and the no-throw error slot
//===--------------------------------------------------------------------===//

static DatabaseInstance *wasm_db_instance = nullptr;

void SetWasmDatabaseInstance(DatabaseInstance *db) {
	wasm_db_instance = db;
}

// HDF5's VFD callbacks are C entry points: a C++ exception must never unwind
// through them. Every callback catches everything and stashes the message
// here; AnndataOpenViaDuckdbFS turns it back into a C++ exception on the far
// side of the H5Fopen call. wasm is single-threaded, so a plain static is safe.
static std::string last_vfd_error;

static void StashError(const std::string &msg) {
	last_vfd_error = msg;
}

//===--------------------------------------------------------------------===//
// Per-file aligned block cache (remote handles only)
//
// HDF5 metadata access is hundreds of sub-4KiB reads scattered across the
// file; over HTTP/S3 each would be one synchronous range request. 1 MiB
// aligned blocks with a small LRU turn a metadata sweep from ~10^3 requests
// into ~10^1 (measured - spec/wasm-support-spec.md, "Read amplification").
// Local-ish sources skip the cache entirely: block-rounding a 48-byte read to
// 1 MiB against FileReaderSync or an in-memory buffer is pure amplification.
//===--------------------------------------------------------------------===//

class VfdBlockCache {
public:
	static constexpr idx_t BLOCK_SIZE = 1ULL << 20; // 1 MiB
	// 32 MiB cap per open file: H5FileCache's LRU keeps up to 8 files open,
	// so the worst-case aggregate is 8 x 32 = 256 MiB inside a wasm32 worker
	// with a ~2-4 GB ceiling shared with duckdb's own buffers. (Metadata
	// working sets measure ~12 MiB; raw-data scans bypass this cache below.)
	static constexpr size_t MAX_BLOCKS = 32;

	// Reads [offset, offset+size) into out, fetching missing blocks via fetch.
	// fetch(block_start, len, dst) must read exactly len bytes.
	template <typename FETCH>
	void Read(uint8_t *out, idx_t offset, idx_t size, idx_t file_size, FETCH &&fetch) {
		idx_t remaining = size;
		idx_t pos = offset;
		while (remaining > 0) {
			idx_t block_num = pos / BLOCK_SIZE;
			idx_t block_start = block_num * BLOCK_SIZE;
			idx_t block_len = std::min(BLOCK_SIZE, file_size - block_start);
			auto it = blocks_.find(block_num);
			if (it == blocks_.end()) {
				std::vector<uint8_t> data(block_len);
				fetch(block_start, block_len, data.data());
				it = blocks_.emplace(block_num, std::move(data)).first;
				lru_.push_front(block_num);
				lru_pos_[block_num] = lru_.begin();
				if (lru_.size() > MAX_BLOCKS) {
					idx_t evict = lru_.back();
					lru_.pop_back();
					lru_pos_.erase(evict);
					blocks_.erase(evict);
				}
			} else {
				// touch
				lru_.erase(lru_pos_[block_num]);
				lru_.push_front(block_num);
				lru_pos_[block_num] = lru_.begin();
			}
			idx_t in_block = pos - block_start;
			idx_t n = std::min(remaining, block_len - in_block);
			std::memcpy(out, blocks_[block_num].data() + in_block, n);
			out += n;
			pos += n;
			remaining -= n;
		}
	}

private:
	std::map<idx_t, std::vector<uint8_t>> blocks_;
	std::list<idx_t> lru_;
	std::map<idx_t, std::list<idx_t>::iterator> lru_pos_;
};

//===--------------------------------------------------------------------===//
// VFD file struct and callbacks
//===--------------------------------------------------------------------===//

struct H5FDDuckdbFile {
	H5FD_t pub;                       // must be first: HDF5 casts H5FD_t* <-> this
	unique_ptr<FileHandle> handle;
	std::unique_ptr<VfdBlockCache> cache; // remote handles only
	haddr_t eoa = 0;
	haddr_t eof = 0;
	std::string path;
};

// Read exactly `size` bytes at `offset` through the duckdb handle. The
// streaming Read (returns a count) is used - WebFileSystem backends may
// short-read without throwing, so the byte count must be verified here.
static void ReadExact(FileHandle &handle, idx_t offset, idx_t size, uint8_t *dst) {
	handle.Seek(offset);
	idx_t got = 0;
	while (got < size) {
		auto n = handle.Read(dst + got, size - got);
		if (n <= 0) {
			throw std::runtime_error("short read at byte " + std::to_string(offset + got) + " (wanted " +
			                         std::to_string(size) + " bytes)");
		}
		got += static_cast<idx_t>(n);
	}
}

static H5FD_t *H5FD_duckdb_open(const char *name, unsigned flags, hid_t /*fapl_id*/, haddr_t maxaddr) {
	try {
		if (flags & H5F_ACC_RDWR) {
			StashError("read-only driver: cannot open for writing: " + std::string(name));
			return nullptr;
		}
		if (!wasm_db_instance) {
			StashError("AnnData wasm file access is not initialized (extension not loaded?)");
			return nullptr;
		}
		auto &fs = wasm_db_instance->GetFileSystem();
		auto handle = fs.OpenFile(name, FileFlags::FILE_FLAGS_READ);
		auto file_size = handle->GetFileSize();
		if (file_size == 0) {
			// duckdb-wasm's WebFileSystem auto-creates an empty entry when an
			// unknown path is opened, so "empty" almost always means "never
			// registered". For remote URLs it can also mean the server did not
			// report a size (no Content-Length / Range support).
			std::string p(name);
			auto slash = p.find_last_of('/');
			std::string base = slash == std::string::npos ? p : p.substr(slash + 1);
			if (FileSystem::IsRemoteFile(p)) {
				StashError("Remote file reports size 0: " + p +
				           ". The server must support Range requests and report Content-Length "
				           "(and allow CORS in a browser).");
			} else {
				StashError("File not found (or empty): " + p +
				           ". In DuckDB-WASM a file must be registered before use, e.g. "
				           "db.registerFileHandle('" +
				           base + "', file, DuckDBDataProtocol.BROWSER_FILEREADER, false) or "
				           "db.registerFileBuffer('" +
				           base + "', new Uint8Array(bytes)).");
			}
			return nullptr;
		}
		if (static_cast<haddr_t>(file_size) > maxaddr) {
			StashError("file exceeds HDF5 maxaddr: " + std::string(name));
			return nullptr;
		}
		auto *file = new H5FDDuckdbFile();
		file->handle = std::move(handle);
		file->eof = static_cast<haddr_t>(file_size);
		file->path = name;
		if (FileSystem::IsRemoteFile(file->path)) {
			// Only paths with a remote scheme (http/https/s3/...) get the block
			// cache. A URL registered under a plain name (registerFileURL) is
			// not detected here - deliberately: duckdb-wasm's own HTTP
			// read-ahead already coalesces those (measured: obs scan on a
			// 110 MB file = 47 range requests), and a block cache in front of
			// registered buffers or FileReaderSync handles would be pure
			// amplification.
			file->cache.reset(new VfdBlockCache());
		}
		return reinterpret_cast<H5FD_t *>(file);
	} catch (std::exception &ex) {
		std::string p(name);
		if (FileSystem::IsRemoteFile(p)) {
			// A thrown remote open is a network/CORS/HEAD failure, not a
			// missing registration - do not tell the user to register a file.
			StashError("Failed to open remote file: " + p +
			           ". The server must support Range requests, report Content-Length, and "
			           "(in a browser) allow CORS. (" +
			           ex.what() + ")");
			return nullptr;
		}
		auto slash = p.find_last_of('/');
		std::string base = slash == std::string::npos ? p : p.substr(slash + 1);
		StashError("File not found: " + p +
		           ". In DuckDB-WASM a file must be registered before use, e.g. "
		           "db.registerFileHandle('" +
		           base + "', file, DuckDBDataProtocol.BROWSER_FILEREADER, false). (" + ex.what() + ")");
		return nullptr;
	} catch (...) {
		StashError("unknown error opening: " + std::string(name));
		return nullptr;
	}
}

static herr_t H5FD_duckdb_close(H5FD_t *file) {
	try {
		delete reinterpret_cast<H5FDDuckdbFile *>(file);
		return 0;
	} catch (...) {
		StashError("error closing file handle");
		return -1;
	}
}

static haddr_t H5FD_duckdb_get_eoa(const H5FD_t *file, H5FD_mem_t /*type*/) {
	return reinterpret_cast<const H5FDDuckdbFile *>(file)->eoa;
}

static herr_t H5FD_duckdb_set_eoa(H5FD_t *file, H5FD_mem_t /*type*/, haddr_t addr) {
	reinterpret_cast<H5FDDuckdbFile *>(file)->eoa = addr;
	return 0;
}

static haddr_t H5FD_duckdb_get_eof(const H5FD_t *file, H5FD_mem_t /*type*/) {
	return reinterpret_cast<const H5FDDuckdbFile *>(file)->eof;
}

static herr_t H5FD_duckdb_read(H5FD_t *_file, H5FD_mem_t type, hid_t /*dxpl*/, haddr_t addr, size_t size,
                               void *buf) {
	auto *file = reinterpret_cast<H5FDDuckdbFile *>(_file);
	try {
		if (size == 0) {
			return 0;
		}
		if (addr + size > file->eof) {
			StashError("read past end of file: " + file->path);
			return -1;
		}
		auto *dst = static_cast<uint8_t *>(buf);
		// Large raw-data (chunk) reads bypass the block cache: routing a
		// multi-MB sequential scan through it would evict the hot metadata
		// blocks between chunk reads and pay block-rounding amplification,
		// for zero reuse. The cache exists for HDF5's many small scattered
		// metadata reads.
		bool use_cache = file->cache && !(type == H5FD_MEM_DRAW && size >= VfdBlockCache::BLOCK_SIZE);
		if (use_cache) {
			file->cache->Read(dst, addr, size, file->eof, [&](idx_t off, idx_t len, uint8_t *block_dst) {
				ReadExact(*file->handle, off, len, block_dst);
			});
		} else {
			ReadExact(*file->handle, addr, size, dst);
		}
		return 0;
	} catch (std::exception &ex) {
		StashError("read failed for " + file->path + ": " + ex.what());
		return -1;
	} catch (...) {
		StashError("read failed for " + file->path);
		return -1;
	}
}

static herr_t H5FD_duckdb_write(H5FD_t * /*file*/, H5FD_mem_t /*type*/, hid_t /*dxpl*/, haddr_t /*addr*/,
                                size_t /*size*/, const void * /*buf*/) {
	StashError("read-only driver: write refused");
	return -1;
}

static herr_t H5FD_duckdb_query(const H5FD_t * /*file*/, unsigned long *flags) {
	if (flags) {
		// DATA_SIEVE is the only H5FD_FEAT_* flag with a read-path effect
		// (64 KiB coalescing for contiguous datasets). The write-side flags
		// are irrelevant for a read-only driver, and POSIX_COMPAT_HANDLE must
		// NOT be advertised - there is no int fd behind a duckdb FileHandle.
		*flags = H5FD_FEAT_DATA_SIEVE;
	}
	return 0;
}

static const H5FD_class_t H5FD_duckdb_g = {
    H5FD_CLASS_VERSION,      // version
    (H5FD_class_value_t)601, // value (custom VFD ID, must be >= 256; 600 = our http VFD)
    "duckdb_fs",             // name
    HADDR_MAX,               // maxaddr
    H5F_CLOSE_WEAK,          // fc_degree
    nullptr,                 // terminate
    nullptr,                 // sb_size
    nullptr,                 // sb_encode
    nullptr,                 // sb_decode
    0,                       // fapl_size (open() needs only the name)
    nullptr,                 // fapl_get
    nullptr,                 // fapl_copy
    nullptr,                 // fapl_free
    0,                       // dxpl_size
    nullptr,                 // dxpl_copy
    nullptr,                 // dxpl_free
    H5FD_duckdb_open,        // open
    H5FD_duckdb_close,       // close
    nullptr,                 // cmp
    H5FD_duckdb_query,       // query
    nullptr,                 // get_type_map
    nullptr,                 // alloc
    nullptr,                 // free
    H5FD_duckdb_get_eoa,     // get_eoa
    H5FD_duckdb_set_eoa,     // set_eoa
    H5FD_duckdb_get_eof,     // get_eof
    nullptr,                 // get_handle
    H5FD_duckdb_read,        // read
    H5FD_duckdb_write,       // write
    nullptr,                 // read_vector
    nullptr,                 // write_vector
    nullptr,                 // read_selection
    nullptr,                 // write_selection
    nullptr,                 // flush
    nullptr,                 // truncate
    nullptr,                 // lock
    nullptr,                 // unlock
    nullptr,                 // del
    nullptr,                 // ctl
    H5FD_FLMAP_DICHOTOMY     // fl_map
};

//===--------------------------------------------------------------------===//
// Public entry point
//===--------------------------------------------------------------------===//

hid_t AnndataOpenViaDuckdbFS(const std::string &path) {
	static hid_t driver_id = H5I_INVALID_HID;
	if (driver_id == H5I_INVALID_HID) {
		driver_id = H5FDregister(&H5FD_duckdb_g);
		if (driver_id < 0) {
			throw std::runtime_error("failed to register the duckdb_fs HDF5 driver");
		}
	}

	hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
	if (fapl < 0) {
		throw std::runtime_error("H5Pcreate failed");
	}
	H5Pset_fclose_degree(fapl, H5F_CLOSE_SEMI);
	if (H5Pset_driver(fapl, driver_id, nullptr) < 0) {
		H5Pclose(fapl);
		throw std::runtime_error("H5Pset_driver failed");
	}

	last_vfd_error.clear();
	hid_t file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
	H5Pclose(fapl);
	if (file < 0) {
		if (!last_vfd_error.empty()) {
			throw std::runtime_error(last_vfd_error);
		}
		// The driver opened the bytes fine but HDF5 could not parse them.
		throw std::runtime_error("File is not a valid HDF5 file: " + path);
	}
	return file;
}

} // namespace duckdb

#else

// Avoid an empty translation unit on non-wasm builds.
namespace duckdb {
void AnndataDuckdbFsVfdUnused() {
}
} // namespace duckdb

#endif // __EMSCRIPTEN__
