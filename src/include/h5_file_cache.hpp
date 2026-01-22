#pragma once

#include <mutex>
#include <unordered_map>
#include <memory>
#include <string>
#include <cstring>
#include <list>
#include <fstream>
#include <sys/stat.h>

// Windows MSVC doesn't define S_ISDIR
#ifdef _MSC_VER
#ifndef S_ISDIR
#define S_ISDIR(m) (((m)&_S_IFMT) == _S_IFDIR)
#endif
#endif

#include "h5_handles.hpp"
#include <hdf5.h>

// Include our HTTP VFD for HTTP/HTTPS/S3 URLs
#ifndef DUCKDB_NO_REMOTE_VFD
#include "h5fd_http.hpp"
#endif

namespace duckdb {

// Global lock for HDF5 operations when library is not threadsafe.
// On threadsafe builds (Linux/macOS), Acquire() returns an empty lock (no overhead).
// On non-threadsafe builds (Windows), all HDF5 operations are serialized.
// Uses recursive_mutex to allow nested calls within the same thread.
class H5GlobalLock {
public:
	static std::recursive_mutex &GetMutex() {
		static std::recursive_mutex mu;
		return mu;
	}

	static bool IsThreadSafe() {
		static bool is_safe = CheckThreadSafe();
		return is_safe;
	}

	static std::unique_lock<std::recursive_mutex> Acquire() {
		if (IsThreadSafe()) {
			return std::unique_lock<std::recursive_mutex>(); // No lock needed
		}
		return std::unique_lock<std::recursive_mutex>(GetMutex());
	}

private:
	static bool CheckThreadSafe() {
		hbool_t is_threadsafe = false;
		H5is_library_threadsafe(&is_threadsafe);
		return is_threadsafe;
	}
};

struct H5FileDeleter {
	void operator()(hid_t *id) const {
		if (id && *id >= 0) {
			// Force closing of all open objects under this file
			auto lock = H5GlobalLock::Acquire();
			H5Fclose(*id);
		}
		delete id;
	}
};

class H5FileCache {
public:
	// Configuration for remote file access
	struct RemoteConfig {
		std::string s3_region;
		std::string s3_access_key;
		std::string s3_secret_key;
		std::string s3_session_token;
		std::string s3_endpoint;
		bool s3_use_ssl = true;
		size_t prefetch_size = 16 * 1024 * 1024; // 16MB
		size_t cache_size = 64 * 1024 * 1024;    // 64MB
		int timeout_seconds = 30;
	};

	static std::shared_ptr<hid_t> Open(const std::string &path, const RemoteConfig *remote_config = nullptr) {
		static H5FileCache &instance = Instance();
		std::lock_guard<std::mutex> guard(instance.mu_);

		// Check if we have a cached handle
		auto it = instance.map_.find(path);
		if (it != instance.map_.end()) {
			if (auto sp = it->second.lock()) {
				// Cached handle still alive - move to front of LRU
				instance.AddToLRU(sp);
				return sp;
			}
			// Weak pointer expired, remove from map
			instance.map_.erase(it);
		}

		// Create a new shared handle with proper settings
		// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
		auto h5_lock = H5GlobalLock::Acquire();

		hid_t file = H5I_INVALID_HID;

		// Detect URL scheme
		bool is_s3 = (path.rfind("s3://", 0) == 0 || path.rfind("s3a://", 0) == 0);
		bool is_http = (path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0);
		bool is_remote = is_s3 || is_http;

#ifndef DUCKDB_NO_REMOTE_VFD
		if (is_http || is_s3) {
			// Use our HTTP VFD for all remote URLs (HTTP/HTTPS/S3)
			// Includes block caching and SigV4 signing for S3
			RemoteFileConfig config = ParseRemoteUrl(path);
			if (remote_config) {
				config.s3_region = remote_config->s3_region;
				config.s3_access_key = remote_config->s3_access_key;
				config.s3_secret_key = remote_config->s3_secret_key;
				config.s3_session_token = remote_config->s3_session_token;
				config.s3_endpoint = remote_config->s3_endpoint;
				config.s3_use_ssl = remote_config->s3_use_ssl;
				config.prefetch_size = remote_config->prefetch_size;
				config.cache_size = remote_config->cache_size;
				config.timeout_seconds = remote_config->timeout_seconds;
			}
			file = H5Fopen_remote(path, config);
		} else
#endif
		{
			// Local file - check existence and permissions before HDF5 operations
			struct stat file_stat;
			if (stat(path.c_str(), &file_stat) != 0) {
				// File doesn't exist or can't be accessed
				throw std::runtime_error("File not found: " + path);
			}

			// Check if it's a directory
			if (S_ISDIR(file_stat.st_mode)) {
				throw std::runtime_error("Path is a directory, not a file: " + path);
			}

			// Check if we can read the file
			std::ifstream test_file(path);
			if (!test_file.good()) {
				throw std::runtime_error("Cannot read file (permission denied?): " + path);
			}
			test_file.close();

			// Local file - use standard POSIX VFD
			hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
			if (fapl < 0) {
				throw std::runtime_error("H5Pcreate failed");
			}

			// Use SEMI close degree - wait for objects but don't force close
			// This is safer than STRONG when multiple handles may exist
			H5Pset_fclose_degree(fapl, H5F_CLOSE_SEMI);

			// Open file in read-only mode
			file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
			H5Pclose(fapl);
		}

		if (file < 0) {
			// Provide more specific error message based on context
			if (is_remote) {
#ifndef DUCKDB_NO_REMOTE_VFD
				// Check if we have a specific HTTP error
				std::string http_error = GetLastHttpErrorMessage();
				if (!http_error.empty()) {
					throw std::runtime_error(http_error);
				}
#endif
				throw std::runtime_error("Failed to open remote file (check URL and credentials): " + path);
			} else {
				throw std::runtime_error("File is not a valid HDF5 file: " + path);
			}
		}

		// Create shared pointer with custom deleter
		std::shared_ptr<hid_t> handle(new hid_t(file), H5FileDeleter {});

		// Store weak pointer in cache
		instance.map_[path] = handle;

		// Keep handle alive in LRU cache
		instance.AddToLRU(handle);

		return handle;
	}

	// Clear the cache (useful for testing)
	static void Clear() {
		static H5FileCache &instance = Instance();
		std::lock_guard<std::mutex> guard(instance.mu_);
		instance.lru_list_.clear();
		instance.map_.clear();
	}

private:
	static H5FileCache &Instance() {
		static H5FileCache instance;
		return instance;
	}

	H5FileCache() = default;
	~H5FileCache() = default;

	// Delete copy/move operations
	H5FileCache(const H5FileCache &) = delete;
	H5FileCache &operator=(const H5FileCache &) = delete;
	H5FileCache(H5FileCache &&) = delete;
	H5FileCache &operator=(H5FileCache &&) = delete;

	std::mutex mu_;
	std::unordered_map<std::string, std::weak_ptr<hid_t>> map_;

	// LRU cache to keep recently-used handles alive (prevents re-opening)
	static constexpr size_t MAX_CACHED_HANDLES = 8;
	std::list<std::shared_ptr<hid_t>> lru_list_;

	void AddToLRU(std::shared_ptr<hid_t> handle) {
		// Add to front of LRU list
		lru_list_.push_front(handle);
		// Remove oldest if over limit
		while (lru_list_.size() > MAX_CACHED_HANDLES) {
			lru_list_.pop_back();
		}
	}
};

} // namespace duckdb
