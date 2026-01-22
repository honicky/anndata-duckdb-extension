#pragma once

#include <hdf5.h>
#include <string>
#include <vector>
#include <memory>
#include <mutex>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Remote File Configuration
//===--------------------------------------------------------------------===//

enum class RemoteScheme {
	LOCAL, // No scheme or file://
	S3,    // s3:// or s3a://
	HTTPS, // https://
	HTTP,  // http://
	GCS    // gs://
};

struct RemoteFileConfig {
	RemoteScheme scheme = RemoteScheme::LOCAL;
	std::string url;

	// S3 configuration
	std::string s3_region;
	std::string s3_access_key;
	std::string s3_secret_key;
	std::string s3_session_token;
	std::string s3_endpoint;
	bool s3_use_ssl = true;

	// HTTP configuration
	size_t prefetch_size = 16 * 1024 * 1024; // 16MB default prefetch
	size_t cache_size = 64 * 1024 * 1024;    // 64MB default cache
	int timeout_seconds = 30;

	// Check if this is a remote URL
	bool IsRemote() const {
		return scheme != RemoteScheme::LOCAL;
	}
};

// Detect URL scheme from path
RemoteScheme DetectScheme(const std::string &path);

// Parse URL and create configuration
RemoteFileConfig ParseRemoteUrl(const std::string &path);

//===--------------------------------------------------------------------===//
// HTTP VFD Public API
//===--------------------------------------------------------------------===//

// Initialize the HTTP VFD (call once at extension load)
// Returns the VFD driver ID, or H5I_INVALID_HID on failure
hid_t H5FD_http_init();

// Terminate the HTTP VFD (call at extension unload)
void H5FD_http_term();

// Get the driver ID (returns H5I_INVALID_HID if not initialized)
hid_t H5FD_http_get_driver_id();

// Configuration structure for HTTP VFD FAPL
struct H5FD_http_fapl_t {
	char url[4096];       // Remote URL
	size_t prefetch_size; // Initial prefetch size
	size_t cache_size;    // Block cache size
	int timeout_seconds;  // Request timeout

	// S3-specific (for s3:// URLs)
	char s3_region[64];
	char s3_access_key[256];
	char s3_secret_key[256];
	char s3_session_token[1024];
	char s3_endpoint[256];
	bool s3_use_ssl;
};

// Set HTTP VFD on a file access property list
herr_t H5Pset_fapl_http(hid_t fapl_id, const H5FD_http_fapl_t *config);

// Get HTTP VFD configuration from a file access property list
herr_t H5Pget_fapl_http(hid_t fapl_id, H5FD_http_fapl_t *config);

//===--------------------------------------------------------------------===//
// HTTP Error Handling
//===--------------------------------------------------------------------===//

// Get the last HTTP error code from remote file operations (returns 0 if no error)
long GetLastHttpErrorCode();

// Get the last HTTP error as a user-friendly message (returns empty string if no error)
std::string GetLastHttpErrorMessage();

// Clear the last HTTP error
void ClearLastHttpError();

// Get user-friendly error message for an HTTP status code
std::string GetHttpErrorMessage(long http_code, const std::string &url);

//===--------------------------------------------------------------------===//
// Helper to open file with appropriate VFD
//===--------------------------------------------------------------------===//

// Open an HDF5 file, automatically selecting VFD based on URL scheme
// Returns file handle, or H5I_INVALID_HID on failure
// On failure, call GetLastHttpErrorMessage() to get details about HTTP errors
hid_t H5Fopen_remote(const std::string &path, const RemoteFileConfig &config);

} // namespace duckdb
