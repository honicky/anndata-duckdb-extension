#include "h5fd_http.hpp"
#include <curl/curl.h>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <ctime>

// C++11 compatibility: make_unique not available until C++14
#if __cplusplus < 201402L
namespace std {
template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&...args) {
	return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
} // namespace std
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// URL Scheme Detection
//===--------------------------------------------------------------------===//

RemoteScheme DetectScheme(const std::string &path) {
	if (path.rfind("s3://", 0) == 0 || path.rfind("s3a://", 0) == 0) {
		return RemoteScheme::S3;
	} else if (path.rfind("https://", 0) == 0) {
		return RemoteScheme::HTTPS;
	} else if (path.rfind("http://", 0) == 0) {
		return RemoteScheme::HTTP;
	} else if (path.rfind("gs://", 0) == 0) {
		return RemoteScheme::GCS;
	}
	return RemoteScheme::LOCAL;
}

RemoteFileConfig ParseRemoteUrl(const std::string &path) {
	RemoteFileConfig config;
	config.scheme = DetectScheme(path);
	config.url = path;
	return config;
}

//===--------------------------------------------------------------------===//
// HTTP Client Implementation
//===--------------------------------------------------------------------===//

class HttpClient {
public:
	HttpClient() : curl_(nullptr), file_size_(0) {
		curl_ = curl_easy_init();
		if (!curl_) {
			throw std::runtime_error("Failed to initialize CURL");
		}
	}

	~HttpClient() {
		if (curl_) {
			curl_easy_cleanup(curl_);
		}
	}

	// Open a URL and get file size
	bool Open(const std::string &url, const H5FD_http_fapl_t *config) {
		url_ = url;
		if (config) {
			config_ = *config;
		}

		// Set up CURL options
		curl_easy_setopt(curl_, CURLOPT_URL, url_.c_str());
		curl_easy_setopt(curl_, CURLOPT_NOBODY, 1L);
		curl_easy_setopt(curl_, CURLOPT_HEADERFUNCTION, HeaderCallback);
		curl_easy_setopt(curl_, CURLOPT_HEADERDATA, this);
		curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
		curl_easy_setopt(curl_, CURLOPT_TIMEOUT, static_cast<long>(config_.timeout_seconds));

		// SSL options
		curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
		curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);

		// Perform HEAD request to get file size
		CURLcode res = curl_easy_perform(curl_);
		if (res != CURLE_OK) {
			return false;
		}

		long http_code = 0;
		curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);
		if (http_code >= 400) {
			return false;
		}

		// Reset for future GET requests
		curl_easy_setopt(curl_, CURLOPT_NOBODY, 0L);

		return true;
	}

	// Read bytes from the remote file
	bool Read(void *buf, size_t offset, size_t size) {
		if (size == 0) {
			return true;
		}

		// Check cache first
		if (TryReadFromCache(buf, offset, size)) {
			return true;
		}

		// Fetch from remote
		return FetchRange(buf, offset, size);
	}

	uint64_t GetFileSize() const {
		return file_size_;
	}

	bool SupportsRangeRequests() const {
		return supports_range_;
	}

	// Prefetch initial bytes into cache
	bool Prefetch(size_t size) {
		if (size == 0 || file_size_ == 0) {
			return true;
		}

		size_t fetch_size = std::min(size, static_cast<size_t>(file_size_));
		cache_.resize(fetch_size);
		cache_offset_ = 0;

		return FetchRangeIntoBuffer(cache_.data(), 0, fetch_size);
	}

private:
	static size_t HeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
		auto *client = static_cast<HttpClient *>(userdata);
		size_t total = size * nitems;

		std::string header(buffer, total);

		// Parse Content-Length
		if (header.rfind("Content-Length:", 0) == 0 || header.rfind("content-length:", 0) == 0) {
			size_t pos = header.find(':');
			if (pos != std::string::npos) {
				client->file_size_ = std::stoull(header.substr(pos + 1));
			}
		}

		// Check for Accept-Ranges
		if (header.rfind("Accept-Ranges:", 0) == 0 || header.rfind("accept-ranges:", 0) == 0) {
			if (header.find("bytes") != std::string::npos) {
				client->supports_range_ = true;
			}
		}

		return total;
	}

	static size_t WriteCallback(char *ptr, size_t size, size_t nmemb, void *userdata) {
		auto *buffer = static_cast<std::vector<uint8_t> *>(userdata);
		size_t total = size * nmemb;
		size_t old_size = buffer->size();
		buffer->resize(old_size + total);
		memcpy(buffer->data() + old_size, ptr, total);
		return total;
	}

	bool TryReadFromCache(void *buf, size_t offset, size_t size) {
		if (cache_.empty()) {
			return false;
		}

		// Check if the requested range is within our cache
		if (offset >= cache_offset_ && offset + size <= cache_offset_ + cache_.size()) {
			memcpy(buf, cache_.data() + (offset - cache_offset_), size);
			return true;
		}

		return false;
	}

	bool FetchRange(void *buf, size_t offset, size_t size) {
		// For small reads, fetch a larger chunk to benefit from caching
		size_t chunk_size = std::max(size, static_cast<size_t>(1024 * 1024)); // At least 1MB
		size_t fetch_offset = offset;
		size_t fetch_size = std::min(chunk_size, static_cast<size_t>(file_size_ - offset));

		std::vector<uint8_t> temp_buffer;
		temp_buffer.reserve(fetch_size);

		if (!FetchRangeIntoBuffer(temp_buffer.data(), fetch_offset, fetch_size)) {
			return false;
		}

		// Update cache
		cache_ = std::move(temp_buffer);
		cache_.resize(fetch_size);
		cache_offset_ = fetch_offset;

		// Copy requested portion to output buffer
		memcpy(buf, cache_.data() + (offset - cache_offset_), size);
		return true;
	}

	bool FetchRangeIntoBuffer(void *buf, size_t offset, size_t size) {
		std::vector<uint8_t> response_buffer;
		response_buffer.reserve(size);

		// Set range header
		std::ostringstream range;
		range << offset << "-" << (offset + size - 1);

		curl_easy_setopt(curl_, CURLOPT_RANGE, range.str().c_str());
		curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_buffer);

		// Add S3 authentication headers if needed
		struct curl_slist *headers = nullptr;
		if (IsS3Url() && config_.s3_access_key[0] != '\0') {
			headers = CreateS3AuthHeaders(offset, size);
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
		}

		CURLcode res = curl_easy_perform(curl_);

		if (headers) {
			curl_slist_free_all(headers);
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
		}

		// Clear range for next request
		curl_easy_setopt(curl_, CURLOPT_RANGE, nullptr);

		if (res != CURLE_OK) {
			return false;
		}

		long http_code = 0;
		curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &http_code);
		if (http_code != 206 && http_code != 200) {
			return false;
		}

		if (response_buffer.size() < size) {
			return false;
		}

		memcpy(buf, response_buffer.data(), size);
		return true;
	}

	bool IsS3Url() const {
		return url_.rfind("s3://", 0) == 0 || url_.rfind("s3a://", 0) == 0;
	}

	// Convert s3://bucket/key to https://bucket.s3.region.amazonaws.com/key
	std::string ConvertS3ToHttps() const {
		std::string result = url_;

		// Parse s3://bucket/key
		size_t start = (url_.rfind("s3://", 0) == 0) ? 5 : 6; // s3:// or s3a://
		size_t slash = url_.find('/', start);

		std::string bucket = url_.substr(start, slash - start);
		std::string key = (slash != std::string::npos) ? url_.substr(slash + 1) : "";

		// Build HTTPS URL
		std::string endpoint = config_.s3_endpoint;
		if (endpoint.empty()) {
			std::string region = config_.s3_region;
			if (region.empty()) {
				region = "us-east-1";
			}
			endpoint = bucket + ".s3." + region + ".amazonaws.com";
		}

		std::string protocol = config_.s3_use_ssl ? "https" : "http";
		return protocol + "://" + endpoint + "/" + key;
	}

	struct curl_slist *CreateS3AuthHeaders(size_t offset, size_t size) {
		// Simplified S3 signature - for production, use AWS SDK or full SigV4
		// This is a placeholder that will work with public buckets
		// For private buckets, implement full AWS Signature Version 4

		struct curl_slist *headers = nullptr;

		// Add host header
		std::string https_url = ConvertS3ToHttps();
		size_t host_start = https_url.find("://") + 3;
		size_t host_end = https_url.find('/', host_start);
		std::string host = https_url.substr(host_start, host_end - host_start);
		headers = curl_slist_append(headers, ("Host: " + host).c_str());

		// TODO: Implement full AWS SigV4 signing for private buckets
		// For now, this works with public buckets or buckets allowing anonymous access

		return headers;
	}

	CURL *curl_;
	std::string url_;
	H5FD_http_fapl_t config_;
	uint64_t file_size_;
	bool supports_range_ = false;

	// Simple cache
	std::vector<uint8_t> cache_;
	size_t cache_offset_ = 0;
};

//===--------------------------------------------------------------------===//
// HDF5 VFD Implementation
//===--------------------------------------------------------------------===//

// VFD file structure
struct H5FD_http_t {
	H5FD_t pub; // Public HDF5 VFD fields (must be first)
	std::unique_ptr<HttpClient> client;
	haddr_t eoa; // End of allocated address space
	H5FD_http_fapl_t config;
};

// Forward declarations of VFD callbacks
static H5FD_t *H5FD_http_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr);
static herr_t H5FD_http_close(H5FD_t *file);
static haddr_t H5FD_http_get_eoa(const H5FD_t *file, H5FD_mem_t type);
static herr_t H5FD_http_set_eoa(H5FD_t *file, H5FD_mem_t type, haddr_t addr);
static haddr_t H5FD_http_get_eof(const H5FD_t *file, H5FD_mem_t type);
static herr_t H5FD_http_read(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, void *buf);
static herr_t H5FD_http_write(H5FD_t *file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, const void *buf);

// Static VFD driver ID
static hid_t H5FD_HTTP_g = H5I_INVALID_HID;

// VFD class structure
static const H5FD_class_t H5FD_http_g = {
    H5FD_CLASS_VERSION,       // version
    (H5FD_class_value_t)600,  // value (custom VFD ID, must be >= 256)
    "http",                   // name
    HADDR_MAX,                // maxaddr
    H5F_CLOSE_WEAK,           // fc_degree
    nullptr,                  // terminate
    nullptr,                  // sb_size
    nullptr,                  // sb_encode
    nullptr,                  // sb_decode
    sizeof(H5FD_http_fapl_t), // fapl_size
    nullptr,                  // fapl_get
    nullptr,                  // fapl_copy
    nullptr,                  // fapl_free
    0,                        // dxpl_size
    nullptr,                  // dxpl_copy
    nullptr,                  // dxpl_free
    H5FD_http_open,           // open
    H5FD_http_close,          // close
    nullptr,                  // cmp
    nullptr,                  // query
    nullptr,                  // get_type_map
    nullptr,                  // alloc
    nullptr,                  // free
    H5FD_http_get_eoa,        // get_eoa
    H5FD_http_set_eoa,        // set_eoa
    H5FD_http_get_eof,        // get_eof
    nullptr,                  // get_handle
    H5FD_http_read,           // read
    H5FD_http_write,          // write
    nullptr,                  // read_vector
    nullptr,                  // write_vector
    nullptr,                  // read_selection
    nullptr,                  // write_selection
    nullptr,                  // flush
    nullptr,                  // truncate
    nullptr,                  // lock
    nullptr,                  // unlock
    nullptr,                  // del
    nullptr,                  // ctl
    H5FD_FLMAP_DICHOTOMY      // fl_map
};

//===--------------------------------------------------------------------===//
// VFD Callback Implementations
//===--------------------------------------------------------------------===//

static H5FD_t *H5FD_http_open(const char *name, unsigned flags, hid_t fapl_id, haddr_t maxaddr) {
	// Only support read-only access
	if (flags & H5F_ACC_RDWR) {
		return nullptr;
	}

	// Get FAPL configuration
	H5FD_http_fapl_t config = {};
	config.prefetch_size = 16 * 1024 * 1024; // 16MB default
	config.cache_size = 64 * 1024 * 1024;    // 64MB default
	config.timeout_seconds = 30;
	config.s3_use_ssl = true;

	if (fapl_id != H5P_DEFAULT) {
		const H5FD_http_fapl_t *fapl_config = nullptr;
		if (H5Pget_driver_info(fapl_id) != nullptr) {
			fapl_config = static_cast<const H5FD_http_fapl_t *>(H5Pget_driver_info(fapl_id));
			if (fapl_config) {
				config = *fapl_config;
			}
		}
	}

	// Create HTTP client
	auto client = std::make_unique<HttpClient>();

	// Determine URL to use
	std::string url = name;
	if (config.url[0] != '\0') {
		url = config.url;
	}

	// Convert S3 URLs to HTTP(S)
	if (url.rfind("s3://", 0) == 0 || url.rfind("s3a://", 0) == 0) {
		// Parse and convert S3 URL
		size_t start = (url.rfind("s3://", 0) == 0) ? 5 : 6;
		size_t slash = url.find('/', start);
		std::string bucket = url.substr(start, slash - start);
		std::string key = (slash != std::string::npos) ? url.substr(slash + 1) : "";

		std::string endpoint = config.s3_endpoint;
		std::string protocol = config.s3_use_ssl ? "https" : "http";

		if (endpoint[0] == '\0') {
			// No custom endpoint - use AWS virtual-hosted style URL
			// s3://bucket/key -> https://bucket.s3.region.amazonaws.com/key
			std::string region = config.s3_region;
			if (region[0] == '\0') {
				region = "us-east-1";
			}
			endpoint = bucket + ".s3." + region + ".amazonaws.com";
			url = protocol + "://" + endpoint + "/" + key;
		} else {
			// Custom endpoint (MinIO, etc.) - use path-style URL
			// s3://bucket/key -> http://endpoint/bucket/key
			url = protocol + "://" + endpoint + "/" + bucket + "/" + key;
		}
	}

	// Open the remote file
	if (!client->Open(url, &config)) {
		return nullptr;
	}

	// Check that server supports range requests
	if (!client->SupportsRangeRequests()) {
		// Try to proceed anyway - some servers don't advertise but support it
	}

	// Prefetch initial bytes
	if (!client->Prefetch(config.prefetch_size)) {
		return nullptr;
	}

	// Allocate VFD structure
	H5FD_http_t *file = new H5FD_http_t();
	memset(&file->pub, 0, sizeof(file->pub));
	file->client = std::move(client);
	file->eoa = 0;
	file->config = config;

	return reinterpret_cast<H5FD_t *>(file);
}

static herr_t H5FD_http_close(H5FD_t *_file) {
	H5FD_http_t *file = reinterpret_cast<H5FD_http_t *>(_file);
	delete file;
	return 0;
}

static haddr_t H5FD_http_get_eoa(const H5FD_t *_file, H5FD_mem_t type) {
	(void)type;
	const H5FD_http_t *file = reinterpret_cast<const H5FD_http_t *>(_file);
	return file->eoa;
}

static herr_t H5FD_http_set_eoa(H5FD_t *_file, H5FD_mem_t type, haddr_t addr) {
	(void)type;
	H5FD_http_t *file = reinterpret_cast<H5FD_http_t *>(_file);
	file->eoa = addr;
	return 0;
}

static haddr_t H5FD_http_get_eof(const H5FD_t *_file, H5FD_mem_t type) {
	(void)type;
	const H5FD_http_t *file = reinterpret_cast<const H5FD_http_t *>(_file);
	return static_cast<haddr_t>(file->client->GetFileSize());
}

static herr_t H5FD_http_read(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size, void *buf) {
	(void)type;
	(void)dxpl_id;

	H5FD_http_t *file = reinterpret_cast<H5FD_http_t *>(_file);

	if (!file->client->Read(buf, static_cast<size_t>(addr), size)) {
		return -1;
	}

	return 0;
}

static herr_t H5FD_http_write(H5FD_t *_file, H5FD_mem_t type, hid_t dxpl_id, haddr_t addr, size_t size,
                              const void *buf) {
	(void)_file;
	(void)type;
	(void)dxpl_id;
	(void)addr;
	(void)size;
	(void)buf;
	// Read-only VFD
	return -1;
}

//===--------------------------------------------------------------------===//
// Public API Implementation
//===--------------------------------------------------------------------===//

hid_t H5FD_http_init() {
	if (H5FD_HTTP_g == H5I_INVALID_HID) {
		// Initialize CURL globally
		curl_global_init(CURL_GLOBAL_DEFAULT);

		H5FD_HTTP_g = H5FDregister(&H5FD_http_g);
	}
	return H5FD_HTTP_g;
}

void H5FD_http_term() {
	if (H5FD_HTTP_g != H5I_INVALID_HID) {
		H5FDunregister(H5FD_HTTP_g);
		H5FD_HTTP_g = H5I_INVALID_HID;
		curl_global_cleanup();
	}
}

hid_t H5FD_http_get_driver_id() {
	return H5FD_HTTP_g;
}

herr_t H5Pset_fapl_http(hid_t fapl_id, const H5FD_http_fapl_t *config) {
	if (H5FD_HTTP_g == H5I_INVALID_HID) {
		H5FD_http_init();
	}

	if (H5FD_HTTP_g == H5I_INVALID_HID) {
		return -1;
	}

	return H5Pset_driver(fapl_id, H5FD_HTTP_g, config);
}

herr_t H5Pget_fapl_http(hid_t fapl_id, H5FD_http_fapl_t *config) {
	if (!config) {
		return -1;
	}

	const H5FD_http_fapl_t *driver_info = static_cast<const H5FD_http_fapl_t *>(H5Pget_driver_info(fapl_id));
	if (!driver_info) {
		return -1;
	}

	*config = *driver_info;
	return 0;
}

hid_t H5Fopen_remote(const std::string &path, const RemoteFileConfig &config) {
	// Create file access property list
	hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);
	if (fapl < 0) {
		return H5I_INVALID_HID;
	}

	// For local files, just open normally
	if (!config.IsRemote()) {
		H5Pset_fclose_degree(fapl, H5F_CLOSE_SEMI);
		hid_t file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
		H5Pclose(fapl);
		return file;
	}

	// Initialize HTTP VFD if needed
	hid_t driver_id = H5FD_http_init();
	if (driver_id == H5I_INVALID_HID) {
		H5Pclose(fapl);
		return H5I_INVALID_HID;
	}

	// Configure HTTP VFD
	H5FD_http_fapl_t http_config = {};
	strncpy(http_config.url, path.c_str(), sizeof(http_config.url) - 1);
	http_config.prefetch_size = config.prefetch_size;
	http_config.cache_size = config.cache_size;
	http_config.timeout_seconds = config.timeout_seconds;

	if (!config.s3_region.empty()) {
		strncpy(http_config.s3_region, config.s3_region.c_str(), sizeof(http_config.s3_region) - 1);
	}
	if (!config.s3_access_key.empty()) {
		strncpy(http_config.s3_access_key, config.s3_access_key.c_str(), sizeof(http_config.s3_access_key) - 1);
	}
	if (!config.s3_secret_key.empty()) {
		strncpy(http_config.s3_secret_key, config.s3_secret_key.c_str(), sizeof(http_config.s3_secret_key) - 1);
	}
	if (!config.s3_session_token.empty()) {
		strncpy(http_config.s3_session_token, config.s3_session_token.c_str(),
		        sizeof(http_config.s3_session_token) - 1);
	}
	if (!config.s3_endpoint.empty()) {
		strncpy(http_config.s3_endpoint, config.s3_endpoint.c_str(), sizeof(http_config.s3_endpoint) - 1);
	}
	http_config.s3_use_ssl = config.s3_use_ssl;

	// Set HTTP VFD on FAPL
	if (H5Pset_fapl_http(fapl, &http_config) < 0) {
		H5Pclose(fapl);
		return H5I_INVALID_HID;
	}

	H5Pset_fclose_degree(fapl, H5F_CLOSE_SEMI);

	// Open file - use URL as the "filename" for HTTP VFD
	hid_t file = H5Fopen(path.c_str(), H5F_ACC_RDONLY, fapl);
	H5Pclose(fapl);

	return file;
}

} // namespace duckdb
