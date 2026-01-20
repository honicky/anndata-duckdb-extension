#include "h5fd_http.hpp"
#include <curl/curl.h>
#include <cstring>
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <list>
#include <unordered_map>
#include <openssl/hmac.h>
#include <openssl/sha.h>

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
// AWS SigV4 Signing Helpers
//===--------------------------------------------------------------------===//

// Convert bytes to lowercase hex string
static std::string ToHex(const unsigned char *data, size_t len) {
	std::ostringstream ss;
	ss << std::hex << std::setfill('0');
	for (size_t i = 0; i < len; ++i) {
		ss << std::setw(2) << static_cast<int>(data[i]);
	}
	return ss.str();
}

// SHA256 hash of a string, returned as lowercase hex
static std::string SHA256Hash(const std::string &data) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(data.c_str()), data.size(), hash);
	return ToHex(hash, SHA256_DIGEST_LENGTH);
}

// HMAC-SHA256, returns raw bytes
static std::vector<unsigned char> HMAC_SHA256(const std::vector<unsigned char> &key, const std::string &data) {
	std::vector<unsigned char> result(SHA256_DIGEST_LENGTH);
	unsigned int len = SHA256_DIGEST_LENGTH;
	HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()), reinterpret_cast<const unsigned char *>(data.c_str()),
	     data.size(), result.data(), &len);
	return result;
}

// HMAC-SHA256 with string key
static std::vector<unsigned char> HMAC_SHA256(const std::string &key, const std::string &data) {
	std::vector<unsigned char> key_bytes(key.begin(), key.end());
	return HMAC_SHA256(key_bytes, data);
}

// URL-encode a string (for canonical URI/query)
static std::string URIEncode(const std::string &str, bool encode_slash = true) {
	std::ostringstream encoded;
	encoded << std::hex << std::uppercase << std::setfill('0');
	for (unsigned char c : str) {
		if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-' ||
		    c == '~' || c == '.') {
			encoded << c;
		} else if (c == '/' && !encode_slash) {
			encoded << c;
		} else {
			encoded << '%' << std::setw(2) << static_cast<int>(c);
		}
	}
	return encoded.str();
}

// Get current UTC time in ISO8601 format (YYYYMMDD'T'HHMMSS'Z')
static std::string GetISO8601Time() {
	std::time_t now = std::time(nullptr);
	std::tm *utc = std::gmtime(&now);
	char buf[32];
	std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", utc);
	return buf;
}

// Get current UTC date (YYYYMMDD)
static std::string GetDateStamp() {
	std::time_t now = std::time(nullptr);
	std::tm *utc = std::gmtime(&now);
	char buf[16];
	std::strftime(buf, sizeof(buf), "%Y%m%d", utc);
	return buf;
}

// Parse host and path from URL
struct ParsedUrl {
	std::string host;
	std::string path;
	std::string query;
};

static ParsedUrl ParseUrl(const std::string &url) {
	ParsedUrl result;

	// Find protocol end
	size_t proto_end = url.find("://");
	size_t host_start = (proto_end != std::string::npos) ? proto_end + 3 : 0;

	// Find path start
	size_t path_start = url.find('/', host_start);
	if (path_start == std::string::npos) {
		result.host = url.substr(host_start);
		result.path = "/";
	} else {
		result.host = url.substr(host_start, path_start - host_start);
		// Find query string
		size_t query_start = url.find('?', path_start);
		if (query_start != std::string::npos) {
			result.path = url.substr(path_start, query_start - path_start);
			result.query = url.substr(query_start + 1);
		} else {
			result.path = url.substr(path_start);
		}
	}

	if (result.path.empty()) {
		result.path = "/";
	}

	return result;
}

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
// LRU Block Cache
//===--------------------------------------------------------------------===//

class BlockCache {
public:
	static constexpr size_t DEFAULT_BLOCK_SIZE = 1024 * 1024; // 1MB blocks
	static constexpr size_t DEFAULT_MAX_BLOCKS = 64;          // 64MB max cache

	BlockCache(size_t block_size = DEFAULT_BLOCK_SIZE, size_t max_blocks = DEFAULT_MAX_BLOCKS)
	    : block_size_(block_size), max_blocks_(max_blocks), hits_(0), misses_(0) {
	}

	// Try to read from cache, returns true if fully satisfied from cache
	bool TryRead(void *buf, size_t offset, size_t size) {
		if (size == 0) {
			return true;
		}

		// Calculate which blocks we need
		size_t start_block = offset / block_size_;
		size_t end_block = (offset + size - 1) / block_size_;

		// Check if all blocks are in cache
		for (size_t block = start_block; block <= end_block; ++block) {
			if (blocks_.find(block) == blocks_.end()) {
				return false;
			}
		}

		// All blocks in cache - copy data to output buffer
		hits_++;
		uint8_t *out = static_cast<uint8_t *>(buf);
		size_t remaining = size;
		size_t current_offset = offset;

		for (size_t block = start_block; block <= end_block; ++block) {
			// Move block to front of LRU list
			TouchBlock(block);

			const auto &block_data = blocks_[block];
			size_t block_start = block * block_size_;
			size_t offset_in_block = current_offset - block_start;
			size_t bytes_from_block = std::min(remaining, block_data.size() - offset_in_block);

			memcpy(out, block_data.data() + offset_in_block, bytes_from_block);
			out += bytes_from_block;
			remaining -= bytes_from_block;
			current_offset += bytes_from_block;
		}

		return true;
	}

	// Store a block in the cache
	void StoreBlock(size_t block_num, std::vector<uint8_t> data) {
		// Evict if at capacity
		while (lru_list_.size() >= max_blocks_ && !lru_list_.empty()) {
			size_t evict_block = lru_list_.back();
			lru_list_.pop_back();
			lru_map_.erase(evict_block);
			blocks_.erase(evict_block);
		}

		// Store the block
		blocks_[block_num] = std::move(data);
		lru_list_.push_front(block_num);
		lru_map_[block_num] = lru_list_.begin();
	}

	// Get blocks that need to be fetched for a read
	std::vector<std::pair<size_t, size_t>> GetMissingRanges(size_t offset, size_t size, size_t file_size) {
		std::vector<std::pair<size_t, size_t>> ranges;

		size_t start_block = offset / block_size_;
		size_t end_block = (offset + size - 1) / block_size_;

		for (size_t block = start_block; block <= end_block; ++block) {
			if (blocks_.find(block) == blocks_.end()) {
				misses_++;
				size_t block_start = block * block_size_;
				size_t block_end = std::min(block_start + block_size_, file_size);
				ranges.emplace_back(block_start, block_end - block_start);
			}
		}

		return ranges;
	}

	size_t GetBlockSize() const {
		return block_size_;
	}

	// Cache statistics
	size_t GetHits() const {
		return hits_;
	}
	size_t GetMisses() const {
		return misses_;
	}

	void Clear() {
		blocks_.clear();
		lru_list_.clear();
		lru_map_.clear();
		hits_ = 0;
		misses_ = 0;
	}

private:
	void TouchBlock(size_t block_num) {
		auto it = lru_map_.find(block_num);
		if (it != lru_map_.end()) {
			lru_list_.erase(it->second);
			lru_list_.push_front(block_num);
			lru_map_[block_num] = lru_list_.begin();
		}
	}

	size_t block_size_;
	size_t max_blocks_;
	size_t hits_;
	size_t misses_;

	// Block storage: block_number -> data
	std::unordered_map<size_t, std::vector<uint8_t>> blocks_;

	// LRU tracking
	std::list<size_t> lru_list_;
	std::unordered_map<size_t, std::list<size_t>::iterator> lru_map_;
};

//===--------------------------------------------------------------------===//
// HTTP Client Implementation
//===--------------------------------------------------------------------===//

class HttpClient {
public:
	HttpClient()
	    : curl_(nullptr), file_size_(0), cache_(BlockCache::DEFAULT_BLOCK_SIZE, BlockCache::DEFAULT_MAX_BLOCKS) {
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

		// For S3 with credentials, we need to add auth headers to HEAD request too
		struct curl_slist *headers = nullptr;
		if (config_.s3_access_key[0] != '\0') {
			headers = CreateS3AuthHeaders(url_, "", "HEAD");
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
		}

		// Perform HEAD request to get file size
		CURLcode res = curl_easy_perform(curl_);

		if (headers) {
			curl_slist_free_all(headers);
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
		}

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

		// Check block cache first
		if (cache_.TryRead(buf, offset, size)) {
			return true;
		}

		// Get missing blocks and fetch them
		auto missing = cache_.GetMissingRanges(offset, size, file_size_);
		for (const auto &range : missing) {
			size_t block_offset = range.first;
			size_t block_size = range.second;
			size_t block_num = block_offset / cache_.GetBlockSize();

			std::vector<uint8_t> block_data(block_size);
			if (!FetchRangeIntoBuffer(block_data.data(), block_offset, block_size)) {
				return false;
			}
			block_data.resize(block_size); // Ensure correct size
			cache_.StoreBlock(block_num, std::move(block_data));
		}

		// Now read from cache (should always succeed)
		return cache_.TryRead(buf, offset, size);
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

		// Prefetch by reading the first N bytes, which populates the block cache
		size_t fetch_size = std::min(size, static_cast<size_t>(file_size_));
		std::vector<uint8_t> temp(fetch_size);
		return Read(temp.data(), 0, fetch_size);
	}

private:
	static size_t HeaderCallback(char *buffer, size_t size, size_t nitems, void *userdata) {
		auto *client = static_cast<HttpClient *>(userdata);
		size_t total = size * nitems;

		std::string header(buffer, total);

		// Parse Content-Length - only set file_size_ if not already set
		// (During range requests, Content-Length is the chunk size, not total file size)
		if (header.rfind("Content-Length:", 0) == 0 || header.rfind("content-length:", 0) == 0) {
			size_t pos = header.find(':');
			if (pos != std::string::npos && client->file_size_ == 0) {
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

	bool FetchRangeIntoBuffer(void *buf, size_t offset, size_t size) {
		std::vector<uint8_t> response_buffer;
		response_buffer.reserve(size);

		// Build range header value
		std::ostringstream range_stream;
		range_stream << "bytes=" << offset << "-" << (offset + size - 1);
		std::string range_header = range_stream.str();

		curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, WriteCallback);
		curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_buffer);

		// Add S3 authentication headers if needed
		struct curl_slist *headers = nullptr;
		if (config_.s3_access_key[0] != '\0') {
			// Use SigV4 signing - pass URL and range header
			headers = CreateS3AuthHeaders(url_, range_header);
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
		} else {
			// Anonymous access - just set range header directly
			std::ostringstream range_curl;
			range_curl << offset << "-" << (offset + size - 1);
			curl_easy_setopt(curl_, CURLOPT_RANGE, range_curl.str().c_str());
		}

		CURLcode res = curl_easy_perform(curl_);

		if (headers) {
			curl_slist_free_all(headers);
			curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, nullptr);
		} else {
			// Clear range for next request (only if we used CURLOPT_RANGE)
			curl_easy_setopt(curl_, CURLOPT_RANGE, nullptr);
		}

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

	struct curl_slist *CreateS3AuthHeaders(const std::string &https_url, const std::string &range_header,
	                                       const std::string &method = "GET") {
		// Full AWS SigV4 signing implementation
		struct curl_slist *headers = nullptr;

		// Parse the URL
		ParsedUrl parsed = ParseUrl(https_url);

		// Get timestamps
		std::string amz_date = GetISO8601Time();
		std::string date_stamp = GetDateStamp();

		// Determine region
		std::string region = config_.s3_region;
		if (region[0] == '\0') {
			region = "us-east-1";
		}

		// Build signed headers list and canonical headers
		// Headers must be sorted alphabetically by lowercase name
		std::string signed_headers;
		std::string canonical_headers;

		// Host header (always required)
		canonical_headers += "host:" + parsed.host + "\n";

		// Range header (if present)
		if (!range_header.empty()) {
			canonical_headers += "range:" + range_header + "\n";
		}

		// x-amz-content-sha256 (required for S3)
		std::string payload_hash = SHA256Hash(""); // Empty payload for GET
		canonical_headers += "x-amz-content-sha256:" + payload_hash + "\n";

		// x-amz-date (required)
		canonical_headers += "x-amz-date:" + amz_date + "\n";

		// x-amz-security-token (if using session credentials)
		std::string session_token = config_.s3_session_token;
		if (session_token[0] != '\0') {
			canonical_headers += "x-amz-security-token:" + std::string(session_token) + "\n";
		}

		// Build signed headers string (semicolon-separated, sorted)
		signed_headers = "host";
		if (!range_header.empty()) {
			signed_headers += ";range";
		}
		signed_headers += ";x-amz-content-sha256;x-amz-date";
		if (session_token[0] != '\0') {
			signed_headers += ";x-amz-security-token";
		}

		// Create canonical request
		// CanonicalRequest =
		//   HTTPRequestMethod + '\n' +
		//   CanonicalURI + '\n' +
		//   CanonicalQueryString + '\n' +
		//   CanonicalHeaders + '\n' +
		//   SignedHeaders + '\n' +
		//   HexEncode(Hash(RequestPayload))

		std::string canonical_uri = URIEncode(parsed.path, false);
		std::string canonical_query = parsed.query; // Already encoded in URL

		// Canonical request format requires empty line between headers and signed_headers
		// canonical_headers ends with \n, so we add one more \n for the empty line
		std::string canonical_request = method + "\n" + canonical_uri + "\n" + canonical_query + "\n" +
		                                canonical_headers + "\n" + signed_headers + "\n" + payload_hash;

		// Create string to sign
		// StringToSign =
		//   Algorithm + '\n' +
		//   RequestDateTime + '\n' +
		//   CredentialScope + '\n' +
		//   HexEncode(Hash(CanonicalRequest))

		std::string algorithm = "AWS4-HMAC-SHA256";
		std::string credential_scope = date_stamp + "/" + region + "/s3/aws4_request";
		std::string canonical_request_hash = SHA256Hash(canonical_request);

		std::string string_to_sign =
		    algorithm + "\n" + amz_date + "\n" + credential_scope + "\n" + canonical_request_hash;

		// Calculate signing key
		// DateKey = HMAC-SHA256("AWS4" + SecretKey, DateStamp)
		// DateRegionKey = HMAC-SHA256(DateKey, Region)
		// DateRegionServiceKey = HMAC-SHA256(DateRegionKey, Service)
		// SigningKey = HMAC-SHA256(DateRegionServiceKey, "aws4_request")

		std::string secret_key = config_.s3_secret_key;
		auto date_key = HMAC_SHA256("AWS4" + std::string(secret_key), date_stamp);
		auto date_region_key = HMAC_SHA256(date_key, region);
		auto date_region_service_key = HMAC_SHA256(date_region_key, "s3");
		auto signing_key = HMAC_SHA256(date_region_service_key, "aws4_request");

		// Calculate signature
		auto signature_bytes = HMAC_SHA256(signing_key, string_to_sign);
		std::string signature = ToHex(signature_bytes.data(), signature_bytes.size());

		// Build Authorization header
		std::string access_key = config_.s3_access_key;
		std::string authorization = algorithm + " Credential=" + std::string(access_key) + "/" + credential_scope +
		                            ", SignedHeaders=" + signed_headers + ", Signature=" + signature;

		// Add all headers to curl list
		// First, disable headers that CURL might add automatically
		headers = curl_slist_append(headers, "Accept:"); // Disable Accept header
		headers = curl_slist_append(headers, "Expect:"); // Disable Expect header

		headers = curl_slist_append(headers, ("Host: " + parsed.host).c_str());
		if (!range_header.empty()) {
			headers = curl_slist_append(headers, ("Range: " + range_header).c_str());
		}
		headers = curl_slist_append(headers, ("x-amz-content-sha256: " + payload_hash).c_str());
		headers = curl_slist_append(headers, ("x-amz-date: " + amz_date).c_str());
		if (session_token[0] != '\0') {
			headers = curl_slist_append(headers, ("x-amz-security-token: " + std::string(session_token)).c_str());
		}
		headers = curl_slist_append(headers, ("Authorization: " + authorization).c_str());

		return headers;
	}

	CURL *curl_;
	std::string url_;
	H5FD_http_fapl_t config_;
	uint64_t file_size_;
	bool supports_range_ = false;

	// LRU block cache
	BlockCache cache_;
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

		// Check if endpoint is AWS default (treat as no custom endpoint)
		bool is_aws_default_endpoint = (endpoint[0] == '\0') || (std::string(endpoint) == "s3.amazonaws.com") ||
		                               (std::string(endpoint).find(".amazonaws.com") != std::string::npos &&
		                                std::string(endpoint).find(bucket) == std::string::npos);

		if (is_aws_default_endpoint) {
			// AWS S3 - use virtual-hosted style URL
			// s3://bucket/key -> https://bucket.s3.region.amazonaws.com/key
			std::string region = config.s3_region;
			if (region[0] == '\0') {
				region = "us-east-1";
			}
			url = protocol + "://" + bucket + ".s3." + region + ".amazonaws.com/" + key;
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
