# Remote File Access Specification (S3 and HTTP)

## Overview

This specification describes adding support for attaching AnnData files from remote locations (S3 and HTTP/HTTPS) to the AnnData DuckDB extension. Remote files are read incrementally using HTTP range requests - **no full file download is required**.

## Motivation

Single-cell genomics datasets are increasingly stored in cloud object storage (S3, GCS, Azure Blob) and served via HTTP from data portals. Currently, users must:

1. Manually download large `.h5ad` files to local disk
2. Manage local storage for potentially many large files
3. Keep track of which local files correspond to which remote sources

With remote file support, users can directly query remote files with streaming reads:

```sql
-- S3 (streaming via ROS3 VFD)
ATTACH 's3://cellxgene-data/pbmc3k.h5ad' AS pbmc (TYPE ANNDATA);

-- HTTP/HTTPS (streaming via custom HTTP VFD)
ATTACH 'https://datasets.cellxgene.cziscience.com/pbmc3k.h5ad' AS pbmc (TYPE ANNDATA);

-- Query immediately - only fetches data actually needed
SELECT * FROM pbmc.obs WHERE cell_type = 'T cell';
```

## Design Approach: HDF5 Virtual File Drivers (VFDs)

HDF5 supports **Virtual File Drivers (VFDs)** - low-level plugins that replace standard POSIX I/O with custom storage backends. Each read operation is intercepted and can be translated to HTTP range requests.

### How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│  HDF5 Library                                                       │
│  ┌─────────────────┐                                                │
│  │ H5Dread()       │  ← User requests data                          │
│  └────────┬────────┘                                                │
│           │                                                         │
│  ┌────────▼────────┐                                                │
│  │ VFD Layer       │  ← Intercepts I/O operations                   │
│  │ read(offset, n) │                                                │
│  └────────┬────────┘                                                │
│           │                                                         │
└───────────┼─────────────────────────────────────────────────────────┘
            │
┌───────────▼─────────────────────────────────────────────────────────┐
│  ROS3 VFD (S3)         │    HTTP VFD (custom)                       │
│  ──────────────────────┼────────────────────────                    │
│  S3 Range GET          │    HTTP Range GET                          │
│  GET bucket/key        │    GET https://host/path                   │
│  Range: bytes=X-Y      │    Range: bytes=X-Y                        │
└─────────────────────────────────────────────────────────────────────┘
```

### VFD Required Callbacks

A VFD implements these core operations:

| Callback | Purpose |
|----------|---------|
| `open()` | Establish connection, get file size |
| `close()` | Clean up resources |
| `read()` | Fetch byte range from remote |
| `get_eof()` | Return total file size |
| `get_eoa()` / `set_eoa()` | Manage address space |
| `query()` | Report VFD capabilities |

## S3 Support: ROS3 VFD (Built-in)

HDF5 includes a built-in **ROS3 (Read-Only S3) VFD** that provides streaming reads from S3.

### Features

- Each `read()` call translates to an S3 Range GET request
- Pre-caches first 16MB on file open (HDF5 1.14.3+)
- Supports AWS credentials via environment, config file, or explicit parameters
- Works with any S3-compatible service (MinIO, GCS, etc.)

### Requirements

- HDF5 built with `--enable-ros3-vfd` (requires libcurl, openssl)
- VCPKG: Need to verify/enable ROS3 feature in HDF5 package

### Usage

```sql
-- Environment credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
ATTACH 's3://bucket/file.h5ad' AS data (TYPE ANNDATA);

-- Explicit credentials
SET anndata_s3_region = 'us-west-2';
SET anndata_s3_access_key = '...';
SET anndata_s3_secret_key = '...';
ATTACH 's3://bucket/file.h5ad' AS data (TYPE ANNDATA);

-- Custom endpoint (MinIO, GCS)
SET anndata_s3_endpoint = 'minio.example.com:9000';
SET anndata_s3_use_ssl = false;
ATTACH 's3://bucket/file.h5ad' AS data (TYPE ANNDATA);
```

## HTTP/HTTPS Support: Custom HTTP VFD

For HTTP/HTTPS URLs, we implement a custom VFD that uses HTTP Range requests.

### Design

The HTTP VFD follows the ROS3 pattern but uses generic HTTP instead of S3-specific APIs:

```cpp
// H5FD_http_t - VFD state structure
struct H5FD_http_t {
    H5FD_t pub;              // Common VFD fields (required)

    // HTTP connection state
    CURL *curl_handle;       // libcurl easy handle
    std::string url;         // Remote file URL
    uint64_t file_size;      // Total file size (from HEAD request)

    // Address tracking
    haddr_t eoa;             // End of allocated address space

    // Read cache (optional optimization)
    std::vector<uint8_t> cache;
    uint64_t cache_offset;
    size_t cache_size;
};
```

### Key Operations

#### `H5FD_http_open()`

```cpp
// 1. Initialize CURL handle
// 2. Send HEAD request to get Content-Length
// 3. Verify server supports range requests (Accept-Ranges: bytes)
// 4. Optionally pre-cache first N bytes
// 5. Return VFD handle
```

#### `H5FD_http_read()`

```cpp
// 1. Check if request is within cache
// 2. If not, send HTTP GET with Range header:
//    Range: bytes=offset-offset+size-1
// 3. Copy response data to buffer
// 4. Update cache if caching enabled
```

### Caching Strategy

To minimize HTTP round-trips:

1. **Metadata Cache**: Pre-fetch first 16MB on open (like ROS3)
   - HDF5 metadata (superblock, B-trees) typically at file start
   - Reduces latency for initial table discovery

2. **Read-Ahead Cache**: Optionally cache larger chunks
   - When reading at offset X, fetch X to X+chunk_size
   - Helps with sequential scan patterns

3. **LRU Block Cache**: For random access patterns
   - Cache recent blocks (e.g., 1MB blocks)
   - Evict least-recently-used when cache full

### Configuration

```sql
-- Enable/disable HTTP VFD (default: enabled)
SET anndata_http_vfd_enabled = true;

-- Pre-cache size on file open (default: 16MB)
SET anndata_http_prefetch_size = '16MB';

-- Read-ahead chunk size (default: 1MB)
SET anndata_http_chunk_size = '1MB';

-- Block cache size (default: 64MB)
SET anndata_http_cache_size = '64MB';

-- Request timeout (default: 30s)
SET anndata_http_timeout = 30;
```

## Architecture

### Integration with Existing Code

```
┌─────────────────────────────────────────────────────────────────────┐
│  ATTACH 's3://...' or 'https://...' AS ad (TYPE ANNDATA)           │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  AnndataStorageAttach (anndata_storage.cpp)                  │   │
│  │  - Parse URL scheme (s3://, https://, file://, local)        │   │
│  │  - Configure appropriate VFD via file access property list   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  H5FileCache::OpenWithVFD(path, vfd_config)                  │   │
│  │  - Create file access property list (FAPL)                    │   │
│  │  - Set VFD on FAPL (ros3, http, or default sec2)             │   │
│  │  - Call H5Fopen(path, flags, fapl)                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  HDF5 Library with VFD                                       │   │
│  │  - VFD intercepts all I/O                                    │   │
│  │  - Translates to HTTP range requests                         │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### URL Scheme Handling

```cpp
enum class RemoteScheme {
    LOCAL,      // No scheme or file://
    S3,         // s3:// or s3a://
    HTTPS,      // https://
    HTTP,       // http://
    GCS         // gs:// (via S3 compatibility or download)
};

RemoteScheme DetectScheme(const string &path) {
    if (path.starts_with("s3://") || path.starts_with("s3a://")) {
        return RemoteScheme::S3;
    } else if (path.starts_with("https://")) {
        return RemoteScheme::HTTPS;
    } else if (path.starts_with("http://")) {
        return RemoteScheme::HTTP;
    } else if (path.starts_with("gs://")) {
        return RemoteScheme::GCS;
    } else {
        return RemoteScheme::LOCAL;
    }
}
```

### VFD Selection Logic

```cpp
hid_t CreateFAPL(const string &path, ClientContext &context) {
    hid_t fapl = H5Pcreate(H5P_FILE_ACCESS);

    switch (DetectScheme(path)) {
    case RemoteScheme::S3:
    case RemoteScheme::S3A:
        // Use built-in ROS3 VFD
        H5FD_ros3_fapl_t ros3_config;
        // ... configure from context settings ...
        H5Pset_fapl_ros3(fapl, &ros3_config);
        break;

    case RemoteScheme::HTTPS:
    case RemoteScheme::HTTP:
        // Use custom HTTP VFD
        H5FD_http_fapl_t http_config;
        // ... configure from context settings ...
        H5Pset_fapl_http(fapl, &http_config);
        break;

    case RemoteScheme::LOCAL:
    default:
        // Use default SEC2 VFD (standard POSIX I/O)
        break;
    }

    return fapl;
}
```

## SQL Interface

### Basic Usage

```sql
-- S3 with default credentials
ATTACH 's3://bucket/file.h5ad' AS data (TYPE ANNDATA);

-- HTTPS
ATTACH 'https://example.com/data.h5ad' AS data (TYPE ANNDATA);

-- HTTP (for local/trusted servers)
ATTACH 'http://localhost:8080/data.h5ad' AS data (TYPE ANNDATA);

-- Query (only fetches needed data)
SELECT COUNT(*) FROM data.obs;
SELECT * FROM data.var WHERE gene_name LIKE 'CD%';
```

### S3 Configuration

```sql
-- AWS credentials
SET anndata_s3_region = 'us-west-2';
SET anndata_s3_access_key = 'AKIAIOSFODNN7EXAMPLE';
SET anndata_s3_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';

-- IAM role / credential chain (on EC2/ECS/Lambda)
SET anndata_s3_use_credential_chain = true;

-- Custom S3 endpoint (MinIO, etc.)
SET anndata_s3_endpoint = 'minio.example.com:9000';
SET anndata_s3_use_ssl = false;

-- Session token (for temporary credentials)
SET anndata_s3_session_token = '...';
```

### HTTP Configuration

```sql
-- Cache settings
SET anndata_http_prefetch_size = '32MB';
SET anndata_http_cache_size = '128MB';

-- Timeout
SET anndata_http_timeout = 60;

-- Custom headers (e.g., for authenticated APIs)
-- Future enhancement
```

### ATTACH Options

```sql
ATTACH 's3://bucket/file.h5ad' AS data (
    TYPE ANNDATA,
    -- Override global S3 settings for this file
    S3_REGION = 'eu-west-1',
    S3_ENDPOINT = 'custom.s3.endpoint.com'
);

ATTACH 'https://example.com/data.h5ad' AS data (
    TYPE ANNDATA,
    -- HTTP-specific options
    TIMEOUT = 120,
    PREFETCH_SIZE = '64MB'
);
```

## Implementation Plan

### Phase 1: ROS3 VFD Integration (S3 Support)

1. **Enable ROS3 in HDF5 build**
   - Update vcpkg port or CMakeLists to enable ROS3 feature
   - Verify libcurl and openssl dependencies

2. **Implement URL scheme detection**
   - Add `DetectScheme()` function
   - Modify `AnndataStorageAttach()` to handle remote URLs

3. **Implement S3 FAPL configuration**
   - Add settings for S3 credentials
   - Create `H5Pset_fapl_ros3()` wrapper

4. **Modify H5FileCache**
   - Add `OpenWithFAPL()` method
   - Handle remote URLs as cache keys

5. **Testing**
   - Unit tests with MinIO container
   - Integration tests with real S3

### Phase 2: HTTP VFD Implementation

1. **Implement HTTP VFD core**
   - `H5FD_http_open()` - HEAD request, connection setup
   - `H5FD_http_read()` - Range GET requests
   - `H5FD_http_close()` - cleanup
   - `H5FD_http_get_eof()` - return file size

2. **Register VFD with HDF5**
   - `H5FDregister()` during extension initialization
   - Implement full `H5FD_class_t` structure

3. **Implement caching layer**
   - Metadata pre-fetch on open
   - Block cache for repeated reads

4. **Add HTTP FAPL configuration**
   - `H5Pset_fapl_http()` function
   - Settings integration

5. **Testing**
   - Unit tests with local HTTP server
   - Integration tests with public datasets

### Phase 3: Optimization & Polish

1. **Connection pooling**
   - Reuse CURL handles across reads
   - Connection keep-alive

2. **Parallel prefetch**
   - Fetch multiple chunks concurrently
   - Especially beneficial for sparse matrix scans

3. **Retry logic**
   - Automatic retry on transient failures
   - Exponential backoff

4. **Progress reporting**
   - Optional progress callbacks for large operations

## Files to Create/Modify

### New Files

```
src/
├── h5_vfd_http.cpp           # HTTP VFD implementation
├── h5_vfd_http.hpp           # HTTP VFD header
├── remote_file_config.cpp    # URL parsing, config management
└── include/
    ├── h5_vfd_http.hpp
    └── remote_file_config.hpp
```

### Modified Files

- `src/anndata_storage.cpp` - URL detection, VFD selection
- `src/include/h5_file_cache.hpp` - Add FAPL support
- `src/anndata_extension.cpp` - Register settings, VFD
- `CMakeLists.txt` - Link libcurl, enable ROS3

## Dependencies

### Required

- **libcurl**: HTTP client library (for HTTP VFD)
- **openssl**: For HTTPS support
- **HDF5 with ROS3**: Need to enable in build

### VCPKG Configuration

```json
{
  "name": "anndata",
  "dependencies": [
    { "name": "hdf5", "features": ["ros3"] },
    "curl",
    "openssl"
  ]
}
```

## Performance Considerations

### Latency vs Local Files

| Operation | Local | S3/HTTP (uncached) | S3/HTTP (cached) |
|-----------|-------|--------------------| -----------------|
| File open | ~1ms | ~100-500ms | ~100-500ms |
| Metadata read | ~1ms | ~50-200ms | ~1ms |
| Data chunk read | ~1ms | ~50-200ms | ~1ms (if cached) |

### Optimization Tips

1. **Pre-fetch on ATTACH**: Cache metadata upfront
2. **Chunk alignment**: Read HDF5 chunks, not arbitrary ranges
3. **Connection reuse**: Keep-alive for repeated requests
4. **Parallel I/O**: Fetch multiple chunks concurrently

### Cloud-Optimized HDF5

For best performance with remote files, HDF5 files should be:
- Chunked (enables partial reads)
- Compressed (reduces transfer size)
- Have metadata aggregated at file start

## Error Handling

```sql
-- Network error
ATTACH 'https://example.com/file.h5ad' AS data (TYPE ANNDATA);
-- Error: Failed to connect to remote file: Connection timeout

-- Server doesn't support range requests
ATTACH 'https://old-server.com/file.h5ad' AS data (TYPE ANNDATA);
-- Error: Server does not support range requests (Accept-Ranges header missing)

-- S3 authentication error
ATTACH 's3://private-bucket/file.h5ad' AS data (TYPE ANNDATA);
-- Error: S3 access denied (403). Check credentials.

-- File not found
ATTACH 'https://example.com/nonexistent.h5ad' AS data (TYPE ANNDATA);
-- Error: Remote file not found (HTTP 404)
```

## Security Considerations

1. **Credentials**: Never log or expose S3 credentials
2. **HTTPS**: Verify SSL certificates by default
3. **Redirects**: Warn on cross-domain redirects
4. **Timeouts**: Prevent hanging on slow/unresponsive servers

## Future Enhancements

### Phase 4: Additional Features

- **GCS native support**: Google Cloud Storage VFD
- **Azure Blob support**: Custom VFD for Azure
- **Presigned URLs**: Generate temporary access URLs
- **Credential providers**: Plugin system for custom auth

### Phase 5: Advanced Optimization

- **Intelligent prefetch**: Predict access patterns
- **Compression-aware caching**: Cache compressed chunks
- **CDN support**: CloudFront, CloudFlare optimization

## Local Testing Setup

### Testing S3 with MinIO

MinIO is an S3-compatible object storage server that runs locally.

#### 1. Start MinIO Server

```bash
# Using Docker
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Or install locally (macOS)
brew install minio/stable/minio
minio server ./data
```

#### 2. Create Test Bucket and Upload File

```bash
# Install MinIO client
brew install minio/stable/mc

# Configure alias
mc alias set local http://localhost:9000 minioadmin minioadmin

# Create bucket
mc mb local/test-bucket

# Upload test h5ad file
mc cp test/data/sample.h5ad local/test-bucket/
```

#### 3. Test in DuckDB

```sql
-- Configure for MinIO
SET anndata_s3_endpoint = 'localhost:9000';
SET anndata_s3_use_ssl = false;
SET anndata_s3_access_key = 'minioadmin';
SET anndata_s3_secret_key = 'minioadmin';

-- Attach and query
ATTACH 's3://test-bucket/sample.h5ad' AS data (TYPE ANNDATA);
SELECT COUNT(*) FROM data.obs;
DETACH data;
```

### Testing HTTP with Python HTTP Server

Python's built-in HTTP server supports range requests, making it ideal for local testing.

#### 1. Start HTTP Server with Range Support

```python
# test/http_server.py
import http.server
import socketserver
import os

class RangeHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler that supports Range requests."""

    def send_head(self):
        path = self.translate_path(self.path)
        if not os.path.isfile(path):
            self.send_error(404, "File not found")
            return None

        file_size = os.path.getsize(path)

        # Check for Range header
        range_header = self.headers.get('Range')
        if range_header:
            # Parse Range: bytes=start-end
            range_spec = range_header.replace('bytes=', '')
            start, end = range_spec.split('-')
            start = int(start) if start else 0
            end = int(end) if end else file_size - 1

            self.send_response(206)  # Partial Content
            self.send_header('Content-Range', f'bytes {start}-{end}/{file_size}')
            self.send_header('Content-Length', end - start + 1)
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Content-Type', 'application/octet-stream')
            self.end_headers()

            f = open(path, 'rb')
            f.seek(start)
            return (f, start, end - start + 1)
        else:
            self.send_response(200)
            self.send_header('Content-Length', file_size)
            self.send_header('Accept-Ranges', 'bytes')
            self.send_header('Content-Type', 'application/octet-stream')
            self.end_headers()
            return open(path, 'rb')

    def copyfile(self, source, outputfile):
        if isinstance(source, tuple):
            f, start, length = source
            outputfile.write(f.read(length))
            f.close()
        else:
            super().copyfile(source, outputfile)

if __name__ == '__main__':
    PORT = 8080
    with socketserver.TCPServer(("", PORT), RangeHTTPRequestHandler) as httpd:
        print(f"Serving at http://localhost:{PORT}")
        print("Place .h5ad files in current directory")
        httpd.serve_forever()
```

#### 2. Start Server and Test

```bash
# Start server in directory with test files
cd test/data
python ../http_server.py
```

#### 3. Test in DuckDB

```sql
-- Test HTTP access
ATTACH 'http://localhost:8080/sample.h5ad' AS data (TYPE ANNDATA);
SELECT COUNT(*) FROM data.obs;
DETACH data;
```

### Automated Test Script

```python
# test/test_remote_access.py
import subprocess
import time
import os
import signal

def test_minio():
    """Test S3 access via MinIO."""
    # Start MinIO container
    proc = subprocess.Popen([
        'docker', 'run', '-d', '--rm',
        '-p', '9000:9000',
        '-e', 'MINIO_ROOT_USER=test',
        '-e', 'MINIO_ROOT_PASSWORD=testtest',
        'minio/minio', 'server', '/data'
    ], stdout=subprocess.PIPE)
    container_id = proc.stdout.read().decode().strip()

    try:
        time.sleep(3)  # Wait for startup

        # Create bucket and upload file
        os.system('mc alias set test http://localhost:9000 test testtest')
        os.system('mc mb test/bucket')
        os.system('mc cp test/data/sample.h5ad test/bucket/')

        # Test with DuckDB
        result = subprocess.run([
            './build/release/duckdb', '-unsigned', '-c', '''
            LOAD 'build/release/extension/anndata/anndata.duckdb_extension';
            SET anndata_s3_endpoint = 'localhost:9000';
            SET anndata_s3_use_ssl = false;
            SET anndata_s3_access_key = 'test';
            SET anndata_s3_secret_key = 'testtest';
            ATTACH 's3://bucket/sample.h5ad' AS data (TYPE ANNDATA);
            SELECT COUNT(*) FROM data.obs;
            '''
        ], capture_output=True, text=True)

        assert 'Error' not in result.stderr, f"S3 test failed: {result.stderr}"
        print("S3 test passed!")

    finally:
        subprocess.run(['docker', 'stop', container_id])

def test_http():
    """Test HTTP access via local server."""
    # Start HTTP server
    server = subprocess.Popen(
        ['python', 'test/http_server.py'],
        cwd='test/data'
    )

    try:
        time.sleep(2)  # Wait for startup

        # Test with DuckDB
        result = subprocess.run([
            './build/release/duckdb', '-unsigned', '-c', '''
            LOAD 'build/release/extension/anndata/anndata.duckdb_extension';
            ATTACH 'http://localhost:8080/sample.h5ad' AS data (TYPE ANNDATA);
            SELECT COUNT(*) FROM data.obs;
            '''
        ], capture_output=True, text=True)

        assert 'Error' not in result.stderr, f"HTTP test failed: {result.stderr}"
        print("HTTP test passed!")

    finally:
        server.terminate()

if __name__ == '__main__':
    test_minio()
    test_http()
```

## Repository Structure Decision

### Option A: Separate Repository for HTTP VFD (Recommended)

Create `hdf5-http-vfd` as a standalone library that can be used by any HDF5-based project.

**Pros:**
- **Reusability**: Other projects (h5py, netCDF, xarray) could use it
- **Focused testing**: Easier to test VFD in isolation
- **Independent versioning**: VFD can evolve separately from AnnData extension
- **Community contribution**: Could be proposed to HDF Group for inclusion in HDF5
- **Cleaner architecture**: Separation of concerns

**Cons:**
- **Additional dependency**: Another library to manage
- **Coordination overhead**: Need to sync versions
- **Build complexity**: Cross-repo dependencies

**Suggested structure:**

```
github.com/yourorg/hdf5-http-vfd/
├── src/
│   ├── h5fd_http.c           # VFD implementation
│   ├── h5fd_http.h           # Public header
│   └── http_client.c         # libcurl wrapper
├── include/
│   └── H5FDhttp.h            # Public API
├── test/
│   ├── test_vfd.c            # Unit tests
│   └── test_server.py        # HTTP test server
├── CMakeLists.txt
├── vcpkg.json
└── README.md
```

**Usage from AnnData extension:**

```cmake
# CMakeLists.txt
find_package(hdf5-http-vfd CONFIG REQUIRED)
target_link_libraries(anndata_extension hdf5-http-vfd::hdf5-http-vfd)
```

### Option B: Embedded in AnnData Extension

Keep the HTTP VFD implementation inside this repository.

**Pros:**
- **Simpler initial development**: No cross-repo coordination
- **Single build**: Everything builds together
- **Tight integration**: Can optimize for AnnData-specific patterns

**Cons:**
- **Not reusable**: Other projects can't easily use it
- **Larger extension**: More code to maintain
- **Testing coupling**: VFD tests mixed with extension tests

**Suggested structure:**

```
anndata-duckdb-extension/
├── src/
│   ├── anndata_extension.cpp
│   ├── anndata_storage.cpp
│   ├── h5_vfd_http.cpp        # VFD implementation
│   └── include/
│       └── h5_vfd_http.hpp
└── ...
```

### Recommendation: Start Embedded, Extract Later

**Phase 1**: Implement HTTP VFD inside this repo
- Faster iteration during initial development
- Easier to debug integration issues
- Get working solution quickly

**Phase 2**: Extract to separate repo when stable
- Once API is stable, move to standalone library
- Create vcpkg port for easy distribution
- Consider proposing to HDF Group

This approach gives you:
1. Quick initial progress without coordination overhead
2. Flexibility to refactor architecture later
3. Option to contribute to broader HDF5 ecosystem

### Code Organization (Embedded Phase)

```
anndata-duckdb-extension/
├── src/
│   ├── anndata_extension.cpp
│   ├── anndata_storage.cpp     # Modified for URL handling
│   ├── h5_reader_multithreaded.cpp
│   ├── vfd/                    # VFD-specific code (easy to extract later)
│   │   ├── h5fd_http.cpp       # HTTP VFD implementation
│   │   ├── h5fd_http.hpp
│   │   ├── http_client.cpp     # libcurl wrapper
│   │   └── http_client.hpp
│   └── include/
│       └── ...
├── test/
│   ├── sql/
│   │   ├── remote_s3.test      # S3 tests
│   │   └── remote_http.test    # HTTP tests
│   └── remote/
│       ├── test_server.py      # HTTP test server
│       └── test_minio.py       # MinIO test setup
└── ...
```

The `src/vfd/` subdirectory isolates VFD code, making future extraction straightforward.

## References

- [HDF5 Virtual File Layer](https://support.hdfgroup.org/documentation/hdf5/latest/_v_f_l_t_n.html)
- [HDF5 ROS3 Driver](https://support.hdfgroup.org/documentation/hdf5-docs/registered_virtual_file_drivers_vfds.html)
- [Cloud Storage Options for HDF5](https://www.hdfgroup.org/2022/08/08/cloud-storage-options-for-hdf5/)
- [libcurl Range Requests](https://curl.se/libcurl/c/CURLOPT_RANGE.html)
- [DuckDB httpfs Extension](https://duckdb.org/docs/stable/core_extensions/httpfs/overview)
- [MinIO Quickstart](https://min.io/docs/minio/container/index.html)
