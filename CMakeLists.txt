cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME anndata)

# Read version from VERSION file
file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" ANNDATA_VERSION)
string(STRIP "${ANNDATA_VERSION}" ANNDATA_VERSION)
message(STATUS "Building AnnData extension version: ${ANNDATA_VERSION}")

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME} VERSION ${ANNDATA_VERSION})

# Generate version header
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/src/include/anndata_version.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/src/include/anndata_version.hpp"
    @ONLY
)

include_directories(src/include)
include_directories(src/lzf)
include_directories(src/vfd)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/src/include)

# Option to use HDF5 C API instead of C++ API
option(USE_HDF5_C_API "Use HDF5 C API instead of C++ API (enables thread-safe mode)" OFF)

# Find HDF5 - use h5wasm for WASM builds, native HDF5 for others
if(EMSCRIPTEN)
    # For WASM builds, use prebuilt h5wasm libraries
    message(STATUS "Building for WASM - using h5wasm")
    include(FetchContent)
    FetchContent_Declare(
        libhdf5-wasm
        URL https://github.com/usnistgov/libhdf5-wasm/releases/download/v0.4.6_3.1.68/HDF5-1.14.6-Emscripten.tar.gz
    )
    FetchContent_MakeAvailable(libhdf5-wasm)
    # Set up HDF5 libraries and includes for WASM
    set(HDF5_LIBRARIES 
        ${libhdf5-wasm_SOURCE_DIR}/lib/libhdf5.a
    )
    set(HDF5_INCLUDE_DIRS ${libhdf5-wasm_SOURCE_DIR}/include)
    include_directories(${HDF5_INCLUDE_DIRS})
    message(STATUS "Using h5wasm libraries: ${HDF5_LIBRARIES}")
    message(STATUS "Using h5wasm includes: ${HDF5_INCLUDE_DIRS}")
elseif(CLANG_TIDY)
    # For tidy-check builds, skip HDF5 dependency
    message(STATUS "Clang-tidy build detected - skipping HDF5 dependency")
    # Define dummy variables to satisfy build
    set(HDF5_LIBRARIES "")
    set(HDF5_INCLUDE_DIRS "")
    # Add compile definitions to handle missing HDF5 in code
    add_compile_definitions(DUCKDB_NO_HDF5)
else()
    # For native builds, use system HDF5
    find_package(hdf5 CONFIG QUIET)
    if(hdf5_FOUND)
        message(STATUS "Found HDF5 via CONFIG mode (VCPKG)")
        # On Windows, use static HDF5 (threadsafe not available on Windows static builds)
        # On other platforms, use static for static builds
        if(WIN32)
            set(HDF5_LIBRARIES hdf5::hdf5-static)
            message(STATUS "Using static HDF5 on Windows (non-threadsafe, serialized via global mutex)")
        elseif(EXTENSION_STATIC_BUILD)
            set(HDF5_LIBRARIES hdf5::hdf5-static)
        else()
            set(HDF5_LIBRARIES hdf5::hdf5-shared)
        endif()
    else()
        # Fall back to traditional FindHDF5 for system installations
        message(STATUS "HDF5 CONFIG not found, trying MODULE mode")
        find_package(HDF5 REQUIRED COMPONENTS C CXX)
        if(HDF5_FOUND)
            include_directories(${HDF5_INCLUDE_DIRS})
            add_definitions(${HDF5_DEFINITIONS})
            message(STATUS "HDF5 found via MODULE mode: ${HDF5_VERSION}")
            message(STATUS "HDF5 libraries: ${HDF5_LIBRARIES}")
        else()
            message(FATAL_ERROR "HDF5 not found. Please install HDF5 or use VCPKG.")
        endif()
    endif()
endif()

# Find libcurl for remote file access (not available on WASM)
if(NOT EMSCRIPTEN AND NOT CLANG_TIDY)
    find_package(CURL CONFIG QUIET)
    if(CURL_FOUND)
        message(STATUS "Found CURL via CONFIG mode (VCPKG)")
        set(CURL_LIBRARIES CURL::libcurl)
    else()
        find_package(CURL REQUIRED)
        if(CURL_FOUND)
            message(STATUS "Found CURL via MODULE mode: ${CURL_VERSION_STRING}")
            include_directories(${CURL_INCLUDE_DIRS})
        endif()
    endif()
    # Also need OpenSSL for S3 signature
    find_package(OpenSSL QUIET)
    if(OPENSSL_FOUND)
        message(STATUS "Found OpenSSL: ${OPENSSL_VERSION}")
    endif()
else()
    set(CURL_LIBRARIES "")
    set(OPENSSL_LIBRARIES "")
endif()

# LZF filter sources for reading LZF-compressed HDF5 datasets
# Excluded from tidy builds since it's third-party code
if(NOT CLANG_TIDY)
    set(LZF_SOURCES
        src/lzf/lzf_filter.c
        src/lzf/lzf_c.c
        src/lzf/lzf_d.c
    )
else()
    set(LZF_SOURCES "")
endif()

# HTTP VFD sources for remote file access (S3, HTTP)
# Not available on WASM or tidy builds
if(NOT EMSCRIPTEN AND NOT CLANG_TIDY)
    set(VFD_SOURCES
        src/vfd/h5fd_http.cpp
    )
else()
    set(VFD_SOURCES "")
    add_compile_definitions(DUCKDB_NO_REMOTE_VFD)
endif()

set(EXTENSION_SOURCES
    src/anndata_extension.cpp
    src/anndata_scanner.cpp
    src/anndata_storage.cpp
    src/h5_reader_multithreaded.cpp
    ${LZF_SOURCES}
    ${VFD_SOURCES}
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link HDF5 libraries using modern CMake
target_link_libraries(${EXTENSION_NAME} ${HDF5_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${HDF5_LIBRARIES})

# Link CURL and OpenSSL for remote file access
if(NOT EMSCRIPTEN AND NOT CLANG_TIDY AND CURL_LIBRARIES)
    target_link_libraries(${EXTENSION_NAME} ${CURL_LIBRARIES})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${CURL_LIBRARIES})
    if(OPENSSL_FOUND)
        target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
        target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
    endif()
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
