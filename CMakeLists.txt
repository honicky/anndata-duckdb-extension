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
        ${libhdf5-wasm_SOURCE_DIR}/lib/libhdf5_cpp.a
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
        set(HDF5_LIBRARIES hdf5::hdf5-shared hdf5::hdf5_cpp-shared)
        # For static builds, use static libraries
        if(EXTENSION_STATIC_BUILD)
            set(HDF5_LIBRARIES hdf5::hdf5-static hdf5::hdf5_cpp-static)
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

set(EXTENSION_SOURCES 
    src/anndata_extension.cpp
    src/anndata_scanner.cpp
    src/h5_reader.cpp
    src/h5_reader_multithreaded.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link HDF5 libraries using modern CMake
target_link_libraries(${EXTENSION_NAME} ${HDF5_LIBRARIES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${HDF5_LIBRARIES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
