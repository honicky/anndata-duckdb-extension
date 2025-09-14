cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME anndata)

# Find HDF5 using CONFIG mode for VCPKG or fall back to MODULE mode
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

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES 
    src/anndata_extension.cpp
    src/anndata_scanner.cpp
    src/h5_reader.cpp
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
