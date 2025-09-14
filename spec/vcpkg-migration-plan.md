# VCPKG Migration Plan for AnnData DuckDB Extension

## Executive Summary

This document outlines the migration from system package management (apt) to VCPKG for dependency management in the AnnData DuckDB extension. The goal is to align with DuckDB community extension standards for better portability, CI/CD integration, and cross-platform support.

## Current State

### Dependencies
- **HDF5 C++ Library**: Currently installed via apt in Docker
- **Build Environment**: Docker-based with Ubuntu base image
- **Platform Support**: Linux only (via Docker)

### Issues with Current Approach
1. Platform-specific (requires Docker for consistent builds)
2. Inconsistent with DuckDB extension ecosystem
3. Difficult to support macOS and Windows natively
4. CI/CD complications with Docker-in-Docker scenarios
5. Version inconsistencies across different environments

## Target State

### Goals
1. **Native builds** on all platforms (Linux, macOS, Windows)
2. **Consistent with DuckDB extension standards**
3. **Simplified CI/CD** using DuckDB's extension-ci-tools
4. **Reproducible builds** with version-locked dependencies
5. **Developer-friendly** setup process

## Migration Plan

### Phase 1: VCPKG Setup (Week 1)

#### 1.1 Create vcpkg.json
```json
{
  "$schema": "https://raw.githubusercontent.com/microsoft/vcpkg/master/scripts/vcpkg.schema.json",
  "name": "anndata",
  "version-string": "0.1.0",
  "description": "DuckDB extension for reading AnnData files",
  "homepage": "https://github.com/yourusername/anndata-duckdb",
  "dependencies": [
    {
      "name": "hdf5",
      "features": ["cpp"]
    }
  ],
  "builtin-baseline": "2024.01.12"
}
```

#### 1.2 Create vcpkg-configuration.json
```json
{
  "default-registry": {
    "kind": "git",
    "baseline": "2024.01.12",
    "repository": "https://github.com/microsoft/vcpkg.git"
  },
  "registries": []
}
```

#### 1.3 Update .gitignore
```
# VCPKG
vcpkg_installed/
*.vcpkg
```

### Phase 2: CMake Modernization (Week 1)

#### 2.1 Update CMakeLists.txt

**Remove old HDF5 finding:**
```cmake
# Remove these lines
find_package(HDF5 REQUIRED COMPONENTS C CXX)
if(HDF5_FOUND)
    include_directories(${HDF5_INCLUDE_DIRS})
    add_definitions(${HDF5_DEFINITIONS})
    # ...
endif()
```

**Add modern VCPKG approach:**
```cmake
# At the top of CMakeLists.txt
cmake_minimum_required(VERSION 3.21)

# Find HDF5 using CONFIG mode (VCPKG provides this)
find_package(hdf5 CONFIG REQUIRED)
find_package(ZLIB REQUIRED) # HDF5 dependency

# Modern target-based linking
target_link_libraries(${EXTENSION_NAME} 
    PRIVATE 
    hdf5::hdf5-shared 
    hdf5::hdf5_cpp-shared
)

target_link_libraries(${LOADABLE_EXTENSION_NAME} 
    PRIVATE 
    hdf5::hdf5-shared 
    hdf5::hdf5_cpp-shared
)

# Include directories are handled automatically by target_link_libraries
```

#### 2.2 Create extension_config.cmake updates
```cmake
# Ensure VCPKG toolchain is used
if(DEFINED ENV{VCPKG_TOOLCHAIN_PATH})
    set(CMAKE_TOOLCHAIN_FILE $ENV{VCPKG_TOOLCHAIN_PATH})
endif()

# Set VCPKG triplet based on platform
if(APPLE)
    set(VCPKG_TARGET_TRIPLET "arm64-osx" CACHE STRING "")
elseif(WIN32)
    set(VCPKG_TARGET_TRIPLET "x64-windows-static" CACHE STRING "")
else()
    set(VCPKG_TARGET_TRIPLET "x64-linux" CACHE STRING "")
endif()
```

### Phase 3: Build Environment Updates (Week 2)

#### 3.1 Developer Setup Script (setup-dev.sh)
```bash
#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}Setting up development environment for AnnData DuckDB extension${NC}"

# Check if VCPKG is installed
if [ -z "$VCPKG_ROOT" ]; then
    echo -e "${BLUE}Installing VCPKG...${NC}"
    git clone https://github.com/Microsoft/vcpkg.git ~/vcpkg
    ~/vcpkg/bootstrap-vcpkg.sh
    export VCPKG_ROOT=~/vcpkg
    echo 'export VCPKG_ROOT=~/vcpkg' >> ~/.bashrc
    echo 'export PATH=$VCPKG_ROOT:$PATH' >> ~/.bashrc
fi

# Install dependencies via VCPKG
echo -e "${BLUE}Installing dependencies...${NC}"
$VCPKG_ROOT/vcpkg install --triplet=$VCPKG_TARGET_TRIPLET

# Setup Python environment
echo -e "${BLUE}Setting up Python environment...${NC}"
uv venv
uv sync

echo -e "${GREEN}Setup complete! Run 'make' to build the extension${NC}"
```

#### 3.2 Simplified Makefile
```makefile
.PHONY: all clean debug release test

# Configuration
VCPKG_TOOLCHAIN_PATH ?= $(VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake
BUILD_TYPE ?= Release
GENERATOR ?= "Unix Makefiles"

all: release

release:
	mkdir -p build/release
	cd build/release && cmake -G $(GENERATOR) \
		-DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_TOOLCHAIN_PATH) \
		-DEXTENSION_STATIC_BUILD=1 \
		../..
	cmake --build build/release --config Release

debug:
	mkdir -p build/debug
	cd build/debug && cmake -G $(GENERATOR) \
		-DCMAKE_BUILD_TYPE=Debug \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_TOOLCHAIN_PATH) \
		-DEXTENSION_STATIC_BUILD=1 \
		../..
	cmake --build build/debug --config Debug

test: release
	cd build/release && ctest --output-on-failure

clean:
	rm -rf build

format:
	find src -name "*.cpp" -o -name "*.hpp" | xargs clang-format -i
```

### Phase 4: Docker Considerations (Week 2)

#### 4.1 Docker Strategy

**Option A: Eliminate Docker (Recommended)**
- **Pros:**
  - Simpler development workflow
  - Native performance
  - Better IDE integration
  - Consistent with DuckDB community practices
  - Easier debugging
  
- **Cons:**
  - Developers need to install VCPKG locally
  - Potential for "works on my machine" issues
  
**Option B: Docker for CI only**
- Keep Docker for CI/CD pipelines
- Developers use native builds locally
- Docker ensures consistent CI environment

**Option C: Optional Docker support**
- Provide Docker as an option for developers who prefer it
- Not the primary development path

#### 4.2 If keeping Docker (Option B/C), update Dockerfile:
```dockerfile
FROM ubuntu:22.04

# Install build essentials
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    python3 \
    python3-pip \
    ninja-build

# Install VCPKG
RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg && \
    /opt/vcpkg/bootstrap-vcpkg.sh && \
    /opt/vcpkg/vcpkg integrate install

ENV VCPKG_ROOT=/opt/vcpkg
ENV PATH="${VCPKG_ROOT}:${PATH}"
ENV VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /workspace

# Pre-install dependencies to cache them
COPY vcpkg.json vcpkg-configuration.json ./
RUN vcpkg install --triplet=x64-linux

# Copy source
COPY . .

# Build
RUN make release
```

### Phase 5: CI/CD Integration (Week 3)

#### 5.1 GitHub Actions Workflow
```yaml
name: Build and Test
on: [push, pull_request]

jobs:
  build:
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        duckdb_version: ['1.1.0', 'main']
    
    runs-on: ${{ matrix.os }}
    
    steps:
    - uses: actions/checkout@v3
      with:
        submodules: recursive
    
    - name: Setup VCPKG
      uses: lukka/run-vcpkg@v11
      with:
        vcpkgGitCommitId: '2024.01.12'
    
    - name: Build Extension
      run: |
        mkdir build
        cd build
        cmake .. -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake
        cmake --build . --config Release
    
    - name: Test Extension
      run: |
        cd build
        ctest --output-on-failure
```

#### 5.2 Use DuckDB Extension CI Tools
```yaml
name: DuckDB Extension CI
on: [push, pull_request]

jobs:
  duckdb-tests:
    uses: duckdb/extension-ci-tools/.github/workflows/test_extension.yml@main
    with:
      extension_name: anndata
      vcpkg_dependencies: true
```

### Phase 6: Documentation Updates (Week 3)

#### 6.1 Update README.md
```markdown
## Building from Source

### Prerequisites
- CMake 3.21+
- C++17 compatible compiler
- VCPKG (automatically installed by setup script)

### Quick Start
```bash
# Clone the repository
git clone https://github.com/yourusername/anndata-duckdb.git
cd anndata-duckdb

# Run setup (installs VCPKG and dependencies)
./setup-dev.sh

# Build
make

# Test
make test
```

### Platform-Specific Notes

#### macOS
- Ensure Xcode command line tools are installed: `xcode-select --install`
- For Apple Silicon: VCPKG will use `arm64-osx` triplet automatically

#### Windows
- Use Visual Studio 2022 or later
- Run commands in Developer Command Prompt
- Use `cmake --build . --config Release` instead of `make`

#### Linux
- Requires GCC 9+ or Clang 10+
- May need to install: `sudo apt-get install build-essential cmake git`
```

## Risk Mitigation

### Risk 1: HDF5 Build Issues with VCPKG
- **Mitigation**: Test HDF5 vcpkg port thoroughly
- **Fallback**: Create custom vcpkg port if needed

### Risk 2: Platform-specific Issues
- **Mitigation**: Test on all platforms early
- **Fallback**: Document platform limitations

### Risk 3: CI/CD Complexity
- **Mitigation**: Start with DuckDB's extension-ci-tools
- **Fallback**: Gradual migration, keep Docker CI initially

## Success Criteria

1. ✅ Extension builds natively on Linux, macOS, and Windows
2. ✅ CI/CD pipeline passes on all platforms
3. ✅ Build time < 5 minutes on standard hardware
4. ✅ Documentation updated and clear
5. ✅ No regression in functionality
6. ✅ Compatible with DuckDB extension ecosystem

## Decision: Docker vs Native

### Recommendation: **Eliminate Docker for Development**

**Rationale:**
1. **Industry Standard**: Most DuckDB extensions don't require Docker
2. **Developer Experience**: Native builds are faster and easier to debug
3. **VCPKG Benefits**: VCPKG provides the consistency Docker was providing
4. **Cross-platform**: VCPKG works on all platforms, Docker doesn't (well) on Windows
5. **CI/CD**: GitHub Actions and DuckDB's CI tools work better without Docker

**Docker should be:**
- **Removed** from primary development workflow
- **Optional** for developers who prefer containerized environments
- **Used** only if specific CI/CD requirements demand it

## Conclusion

Migrating to VCPKG aligns the AnnData extension with DuckDB community standards, improves cross-platform support, and simplifies the development workflow. The elimination of Docker as a requirement (making it optional) will improve developer experience while VCPKG provides the dependency management consistency we need.