#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}AnnData DuckDB Extension Development Setup${NC}"
echo -e "${BLUE}========================================${NC}"

# Detect OS
OS="unknown"
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    OS="linux"
    VCPKG_TRIPLET="x64-linux"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    OS="macos"
    # Detect architecture
    if [[ $(uname -m) == "arm64" ]]; then
        VCPKG_TRIPLET="arm64-osx"
    else
        VCPKG_TRIPLET="x64-osx"
    fi
elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
    OS="windows"
    VCPKG_TRIPLET="x64-windows-static"
fi

echo -e "${GREEN}Detected OS: ${OS} (${VCPKG_TRIPLET})${NC}"

# Check prerequisites
echo -e "\n${BLUE}Checking prerequisites...${NC}"

# Check for CMake
if ! command -v cmake &> /dev/null; then
    echo -e "${RED}CMake is not installed. Please install CMake 3.5+ first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ CMake found: $(cmake --version | head -n1)${NC}"

# Check for Git
if ! command -v git &> /dev/null; then
    echo -e "${RED}Git is not installed. Please install Git first.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Git found: $(git --version)${NC}"

# Check for C++ compiler
if command -v g++ &> /dev/null; then
    echo -e "${GREEN}✓ G++ found: $(g++ --version | head -n1)${NC}"
elif command -v clang++ &> /dev/null; then
    echo -e "${GREEN}✓ Clang++ found: $(clang++ --version | head -n1)${NC}"
elif command -v cl &> /dev/null; then
    echo -e "${GREEN}✓ MSVC found${NC}"
else
    echo -e "${RED}No C++ compiler found. Please install GCC, Clang, or MSVC.${NC}"
    exit 1
fi

# Setup VCPKG
echo -e "\n${BLUE}Setting up VCPKG...${NC}"

# Check if VCPKG_ROOT is already set
if [ -n "$VCPKG_ROOT" ] && [ -d "$VCPKG_ROOT" ]; then
    echo -e "${GREEN}✓ VCPKG already installed at: $VCPKG_ROOT${NC}"
else
    # Check common locations
    VCPKG_LOCATIONS=(
        "$HOME/vcpkg"
        "$HOME/.vcpkg"
        "/opt/vcpkg"
        "C:/vcpkg"
        "C:/tools/vcpkg"
    )
    
    FOUND_VCPKG=false
    for loc in "${VCPKG_LOCATIONS[@]}"; do
        if [ -d "$loc" ] && [ -f "$loc/vcpkg" ]; then
            export VCPKG_ROOT="$loc"
            echo -e "${GREEN}✓ Found VCPKG at: $VCPKG_ROOT${NC}"
            FOUND_VCPKG=true
            break
        fi
    done
    
    if [ "$FOUND_VCPKG" = false ]; then
        echo -e "${YELLOW}VCPKG not found. Installing...${NC}"
        
        # Install in user's home directory
        VCPKG_ROOT="$HOME/vcpkg"
        
        if [ -d "$VCPKG_ROOT" ]; then
            echo -e "${YELLOW}Directory $VCPKG_ROOT exists. Removing old installation...${NC}"
            rm -rf "$VCPKG_ROOT"
        fi
        
        git clone https://github.com/Microsoft/vcpkg.git "$VCPKG_ROOT"
        
        # Bootstrap VCPKG
        if [[ "$OS" == "windows" ]]; then
            "$VCPKG_ROOT/bootstrap-vcpkg.bat"
        else
            "$VCPKG_ROOT/bootstrap-vcpkg.sh"
        fi
        
        echo -e "${GREEN}✓ VCPKG installed at: $VCPKG_ROOT${NC}"
    fi
fi

# Export VCPKG variables
export VCPKG_ROOT
export PATH="$VCPKG_ROOT:$PATH"
export VCPKG_TOOLCHAIN_PATH="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

# Add to shell profile if not already there
add_to_profile() {
    local profile_file="$1"
    if [ -f "$profile_file" ]; then
        if ! grep -q "VCPKG_ROOT" "$profile_file"; then
            echo "" >> "$profile_file"
            echo "# VCPKG configuration" >> "$profile_file"
            echo "export VCPKG_ROOT=\"$VCPKG_ROOT\"" >> "$profile_file"
            echo "export PATH=\"\$VCPKG_ROOT:\$PATH\"" >> "$profile_file"
            echo "export VCPKG_TOOLCHAIN_PATH=\"\$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake\"" >> "$profile_file"
            echo -e "${GREEN}✓ Added VCPKG to $profile_file${NC}"
        fi
    fi
}

# Add to appropriate shell profile
if [[ "$SHELL" == *"bash"* ]]; then
    add_to_profile "$HOME/.bashrc"
elif [[ "$SHELL" == *"zsh"* ]]; then
    add_to_profile "$HOME/.zshrc"
fi

# Install dependencies via VCPKG
echo -e "\n${BLUE}Installing dependencies via VCPKG...${NC}"
echo -e "${YELLOW}This may take several minutes on first run...${NC}"

"$VCPKG_ROOT/vcpkg" install --triplet="$VCPKG_TRIPLET"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Dependencies installed successfully${NC}"
else
    echo -e "${RED}Failed to install dependencies. Check vcpkg-manifest-install.log for details.${NC}"
    exit 1
fi

# Setup Python environment
echo -e "\n${BLUE}Setting up Python environment...${NC}"

# Check for uv
if command -v uv &> /dev/null; then
    echo -e "${GREEN}✓ uv found${NC}"
    
    # Create venv if it doesn't exist
    if [ ! -d ".venv" ]; then
        uv venv
    fi
    
    # Sync dependencies
    uv sync
    echo -e "${GREEN}✓ Python environment ready${NC}"
else
    echo -e "${YELLOW}uv not found. Install it for Python development:${NC}"
    echo -e "${YELLOW}  curl -LsSf https://astral.sh/uv/install.sh | sh${NC}"
fi

# Create Makefile if it doesn't exist
if [ ! -f "Makefile" ]; then
    echo -e "\n${BLUE}Creating Makefile...${NC}"
    cat > Makefile << 'EOF'
.PHONY: all clean debug release test format

# Configuration
VCPKG_TOOLCHAIN_PATH ?= $(VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake
BUILD_TYPE ?= Release
GENERATOR ?= "Unix Makefiles"
NPROCS ?= $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

all: release

release:
	mkdir -p build/release
	cd build/release && cmake -G $(GENERATOR) \
		-DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_TOOLCHAIN_PATH) \
		-DEXTENSION_STATIC_BUILD=1 \
		../..
	cmake --build build/release --config Release --parallel $(NPROCS)

debug:
	mkdir -p build/debug
	cd build/debug && cmake -G $(GENERATOR) \
		-DCMAKE_BUILD_TYPE=Debug \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_TOOLCHAIN_PATH) \
		-DEXTENSION_STATIC_BUILD=1 \
		../..
	cmake --build build/debug --config Debug --parallel $(NPROCS)

test: release
	@echo "Running tests..."
	cd build/release && ctest --output-on-failure

clean:
	rm -rf build

format:
	find src -name "*.cpp" -o -name "*.hpp" | xargs clang-format -i

# Install the extension locally for testing
install: release
	cp build/release/extension/anndata/anndata.duckdb_extension ~/.duckdb/extensions/
	@echo "Extension installed to ~/.duckdb/extensions/"
EOF
    echo -e "${GREEN}✓ Makefile created${NC}"
fi

# Update .gitignore
echo -e "\n${BLUE}Updating .gitignore...${NC}"
if [ -f ".gitignore" ]; then
    # Add VCPKG entries if not present
    if ! grep -q "vcpkg_installed" .gitignore; then
        echo "" >> .gitignore
        echo "# VCPKG" >> .gitignore
        echo "vcpkg_installed/" >> .gitignore
        echo "vcpkg-manifest-install.log" >> .gitignore
        echo -e "${GREEN}✓ Updated .gitignore${NC}"
    fi
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nEnvironment variables set:"
echo -e "  VCPKG_ROOT=$VCPKG_ROOT"
echo -e "  VCPKG_TOOLCHAIN_PATH=$VCPKG_TOOLCHAIN_PATH"
echo -e "  VCPKG_TRIPLET=$VCPKG_TRIPLET"
echo -e "\nNext steps:"
echo -e "  1. ${BLUE}source ~/.bashrc${NC} (or restart terminal)"
echo -e "  2. ${BLUE}make${NC} to build the extension"
echo -e "  3. ${BLUE}make test${NC} to run tests"
echo -e "\nUseful commands:"
echo -e "  ${BLUE}make release${NC} - Build release version"
echo -e "  ${BLUE}make debug${NC}   - Build debug version"
echo -e "  ${BLUE}make clean${NC}   - Clean build files"
echo -e "  ${BLUE}make format${NC}  - Format code"