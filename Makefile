PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=anndata
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Version management targets
.PHONY: version-patch version-minor version-major version-current release

version-patch:
	@python3 scripts/bump_version.py patch

version-minor:
	@python3 scripts/bump_version.py minor

version-major:
	@python3 scripts/bump_version.py major

version-current:
	@python3 scripts/bump_version.py current

# Release targets - prepare release and create tag
release-patch: format-fix tidy-check
	@echo "Creating patch release..."
	@python3 scripts/bump_version.py patch
	@git add -A
	@VERSION=$$(cat VERSION) && git commit -m "chore: bump version to $$VERSION"
	@VERSION=$$(cat VERSION) && git tag -a "v$$VERSION" -m "Release version $$VERSION"
	@echo ""
	@echo "✅ Release v$$(cat VERSION) prepared!"
	@echo ""
	@echo "To trigger the release:"
	@echo "  git push origin main"
	@echo "  git push origin v$$(cat VERSION)"
	@echo ""
	@echo "This will trigger GitHub Actions to:"
	@echo "  1. Build extensions for all platforms"
	@echo "  2. Create a GitHub release with artifacts"
	@echo "  3. Publish to https://github.com/$$GITHUB_REPOSITORY/releases"

release-minor: format-fix tidy-check
	@echo "Creating minor release..."
	@python3 scripts/bump_version.py minor
	@git add -A
	@VERSION=$$(cat VERSION) && git commit -m "chore: bump version to $$VERSION"
	@VERSION=$$(cat VERSION) && git tag -a "v$$VERSION" -m "Release version $$VERSION"
	@echo ""
	@echo "✅ Release v$$(cat VERSION) prepared!"
	@echo ""
	@echo "To trigger the release:"
	@echo "  git push origin main"
	@echo "  git push origin v$$(cat VERSION)"
	@echo ""
	@echo "This will trigger GitHub Actions to:"
	@echo "  1. Build extensions for all platforms"
	@echo "  2. Create a GitHub release with artifacts"
	@echo "  3. Publish to https://github.com/$$GITHUB_REPOSITORY/releases"

release-major: format-fix tidy-check
	@echo "Creating major release..."
	@python3 scripts/bump_version.py major
	@git add -A
	@VERSION=$$(cat VERSION) && git commit -m "chore: bump version to $$VERSION"
	@VERSION=$$(cat VERSION) && git tag -a "v$$VERSION" -m "Release version $$VERSION"
	@echo ""
	@echo "✅ Release v$$(cat VERSION) prepared!"
	@echo ""
	@echo "To trigger the release:"
	@echo "  git push origin main"
	@echo "  git push origin v$$(cat VERSION)"
	@echo ""
	@echo "This will trigger GitHub Actions to:"
	@echo "  1. Build extensions for all platforms"
	@echo "  2. Create a GitHub release with artifacts"
	@echo "  3. Publish to https://github.com/$$GITHUB_REPOSITORY/releases"