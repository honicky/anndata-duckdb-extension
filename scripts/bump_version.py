#!/usr/bin/env python3
"""
Version bumping utility for the AnnData DuckDB Extension.

Usage:
    python scripts/bump_version.py patch  # 0.1.0 -> 0.1.1
    python scripts/bump_version.py minor  # 0.1.0 -> 0.2.0
    python scripts/bump_version.py major  # 0.1.0 -> 1.0.0
    python scripts/bump_version.py set 0.3.0  # Set specific version
"""

import sys
import re
from pathlib import Path
import subprocess
from typing import Tuple
import datetime

def read_version() -> str:
    """Read current version from VERSION file."""
    version_file = Path("VERSION")
    if not version_file.exists():
        raise FileNotFoundError("VERSION file not found")
    return version_file.read_text().strip()

def parse_version(version: str) -> Tuple[int, int, int]:
    """Parse version string into major, minor, patch."""
    match = re.match(r'^(\d+)\.(\d+)\.(\d+)', version)
    if not match:
        raise ValueError(f"Invalid version format: {version}")
    return int(match.group(1)), int(match.group(2)), int(match.group(3))

def bump_version(version: str, bump_type: str) -> str:
    """Bump version based on type."""
    major, minor, patch = parse_version(version)
    
    if bump_type == "major":
        return f"{major + 1}.0.0"
    elif bump_type == "minor":
        return f"{major}.{minor + 1}.0"
    elif bump_type == "patch":
        return f"{major}.{minor}.{patch + 1}"
    else:
        raise ValueError(f"Invalid bump type: {bump_type}")

def update_version_in_files(old_version: str, new_version: str):
    """Update version in all relevant files."""
    # Only VERSION file needs to be updated now - everything else reads from it
    version_file = Path("VERSION")
    version_file.write_text(new_version)
    print(f"Updated VERSION file: {old_version} → {new_version}")

def update_changelog(new_version: str):
    """Add new version section to CHANGELOG.md."""
    changelog_path = Path("CHANGELOG.md")
    
    # Create CHANGELOG if it doesn't exist
    if not changelog_path.exists():
        changelog_path.write_text(f"""# Changelog

All notable changes to the AnnData DuckDB Extension will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [{new_version}] - {datetime.date.today().isoformat()}

### Added
- Initial release of AnnData DuckDB Extension
- Support for reading obs, var, X matrices
- Support for obsm, varm, and layers
- Sparse matrix support (CSR/CSC formats)

""")
        print(f"Created CHANGELOG.md with version {new_version}")
    else:
        content = changelog_path.read_text()
        
        # Check if version already exists
        if f"## [{new_version}]" in content:
            print(f"Version {new_version} already in CHANGELOG.md")
            return
        
        # Add new version section after [Unreleased]
        unreleased_pattern = r'## \[Unreleased\]'
        if re.search(unreleased_pattern, content):
            new_section = f"""## [Unreleased]

## [{new_version}] - {datetime.date.today().isoformat()}

### Added
- 

### Changed
- 

### Fixed
- 

"""
            new_content = re.sub(
                unreleased_pattern,
                new_section.rstrip(),
                content,
                count=1
            )
            changelog_path.write_text(new_content)
            print(f"Added version {new_version} to CHANGELOG.md")
        else:
            print("Warning: Could not find [Unreleased] section in CHANGELOG.md")

def create_git_tag(version: str, push: bool = False):
    """Create and optionally push a git tag."""
    tag_name = f"v{version}"
    
    # Check if tag already exists
    result = subprocess.run(
        ["git", "tag", "-l", tag_name],
        capture_output=True,
        text=True
    )
    if result.stdout.strip():
        print(f"Tag {tag_name} already exists")
        return
    
    # Create annotated tag
    subprocess.run([
        "git", "tag", "-a", tag_name,
        "-m", f"Release version {version}"
    ], check=True)
    print(f"Created tag {tag_name}")
    
    if push:
        subprocess.run(["git", "push", "origin", tag_name], check=True)
        print(f"Pushed tag {tag_name} to origin")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    
    try:
        current_version = read_version()
        print(f"Current version: {current_version}")
        
        if command == "set" and len(sys.argv) > 2:
            new_version = sys.argv[2]
            # Validate version format
            parse_version(new_version)
        elif command in ["major", "minor", "patch"]:
            new_version = bump_version(current_version, command)
        elif command == "current":
            print(f"Version: {current_version}")
            sys.exit(0)
        else:
            print(f"Invalid command: {command}")
            print(__doc__)
            sys.exit(1)
        
        print(f"New version: {new_version}")
        
        # Update version in all files
        update_version_in_files(current_version, new_version)
        
        # Update changelog
        update_changelog(new_version)
        
        # Commit changes
        print("\nTo complete the version bump:")
        print(f"  git add VERSION CHANGELOG.md")
        print(f'  git commit -m "chore: bump version to {new_version}"')
        print(f"  git push origin main")
        
        # Optionally create tag
        if "--tag" in sys.argv:
            create_git_tag(new_version, push="--push" in sys.argv)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()