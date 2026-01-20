#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Result of glob pattern expansion
struct GlobResult {
	vector<string> matched_files;
	bool is_pattern; // True if input contained glob characters
	bool is_remote;  // True if any file is remote (S3, HTTP, etc.)
	string original_pattern;

	GlobResult() : is_pattern(false), is_remote(false) {
	}
};

// Utility class for handling glob patterns
class GlobHandler {
public:
	// Check if a path contains glob pattern characters
	static bool IsGlobPattern(const string &path);

	// Check if a path is a remote URL (S3, HTTP, etc.)
	static bool IsRemotePath(const string &path);

	// Expand a glob pattern to a list of matching files
	// For local files, uses DuckDB's FileSystem::Glob
	// For S3, uses prefix listing (limited glob support)
	static GlobResult ExpandGlobPattern(ClientContext &context, const string &pattern);

	// Expand multiple patterns and combine results
	static GlobResult ExpandGlobPatterns(ClientContext &context, const vector<string> &patterns);

	// Extract the base filename from a path (for _file_name column)
	static string GetBaseName(const string &path);

private:
	// Expand local file glob pattern
	static vector<string> ExpandLocalGlob(ClientContext &context, const string &pattern);

	// Expand S3 glob pattern using prefix listing
	static vector<string> ExpandS3Glob(ClientContext &context, const string &pattern);
};

} // namespace duckdb
