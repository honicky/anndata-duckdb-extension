#include "include/glob_handler.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include <algorithm>

namespace duckdb {

bool GlobHandler::IsGlobPattern(const string &path) {
	// Check for common glob characters
	return path.find('*') != string::npos || path.find('?') != string::npos || path.find('[') != string::npos;
}

bool GlobHandler::IsRemotePath(const string &path) {
	return path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	       path.rfind("s3a://", 0) == 0 || path.rfind("gs://", 0) == 0;
}

string GlobHandler::GetBaseName(const string &path) {
	// Find the last path separator
	size_t last_sep = path.find_last_of("/\\");
	if (last_sep == string::npos) {
		return path;
	}
	return path.substr(last_sep + 1);
}

vector<string> GlobHandler::ExpandLocalGlob(ClientContext &context, const string &pattern) {
	auto &fs = FileSystem::GetFileSystem(context);
	vector<string> result;

	// Use DuckDB's built-in glob functionality
	auto matches = fs.GlobFiles(pattern, context);
	for (const auto &match : matches) {
		result.push_back(match);
	}

	// Sort for consistent ordering
	std::sort(result.begin(), result.end());
	return result;
}

vector<string> GlobHandler::ExpandS3Glob(ClientContext &context, const string &pattern) {
	// For S3, we need to convert glob patterns to prefix-based listing
	// This is a simplified implementation - full glob support would require
	// listing all objects and filtering client-side

	// Find the position of the first glob character
	size_t glob_pos = pattern.find_first_of("*?[");
	if (glob_pos == string::npos) {
		// No glob characters, return as-is
		return {pattern};
	}

	// Find the last '/' before the glob character to get the prefix
	size_t prefix_end = pattern.rfind('/', glob_pos);
	string prefix;
	string suffix_pattern;

	if (prefix_end == string::npos) {
		// Glob in the bucket name - not supported
		throw InvalidInputException("Glob patterns in S3 bucket names are not supported: " + pattern);
	}

	prefix = pattern.substr(0, prefix_end + 1);
	suffix_pattern = pattern.substr(prefix_end + 1);

	// For now, use DuckDB's FileSystem which may have S3 glob support
	// If httpfs extension is loaded, it should handle S3 globs
	auto &fs = FileSystem::GetFileSystem(context);
	vector<string> result;

	try {
		auto matches = fs.GlobFiles(pattern, context);
		for (const auto &match : matches) {
			result.push_back(match);
		}
	} catch (const std::exception &e) {
		// If glob fails, try to provide a helpful error message
		throw InvalidInputException("Failed to expand S3 glob pattern '" + pattern +
		                            "'. Ensure the httpfs extension is loaded and credentials are configured. "
		                            "Error: " +
		                            e.what());
	}

	std::sort(result.begin(), result.end());
	return result;
}

GlobResult GlobHandler::ExpandGlobPattern(ClientContext &context, const string &pattern) {
	GlobResult result;
	result.original_pattern = pattern;
	result.is_pattern = IsGlobPattern(pattern);
	result.is_remote = IsRemotePath(pattern);

	if (!result.is_pattern) {
		// Not a glob pattern, just return the single file
		result.matched_files.push_back(pattern);
		return result;
	}

	if (result.is_remote) {
		result.matched_files = ExpandS3Glob(context, pattern);
	} else {
		result.matched_files = ExpandLocalGlob(context, pattern);
	}

	if (result.matched_files.empty()) {
		throw InvalidInputException("No files matching pattern '" + pattern + "' found");
	}

	return result;
}

GlobResult GlobHandler::ExpandGlobPatterns(ClientContext &context, const vector<string> &patterns) {
	GlobResult combined;
	combined.is_pattern = false;
	combined.is_remote = false;

	for (const auto &pattern : patterns) {
		auto single_result = ExpandGlobPattern(context, pattern);

		// Update flags
		if (single_result.is_pattern) {
			combined.is_pattern = true;
		}
		if (single_result.is_remote) {
			combined.is_remote = true;
		}

		// Add matched files (avoid duplicates)
		for (const auto &file : single_result.matched_files) {
			if (std::find(combined.matched_files.begin(), combined.matched_files.end(), file) ==
			    combined.matched_files.end()) {
				combined.matched_files.push_back(file);
			}
		}
	}

	// Sort for consistent ordering
	std::sort(combined.matched_files.begin(), combined.matched_files.end());

	if (patterns.size() == 1) {
		combined.original_pattern = patterns[0];
	} else {
		combined.original_pattern = "[multiple patterns]";
	}

	return combined;
}

} // namespace duckdb
