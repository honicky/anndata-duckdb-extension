#pragma once

#include "duckdb.hpp"
#include "h5_reader_multithreaded.hpp"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace duckdb {

// Schema mode for multi-file queries
enum class SchemaMode { INTERSECTION, UNION };

// Information about a single column
struct ColumnInfo {
	string name;
	string original_name; // HDF5 name (for mangled names)
	LogicalType type;

	ColumnInfo() = default;
	ColumnInfo(const string &n, const string &orig, LogicalType t) : name(n), original_name(orig), type(t) {
	}
};

// Schema information for a single file
struct FileSchema {
	string file_path;
	vector<ColumnInfo> columns;

	// For X/layers: var information
	idx_t n_obs = 0;
	idx_t n_var = 0;
	vector<string> var_names;
	unordered_map<string, idx_t> var_name_to_idx;

	FileSchema() = default;
	FileSchema(const string &path) : file_path(path) {
	}
};

// Result of schema harmonization
struct HarmonizedSchema {
	// The final schema to use
	vector<ColumnInfo> columns;

	// Per-file column mappings: for each file, maps harmonized column index -> file column index
	// -1 means the column doesn't exist in that file (union mode only)
	vector<vector<int>> file_column_mappings;

	// For X/layers: per-file var mappings
	// Maps harmonized var index -> file var index
	vector<vector<idx_t>> file_var_mappings;
	vector<string> common_var_names;

	// Total row count across all files (for non-X tables)
	idx_t total_row_count = 0;

	// Per-file row counts
	vector<idx_t> file_row_counts;
};

// Utility class for schema harmonization
class SchemaHarmonizer {
public:
	// Compute the harmonized schema for obs/var tables
	static HarmonizedSchema ComputeObsVarSchema(const vector<FileSchema> &file_schemas, SchemaMode mode);

	// Compute harmonized schema for X/layers (with var intersection)
	// If projected_var_names is empty, computes full intersection/union
	// Otherwise, only considers the specified var names
	static HarmonizedSchema ComputeXSchema(const vector<FileSchema> &file_schemas, SchemaMode mode,
	                                       const vector<string> &projected_var_names = {});

	// Compute harmonized schema for obsm/varm matrices
	// These have fixed schema (obs_idx/var_idx + dim_0, dim_1, ...) but need dimension intersection
	static HarmonizedSchema ComputeObsmVarmSchema(const vector<FileSchema> &file_schemas, SchemaMode mode,
	                                              idx_t expected_cols);

	// Get the file schema for obs from a single file
	static FileSchema GetObsSchema(ClientContext &context, const string &file_path);

	// Get the file schema for var from a single file
	static FileSchema GetVarSchema(ClientContext &context, const string &file_path);

	// Get the file schema for X matrix from a single file
	static FileSchema GetXSchema(ClientContext &context, const string &file_path, const string &var_name_column);

	// Get the file schema for a layer from a single file
	static FileSchema GetLayerSchema(ClientContext &context, const string &file_path, const string &layer_name,
	                                 const string &var_name_column);

	// Get the file schema for obsm/varm from a single file
	static FileSchema GetObsmSchema(ClientContext &context, const string &file_path, const string &matrix_name);
	static FileSchema GetVarmSchema(ClientContext &context, const string &file_path, const string &matrix_name);

	// Get the file schema for obsp/varp from a single file
	static FileSchema GetObspSchema(ClientContext &context, const string &file_path, const string &matrix_name);
	static FileSchema GetVarpSchema(ClientContext &context, const string &file_path, const string &matrix_name);

	// Helper to coerce types when they differ across files
	static LogicalType CoerceTypes(const LogicalType &type1, const LogicalType &type2);

private:
	// Create H5Reader with proper credentials
	static unique_ptr<H5ReaderMultithreaded> CreateReader(ClientContext &context, const string &file_path);
};

} // namespace duckdb
