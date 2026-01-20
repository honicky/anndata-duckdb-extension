#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "h5_reader_multithreaded.hpp"
#include "schema_harmonizer.hpp"
#include <string>
#include <memory>

namespace duckdb {

// Forward declaration
class H5ReaderMultithreaded;

class AnndataScanner {
public:
	// Table function for scanning .obs table
	static unique_ptr<FunctionData> ObsBind(ClientContext &context, TableFunctionBindInput &input,
	                                        vector<LogicalType> &return_types, vector<string> &names);
	static void ObsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning .var table
	static unique_ptr<FunctionData> VarBind(ClientContext &context, TableFunctionBindInput &input,
	                                        vector<LogicalType> &return_types, vector<string> &names);
	static void VarScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning X matrix
	static unique_ptr<FunctionData> XBind(ClientContext &context, TableFunctionBindInput &input,
	                                      vector<LogicalType> &return_types, vector<string> &names);
	static void XScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning obsm matrices
	static unique_ptr<FunctionData> ObsmBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void ObsmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning varm matrices
	static unique_ptr<FunctionData> VarmBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void VarmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning layer matrices
	static unique_ptr<FunctionData> LayerBind(ClientContext &context, TableFunctionBindInput &input,
	                                          vector<LogicalType> &return_types, vector<string> &names);
	static void LayerScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for scanning uns (unstructured) data
	static unique_ptr<FunctionData> UnsBind(ClientContext &context, TableFunctionBindInput &input,
	                                        vector<LogicalType> &return_types, vector<string> &names);
	static void UnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table functions for scanning obsp/varp (pairwise matrices)
	static unique_ptr<FunctionData> ObspBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void ObspScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	static unique_ptr<FunctionData> VarpBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void VarpScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table functions for raw section
	static unique_ptr<FunctionData> RawXBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void RawXScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	static unique_ptr<FunctionData> RawVarBind(ClientContext &context, TableFunctionBindInput &input,
	                                           vector<LogicalType> &return_types, vector<string> &names);
	static void RawVarScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	static unique_ptr<FunctionData> RawVarmBind(ClientContext &context, TableFunctionBindInput &input,
	                                            vector<LogicalType> &return_types, vector<string> &names);
	static void RawVarmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Table function for file info
	static unique_ptr<FunctionData> InfoBind(ClientContext &context, TableFunctionBindInput &input,
	                                         vector<LogicalType> &return_types, vector<string> &names);
	static void InfoScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

	// Utility functions
	static bool IsAnndataFile(const string &path);
	static bool IsAnndataFile(ClientContext &context, const string &path);
	static string GetAnndataInfo(const string &path);
};

// Bind data for table functions
struct AnndataBindData : public TableFunctionData {
	string file_path;
	idx_t row_count;
	idx_t column_count;
	vector<string> column_names;   // Display names (may be mangled)
	vector<string> original_names; // Original HDF5 dataset names
	vector<LogicalType> column_types;

	// Multi-file support
	bool is_multi_file = false;
	vector<string> file_paths;        // All files to scan (from glob expansion)
	string original_pattern;          // Original glob pattern
	SchemaMode schema_mode = SchemaMode::INTERSECTION;
	HarmonizedSchema harmonized_schema;

	// For X matrix scanning
	bool is_x_scan = false;
	idx_t n_obs = 0;
	idx_t n_var = 0;
	vector<string> var_names;
	string var_name_column = "_index"; // Default column for gene names (var_names in AnnData, stored as _index in HDF5)
	string var_id_column = "_index";   // Default column for gene IDs

	// For obsm/varm matrix scanning
	bool is_obsm_scan = false;
	bool is_varm_scan = false;
	string obsm_varm_matrix_name;
	idx_t matrix_rows = 0;
	idx_t matrix_cols = 0;

	// For layer scanning
	bool is_layer_scan = false;
	string layer_name;

	// For uns scanning
	bool is_uns_scan = false;
	vector<H5ReaderMultithreaded::UnsInfo> uns_keys;

	// For obsp/varp scanning
	bool is_obsp_scan = false;
	bool is_varp_scan = false;
	string pairwise_matrix_name;
	idx_t nnz = 0; // number of non-zero elements

	// For raw section scanning
	bool is_raw_x_scan = false;
	bool is_raw_var_scan = false;
	bool is_raw_varm_scan = false;

	// For info scanning
	bool is_info_scan = false;

	AnndataBindData(const string &path) : file_path(path), row_count(0), column_count(0) {
	}

	// Constructor for multi-file
	AnndataBindData(const vector<string> &paths, const string &pattern)
	    : file_path(paths.empty() ? "" : paths[0]), row_count(0), column_count(0), is_multi_file(paths.size() > 1),
	      file_paths(paths), original_pattern(pattern) {
	}
};

// Global scan state
struct AnndataGlobalState : public GlobalTableFunctionState {
	idx_t current_row;
	unique_ptr<H5ReaderMultithreaded> h5_reader;
	vector<column_t> column_ids; // Column indices requested by DuckDB (for projection pushdown)

	// Multi-file state
	idx_t current_file_idx = 0;
	idx_t current_row_in_file = 0;
	string current_file_name;
	vector<int> current_column_mapping;   // Maps result columns to file columns
	vector<idx_t> current_var_mapping;    // Maps result var indices to file var indices (for X/layers)

	AnndataGlobalState() : current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}

	// Advance to the next file in multi-file mode
	// Returns true if there's a next file, false if all files are exhausted
	bool AdvanceToNextFile(ClientContext &context, const AnndataBindData &bind_data);

	// Open the current file and set up mappings
	void OpenCurrentFile(ClientContext &context, const AnndataBindData &bind_data);
};

// Local scan state
struct AnndataLocalState : public LocalTableFunctionState {
	idx_t batch_index;

	AnndataLocalState() : batch_index(0) {
	}
};

} // namespace duckdb
