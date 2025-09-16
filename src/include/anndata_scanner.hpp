#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "h5_reader.hpp"
#include <string>
#include <memory>

namespace duckdb {

// Forward declaration
class H5Reader;

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

	// Utility functions
	static bool IsAnndataFile(const string &path);
	static string GetAnndataInfo(const string &path);
};

// Bind data for table functions
struct AnndataBindData : public TableFunctionData {
	string file_path;
	idx_t row_count;
	idx_t column_count;
	vector<string> column_names;
	vector<LogicalType> column_types;
	
	// For X matrix scanning
	bool is_x_scan = false;
	idx_t n_obs = 0;
	idx_t n_var = 0;
	vector<string> var_names;
	string var_name_column = "_index";  // Default column for gene names

	AnndataBindData(const string &path) : file_path(path), row_count(0), column_count(0) {
	}
};

// Global scan state
struct AnndataGlobalState : public GlobalTableFunctionState {
	idx_t current_row;
	unique_ptr<H5Reader> h5_reader;

	AnndataGlobalState() : current_row(0) {
	}

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for now
	}
};

// Local scan state
struct AnndataLocalState : public LocalTableFunctionState {
	idx_t batch_index;

	AnndataLocalState() : batch_index(0) {
	}
};

} // namespace duckdb
