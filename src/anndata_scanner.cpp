#include "include/anndata_scanner.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

bool AnndataScanner::IsAnndataFile(const string &path) {
	// Check if file exists and has .h5ad extension
	std::ifstream file(path);
	if (!file.good()) {
		return false;
	}

	// Check extension
	if (path.size() < 5) {
		return false;
	}

	string ext = path.substr(path.size() - 5);
	return ext == ".h5ad";
}

string AnndataScanner::GetAnndataInfo(const string &path) {
	if (!IsAnndataFile(path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + path);
	}

	// For now, return basic info
	// In real implementation, we'd read HDF5 metadata
	return StringUtil::Format("AnnData file: %s", path.c_str());
}

// Table function implementations for .obs table
unique_ptr<FunctionData> AnndataScanner::ObsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get actual columns from HDF5
	auto columns = reader.GetObsColumns();

	for (const auto &col : columns) {
		names.push_back(col.name);
		return_types.push_back(col.type);
	}

	// Get actual row count
	bind_data->row_count = reader.GetObsCount();
	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::ObsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
	if (count == 0) {
		return;
	}

	// Read actual data from HDF5
	for (idx_t col = 0; col < bind_data.column_count; col++) {
		auto &vec = output.data[col];
		gstate.h5_reader->ReadObsColumn(bind_data.column_names[col], vec, gstate.current_row, count);
	}

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Table function implementations for .var table
unique_ptr<FunctionData> AnndataScanner::VarBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get actual columns from HDF5
	auto columns = reader.GetVarColumns();

	for (const auto &col : columns) {
		names.push_back(col.name);
		return_types.push_back(col.type);
	}

	// Get actual row count
	bind_data->row_count = reader.GetVarCount();
	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::VarScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
	if (count == 0) {
		return;
	}

	// Read actual data from HDF5
	for (idx_t col = 0; col < bind_data.column_count; col++) {
		auto &vec = output.data[col];
		gstate.h5_reader->ReadVarColumn(bind_data.column_names[col], vec, gstate.current_row, count);
	}

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Global state initialization
static unique_ptr<GlobalTableFunctionState> AnndataInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AnndataGlobalState>();
}

// Local state initialization
static unique_ptr<LocalTableFunctionState> AnndataInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                            GlobalTableFunctionState *global_state) {
	return make_uniq<AnndataLocalState>();
}

// Register the table functions
void RegisterAnndataTableFunctions(DatabaseInstance &db) {
	// Register anndata_scan_obs function
	TableFunction obs_func("anndata_scan_obs", {LogicalType::VARCHAR}, AnndataScanner::ObsScan, AnndataScanner::ObsBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	obs_func.name = "anndata_scan_obs";
	ExtensionUtil::RegisterFunction(db, obs_func);

	// Register anndata_scan_var function
	TableFunction var_func("anndata_scan_var", {LogicalType::VARCHAR}, AnndataScanner::VarScan, AnndataScanner::VarBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	var_func.name = "anndata_scan_var";
	ExtensionUtil::RegisterFunction(db, var_func);

	// Register anndata_info function (scalar function)
	auto info_func = ScalarFunction(
	    "anndata_info", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    [](DataChunk &args, ExpressionState &state, Vector &result) {
		    auto &path_vec = args.data[0];
		    UnaryExecutor::Execute<string_t, string_t>(path_vec, result, args.size(), [&](string_t path) {
			    return StringVector::AddString(result, AnndataScanner::GetAnndataInfo(path.GetString()));
		    });
	    });
	ExtensionUtil::RegisterFunction(db, info_func);
}

} // namespace duckdb
