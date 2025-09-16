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

// Table function implementations for X matrix
unique_ptr<FunctionData> AnndataScanner::XBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());

	// Check for optional var_name_column parameter
	if (input.inputs.size() > 1) {
		bind_data->var_name_column = input.inputs[1].GetValue<string>();
	}

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get matrix info
	auto x_info = reader.GetXMatrixInfo();
	bind_data->n_obs = x_info.n_obs;
	bind_data->n_var = x_info.n_var;
	bind_data->is_x_scan = true;

	// Get variable names for column headers
	bind_data->var_names = reader.GetVarNames(bind_data->var_name_column);

	// Set up columns: obs_idx + one column per gene
	names.push_back("obs_idx");
	return_types.push_back(LogicalType::BIGINT);

	// Add one column for each gene
	for (size_t i = 0; i < bind_data->n_var && i < bind_data->var_names.size(); i++) {
		names.push_back(bind_data->var_names[i]);
		return_types.push_back(LogicalType::DOUBLE);
	}

	// Set row count to number of observations
	bind_data->row_count = bind_data->n_obs;
	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::XScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	// Calculate how many observations to read
	idx_t remaining = bind_data.n_obs - gstate.current_row;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (count == 0) {
		return;
	}

	// First column is obs_idx
	auto &obs_idx_vec = output.data[0];
	for (idx_t i = 0; i < count; i++) {
		obs_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row + i));
	}

	// Initialize all gene columns with zeros (NULL would be better but DOUBLE(0.0) for now)
	for (idx_t var_idx = 0; var_idx < bind_data.n_var; var_idx++) {
		auto &gene_vec = output.data[var_idx + 1]; // +1 to skip obs_idx column
		for (idx_t obs_idx = 0; obs_idx < count; obs_idx++) {
			gene_vec.SetValue(obs_idx, Value::DOUBLE(0.0));
		}
	}

	// Check if matrix is sparse
	auto x_info = gstate.h5_reader->GetXMatrixInfo();
	if (x_info.is_sparse) {
		// Read sparse matrix - only non-zero values
		auto sparse_data = gstate.h5_reader->ReadSparseXMatrix(gstate.current_row, count, 0, bind_data.n_var);

		// Fill in the non-zero values
		for (size_t i = 0; i < sparse_data.values.size(); i++) {
			idx_t obs_idx = sparse_data.row_indices[i];
			idx_t var_idx = sparse_data.col_indices[i];
			double value = sparse_data.values[i];

			// Set the value in the appropriate column
			auto &gene_vec = output.data[var_idx + 1]; // +1 to skip obs_idx column
			gene_vec.SetValue(obs_idx, Value::DOUBLE(value));
		}
	} else {
		// Read dense matrix
		std::vector<double> values;
		gstate.h5_reader->ReadXMatrix(gstate.current_row, count, 0, bind_data.n_var, values);

		// Fill gene expression columns from dense matrix
		for (idx_t var_idx = 0; var_idx < bind_data.n_var; var_idx++) {
			auto &gene_vec = output.data[var_idx + 1]; // +1 to skip obs_idx column

			for (idx_t obs_idx = 0; obs_idx < count; obs_idx++) {
				// Matrix is stored row-major: [obs][var]
				idx_t matrix_idx = obs_idx * bind_data.n_var + var_idx;
				if (matrix_idx < values.size()) {
					gene_vec.SetValue(obs_idx, Value::DOUBLE(values[matrix_idx]));
				}
			}
		}
	}

	gstate.current_row += count;
	output.SetCardinality(count);
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

	// Register anndata_scan_x function
	TableFunction x_func("anndata_scan_x", {LogicalType::VARCHAR}, AnndataScanner::XScan, AnndataScanner::XBind,
	                     AnndataInitGlobal, AnndataInitLocal);
	x_func.name = "anndata_scan_x";
	ExtensionUtil::RegisterFunction(db, x_func);

	// Also register with optional var_name_column parameter
	TableFunction x_func_with_param("anndata_scan_x", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AnndataScanner::XScan, AnndataScanner::XBind, AnndataInitGlobal, AnndataInitLocal);
	x_func_with_param.name = "anndata_scan_x";
	ExtensionUtil::RegisterFunction(db, x_func_with_param);

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
