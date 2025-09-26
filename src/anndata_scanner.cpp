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

	H5Reader reader(path);
	std::stringstream info;

	info << "AnnData file: " << path << "\n";
	info << "  Observations: " << reader.GetObsCount() << "\n";
	info << "  Variables: " << reader.GetVarCount() << "\n";

	// Get X matrix info
	auto x_info = reader.GetXMatrixInfo();
	info << "  X matrix: " << x_info.n_obs << " x " << x_info.n_var;
	if (x_info.is_sparse) {
		info << " (sparse, " << x_info.sparse_format << ")";
	}
	info << "\n";

	// List obsm matrices
	auto obsm_matrices = reader.GetObsmMatrices();
	if (!obsm_matrices.empty()) {
		info << "  obsm matrices:\n";
		for (const auto &matrix : obsm_matrices) {
			info << "    - " << matrix.name << ": " << matrix.rows << " x " << matrix.cols << "\n";
		}
	}

	// List varm matrices
	auto varm_matrices = reader.GetVarmMatrices();
	if (!varm_matrices.empty()) {
		info << "  varm matrices:\n";
		for (const auto &matrix : varm_matrices) {
			info << "    - " << matrix.name << ": " << matrix.rows << " x " << matrix.cols << "\n";
		}
	}

	// List layers
	auto layers = reader.GetLayers();
	if (!layers.empty()) {
		info << "  layers:\n";
		for (const auto &layer : layers) {
			info << "    - " << layer.name << ": " << layer.rows << " x " << layer.cols;
			if (layer.is_sparse) {
				info << " (sparse, " << layer.sparse_format << ")";
			}
			info << "\n";
		}
	}

	return info.str();
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

	// Get actual columns from HDF5 (already includes obs_idx)
	auto columns = reader.GetObsColumns();

	vector<string> original_names;
	for (const auto &col : columns) {
		names.push_back(col.name);
		original_names.push_back(col.original_name);
		return_types.push_back(col.type);
	}

	// Get actual row count
	bind_data->row_count = reader.GetObsCount();
	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->original_names = original_names;
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

	// Read actual data from HDF5 (obs_idx handling is in ReadObsColumn)
	for (idx_t col = 0; col < bind_data.column_count; col++) {
		auto &vec = output.data[col];
		// Use original_name for reading from HDF5 (handles mangled names)
		gstate.h5_reader->ReadObsColumn(bind_data.original_names[col], vec, gstate.current_row, count);
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

	// Get actual columns from HDF5 (already includes var_idx)
	auto columns = reader.GetVarColumns();

	vector<string> original_names;
	for (const auto &col : columns) {
		names.push_back(col.name);
		original_names.push_back(col.original_name);
		return_types.push_back(col.type);
	}

	// Get actual row count
	bind_data->row_count = reader.GetVarCount();
	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->original_names = original_names;
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

	// Read actual data from HDF5 (var_idx handling is in ReadVarColumn)
	for (idx_t col = 0; col < bind_data.column_count; col++) {
		auto &vec = output.data[col];
		// Use original_name for reading from HDF5 (handles mangled names)
		gstate.h5_reader->ReadVarColumn(bind_data.original_names[col], vec, gstate.current_row, count);
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

	// Use unified batch reader for X matrix
	gstate.h5_reader->ReadXMatrixBatch(gstate.current_row, count, 0, bind_data.n_var, output);

	gstate.current_row += count;
}

// Table function implementations for obsm matrices
unique_ptr<FunctionData> AnndataScanner::ObsmBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Require file path and matrix name
	if (input.inputs.size() < 2) {
		throw InvalidInputException("anndata_scan_obsm requires file path and matrix name");
	}

	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	bind_data->obsm_varm_matrix_name = input.inputs[1].GetValue<string>();
	bind_data->is_obsm_scan = true;

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get obsm matrix info
	auto matrices = reader.GetObsmMatrices();
	bool found = false;

	for (const auto &matrix : matrices) {
		if (matrix.name == bind_data->obsm_varm_matrix_name) {
			bind_data->matrix_rows = matrix.rows;
			bind_data->matrix_cols = matrix.cols;
			bind_data->row_count = matrix.rows;

			// First column is obs_idx
			names.push_back("obs_idx");
			return_types.push_back(LogicalType::BIGINT);

			// Add columns for each dimension
			for (idx_t i = 0; i < matrix.cols; i++) {
				names.push_back(bind_data->obsm_varm_matrix_name + "_" + to_string(i));
				return_types.push_back(matrix.dtype);
			}

			bind_data->column_count = names.size();
			bind_data->column_names = names;
			bind_data->column_types = return_types;

			found = true;
			break;
		}
	}

	if (!found) {
		throw InvalidInputException("obsm matrix '%s' not found in file", bind_data->obsm_varm_matrix_name.c_str());
	}

	return std::move(bind_data);
}

void AnndataScanner::ObsmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
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

	// First column is obs_idx
	auto &obs_idx_vec = output.data[0];
	for (idx_t i = 0; i < count; i++) {
		obs_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row + i));
	}

	// Read each column of the matrix
	for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
		auto &vec = output.data[col + 1]; // +1 to skip obs_idx
		gstate.h5_reader->ReadObsmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row, count, col, vec);
	}

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Table function implementations for varm matrices
unique_ptr<FunctionData> AnndataScanner::VarmBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Require file path and matrix name
	if (input.inputs.size() < 2) {
		throw InvalidInputException("anndata_scan_varm requires file path and matrix name");
	}

	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	bind_data->obsm_varm_matrix_name = input.inputs[1].GetValue<string>();
	bind_data->is_varm_scan = true;

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get varm matrix info
	auto matrices = reader.GetVarmMatrices();
	bool found = false;

	for (const auto &matrix : matrices) {
		if (matrix.name == bind_data->obsm_varm_matrix_name) {
			bind_data->matrix_rows = matrix.rows;
			bind_data->matrix_cols = matrix.cols;
			bind_data->row_count = matrix.rows;

			// First column is var_idx
			names.push_back("var_idx");
			return_types.push_back(LogicalType::BIGINT);

			// Add columns for each dimension
			for (idx_t i = 0; i < matrix.cols; i++) {
				names.push_back(bind_data->obsm_varm_matrix_name + "_" + to_string(i));
				return_types.push_back(matrix.dtype);
			}

			bind_data->column_count = names.size();
			bind_data->column_names = names;
			bind_data->column_types = return_types;

			found = true;
			break;
		}
	}

	if (!found) {
		throw InvalidInputException("varm matrix '%s' not found in file", bind_data->obsm_varm_matrix_name.c_str());
	}

	return std::move(bind_data);
}

void AnndataScanner::VarmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
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

	// First column is var_idx
	auto &var_idx_vec = output.data[0];
	for (idx_t i = 0; i < count; i++) {
		var_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row + i));
	}

	// Read each column of the matrix
	for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
		auto &vec = output.data[col + 1]; // +1 to skip var_idx
		gstate.h5_reader->ReadVarmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row, count, col, vec);
	}

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Helper functions for error handling
unique_ptr<FunctionData> ObsmBindError(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	throw InvalidInputException("anndata_scan_obsm requires file path and matrix name");
}

unique_ptr<FunctionData> VarmBindError(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	throw InvalidInputException("anndata_scan_varm requires file path and matrix name");
}

// Layer scanning implementation
unique_ptr<FunctionData> AnndataScanner::LayerBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	result->is_layer_scan = true;
	result->layer_name = input.inputs[1].GetValue<string>();

	// Open file to get layer information
	H5Reader reader(result->file_path);
	auto layers = reader.GetLayers();

	// Find the requested layer
	bool found = false;
	H5Reader::LayerInfo layer_info;
	for (const auto &layer : layers) {
		if (layer.name == result->layer_name) {
			layer_info = layer;
			found = true;
			break;
		}
	}

	if (!found) {
		throw InvalidInputException("Layer '%s' not found in file %s", result->layer_name.c_str(),
		                            result->file_path.c_str());
	}

	// Get dimensions
	result->n_obs = layer_info.rows;
	result->n_var = layer_info.cols;

	// Get variable names - allow custom column selection
	string var_column;
	if (input.inputs.size() > 2) {
		var_column = input.inputs[2].GetValue<string>();
	} else {
		// Try to find a suitable default column
		auto var_columns = reader.GetVarColumns();
		bool has_gene_name = false;
		bool has_gene_id = false;
		for (const auto &col : var_columns) {
			if (col.name == "gene_name")
				has_gene_name = true;
			if (col.name == "gene_id")
				has_gene_id = true;
		}

		// Use gene_name if available, otherwise gene_id, otherwise generate generic names
		if (has_gene_name) {
			var_column = "gene_name";
		} else if (has_gene_id) {
			var_column = "gene_id";
		} else {
			var_column = ""; // Will generate generic names
		}
	}

	// Validate var column if specified
	if (!var_column.empty()) {
		auto var_columns = reader.GetVarColumns();
		bool var_column_found = false;
		for (const auto &col : var_columns) {
			if (col.name == var_column) {
				var_column_found = true;
				break;
			}
		}

		if (!var_column_found) {
			throw InvalidInputException("Variable column '%s' not found in var table", var_column.c_str());
		}
	}

	// Read variable names for column headers
	result->var_names.resize(result->n_var);
	for (idx_t i = 0; i < result->n_var; i++) {
		string var_name;
		if (var_column.empty()) {
			// Generate generic names
			var_name = "gene_" + std::to_string(i);
		} else {
			var_name = reader.ReadVarColumnString(var_column, i);
			// Sanitize column names
			std::replace(var_name.begin(), var_name.end(), ' ', '_');
			std::replace(var_name.begin(), var_name.end(), '-', '_');
			std::replace(var_name.begin(), var_name.end(), '.', '_');
		}
		result->var_names[i] = var_name;
	}

	// Set up column schema: obs_idx + all gene columns
	names.push_back("obs_idx");
	return_types.push_back(LogicalType::BIGINT);

	for (const auto &var_name : result->var_names) {
		names.push_back(var_name);
		return_types.push_back(layer_info.dtype);
	}

	result->column_names = names;
	result->column_types = return_types;
	result->column_count = names.size();
	result->row_count = result->n_obs;

	return std::move(result);
}

void AnndataScanner::LayerScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AnndataBindData>();
	auto &state = data.global_state->Cast<AnndataGlobalState>();

	// Initialize h5_reader if not already done
	if (!state.h5_reader) {
		state.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	// Calculate how many rows to read
	idx_t remaining = bind_data.row_count - state.current_row;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (count == 0) {
		return;
	}

	// Use batch reading for better performance
	state.h5_reader->ReadLayerMatrixBatch(bind_data.layer_name, state.current_row, count, 0, bind_data.n_var, output);

	state.current_row += count;
}

// Error handling function for layers when layer name is missing
unique_ptr<FunctionData> LayerBindError(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	throw InvalidInputException("anndata_scan_layers requires layer name");
}

void DummyScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// Never called, bind function throws error
}

// Table function implementations for uns (unstructured) data
unique_ptr<FunctionData> AnndataScanner::UnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	bind_data->is_uns_scan = true;

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get uns keys
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get uns keys
	bind_data->uns_keys = reader.GetUnsKeys();

	if (bind_data->uns_keys.empty()) {
		// No uns data - return empty result with just a message column
		names.push_back("message");
		return_types.push_back(LogicalType::VARCHAR);
		bind_data->row_count = 1;
	} else {
		// Set up column schema
		names.push_back("key");
		return_types.push_back(LogicalType::VARCHAR);

		names.push_back("type");
		return_types.push_back(LogicalType::VARCHAR);

		names.push_back("dtype");
		return_types.push_back(LogicalType::VARCHAR);

		names.push_back("shape");
		return_types.push_back(LogicalType::VARCHAR);

		names.push_back("value");
		return_types.push_back(LogicalType::VARCHAR);

		bind_data->row_count = bind_data->uns_keys.size();
	}

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::UnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	if (bind_data.uns_keys.empty()) {
		// No uns data - return single row with message
		if (gstate.current_row == 0) {
			output.data[0].SetValue(0, Value("No uns data in file"));
			output.SetCardinality(1);
			gstate.current_row = 1;
		}
		return;
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
	if (count == 0) {
		return;
	}

	// Fill columns with uns metadata
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = gstate.current_row + i;
		const auto &uns_info = bind_data.uns_keys[idx];

		// Key column
		output.data[0].SetValue(i, Value(uns_info.key));

		// Type column
		output.data[1].SetValue(i, Value(uns_info.type));

		// Dtype column
		string dtype_str;
		switch (uns_info.dtype.id()) {
		case LogicalTypeId::VARCHAR:
			dtype_str = "string";
			break;
		case LogicalTypeId::BIGINT:
			dtype_str = "int64";
			break;
		case LogicalTypeId::INTEGER:
			dtype_str = "int32";
			break;
		case LogicalTypeId::DOUBLE:
			dtype_str = "float64";
			break;
		case LogicalTypeId::BOOLEAN:
			dtype_str = "bool";
			break;
		default:
			dtype_str = "unknown";
			break;
		}
		output.data[2].SetValue(i, Value(dtype_str));

		// Shape column
		if (uns_info.type == "scalar") {
			output.data[3].SetValue(i, Value("()"));
		} else if (uns_info.type == "array" && !uns_info.shape.empty()) {
			string shape_str = "(";
			for (size_t j = 0; j < uns_info.shape.size(); j++) {
				if (j > 0)
					shape_str += ", ";
				shape_str += to_string(uns_info.shape[j]);
			}
			shape_str += ")";
			output.data[3].SetValue(i, Value(shape_str));
		} else if (uns_info.type == "group" || uns_info.type == "dataframe") {
			output.data[3].SetValue(i, Value()); // NULL for groups
		} else {
			output.data[3].SetValue(i, Value());
		}

		// Value column - for scalars, show the actual value
		if (uns_info.type == "scalar") {
			if (!uns_info.value_str.empty()) {
				// String scalar - we already have the value
				output.data[4].SetValue(i, Value(uns_info.value_str));
			} else {
				// Non-string scalar - read it
				Value scalar_value = gstate.h5_reader->ReadUnsScalar(uns_info.key);
				if (!scalar_value.IsNull()) {
					output.data[4].SetValue(i, scalar_value.ToString());
				} else {
					output.data[4].SetValue(i, Value());
				}
			}
		} else {
			output.data[4].SetValue(i, Value()); // NULL for non-scalars
		}
	}

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Table function implementations for obsp (observation pairwise matrices)
unique_ptr<FunctionData> AnndataScanner::ObspBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Validate input parameters
	if (input.inputs.size() != 2) {
		throw InvalidInputException("anndata_scan_obsp requires 2 parameters: file_path and matrix_name");
	}
	
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	bind_data->is_obsp_scan = true;
	bind_data->pairwise_matrix_name = input.inputs[1].GetValue<string>();

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get matrix info
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get sparse matrix info
	try {
		H5Reader::SparseMatrixInfo info = reader.GetObspMatrixInfo(bind_data->pairwise_matrix_name);
		bind_data->nnz = info.nnz;
		bind_data->row_count = info.nnz;  // We return one row per non-zero element
	} catch (const InvalidInputException &e) {
		// Matrix not found
		bind_data->row_count = 0;
		bind_data->nnz = 0;
	}
	
	// Set up column schema
	names.push_back("obs_idx_1");
	return_types.push_back(LogicalType::BIGINT);
	
	names.push_back("obs_idx_2");
	return_types.push_back(LogicalType::BIGINT);
	
	names.push_back("value");
	return_types.push_back(LogicalType::FLOAT);

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::ObspScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	if (bind_data.nnz == 0) {
		// No data to return
		return;
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
	if (count == 0) {
		return;
	}

	// Read sparse matrix triplets
	auto &row_vec = output.data[0];
	auto &col_vec = output.data[1];
	auto &val_vec = output.data[2];
	
	gstate.h5_reader->ReadObspMatrix(bind_data.pairwise_matrix_name, row_vec, col_vec, val_vec, gstate.current_row, count);

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Table function implementations for varp (variable pairwise matrices)
unique_ptr<FunctionData> AnndataScanner::VarpBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Validate input parameters
	if (input.inputs.size() != 2) {
		throw InvalidInputException("anndata_scan_varp requires 2 parameters: file_path and matrix_name");
	}
	
	auto bind_data = make_uniq<AnndataBindData>(input.inputs[0].GetValue<string>());
	bind_data->is_varp_scan = true;
	bind_data->pairwise_matrix_name = input.inputs[1].GetValue<string>();

	if (!IsAnndataFile(bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get matrix info
	H5Reader reader(bind_data->file_path);

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get sparse matrix info
	try {
		H5Reader::SparseMatrixInfo info = reader.GetVarpMatrixInfo(bind_data->pairwise_matrix_name);
		bind_data->nnz = info.nnz;
		bind_data->row_count = info.nnz;  // We return one row per non-zero element
	} catch (const InvalidInputException &e) {
		// Matrix not found
		bind_data->row_count = 0;
		bind_data->nnz = 0;
	}
	
	// Set up column schema
	names.push_back("var_idx_1");
	return_types.push_back(LogicalType::BIGINT);
	
	names.push_back("var_idx_2");
	return_types.push_back(LogicalType::BIGINT);
	
	names.push_back("value");
	return_types.push_back(LogicalType::FLOAT);

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::VarpScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Open file on first scan
	if (!gstate.h5_reader) {
		gstate.h5_reader = make_uniq<H5Reader>(bind_data.file_path);
	}

	if (bind_data.nnz == 0) {
		// No data to return
		return;
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
	if (count == 0) {
		return;
	}

	// Read sparse matrix triplets
	auto &row_vec = output.data[0];
	auto &col_vec = output.data[1];
	auto &val_vec = output.data[2];
	
	gstate.h5_reader->ReadVarpMatrix(bind_data.pairwise_matrix_name, row_vec, col_vec, val_vec, gstate.current_row, count);

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

	// Register anndata_scan_obsm function (2 parameters - correct usage)
	TableFunction obsm_func("anndata_scan_obsm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::ObsmScan,
	                        AnndataScanner::ObsmBind, AnndataInitGlobal, AnndataInitLocal);
	obsm_func.name = "anndata_scan_obsm";
	ExtensionUtil::RegisterFunction(db, obsm_func);

	// Register anndata_scan_obsm function (1 parameter - error message)
	TableFunction obsm_func_error("anndata_scan_obsm", {LogicalType::VARCHAR}, DummyScan, ObsmBindError,
	                              AnndataInitGlobal, AnndataInitLocal);
	obsm_func_error.name = "anndata_scan_obsm";
	ExtensionUtil::RegisterFunction(db, obsm_func_error);

	// Register anndata_scan_varm function (2 parameters - correct usage)
	TableFunction varm_func("anndata_scan_varm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::VarmScan,
	                        AnndataScanner::VarmBind, AnndataInitGlobal, AnndataInitLocal);
	varm_func.name = "anndata_scan_varm";
	ExtensionUtil::RegisterFunction(db, varm_func);

	// Register anndata_scan_varm function (1 parameter - error message)
	TableFunction varm_func_error("anndata_scan_varm", {LogicalType::VARCHAR}, DummyScan, VarmBindError,
	                              AnndataInitGlobal, AnndataInitLocal);
	varm_func_error.name = "anndata_scan_varm";
	ExtensionUtil::RegisterFunction(db, varm_func_error);

	// Register anndata_scan_layers function (2 parameters)
	TableFunction layers_func("anndata_scan_layers", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          AnndataScanner::LayerScan, AnndataScanner::LayerBind, AnndataInitGlobal,
	                          AnndataInitLocal);
	layers_func.name = "anndata_scan_layers";
	ExtensionUtil::RegisterFunction(db, layers_func);

	// Register anndata_scan_layers function (3 parameters - with custom var column)
	TableFunction layers_func_custom(
	    "anndata_scan_layers", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    AnndataScanner::LayerScan, AnndataScanner::LayerBind, AnndataInitGlobal, AnndataInitLocal);
	layers_func_custom.name = "anndata_scan_layers";
	ExtensionUtil::RegisterFunction(db, layers_func_custom);

	// Register anndata_scan_layers function (1 parameter - error message)
	TableFunction layers_func_error("anndata_scan_layers", {LogicalType::VARCHAR}, DummyScan, LayerBindError,
	                                AnndataInitGlobal, AnndataInitLocal);
	layers_func_error.name = "anndata_scan_layers";
	ExtensionUtil::RegisterFunction(db, layers_func_error);

	// Register anndata_scan_uns function
	TableFunction uns_func("anndata_scan_uns", {LogicalType::VARCHAR}, AnndataScanner::UnsScan, AnndataScanner::UnsBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	uns_func.name = "anndata_scan_uns";
	ExtensionUtil::RegisterFunction(db, uns_func);

	// Register anndata_scan_obsp function
	TableFunction obsp_func("anndata_scan_obsp", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
	                        AnndataScanner::ObspScan, AnndataScanner::ObspBind,
	                        AnndataInitGlobal, AnndataInitLocal);
	obsp_func.name = "anndata_scan_obsp";
	ExtensionUtil::RegisterFunction(db, obsp_func);

	// Register anndata_scan_varp function
	TableFunction varp_func("anndata_scan_varp", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                        AnndataScanner::VarpScan, AnndataScanner::VarpBind,
	                        AnndataInitGlobal, AnndataInitLocal);
	varp_func.name = "anndata_scan_varp";
	ExtensionUtil::RegisterFunction(db, varp_func);

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
