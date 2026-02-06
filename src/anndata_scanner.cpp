#include "include/anndata_scanner.hpp"
#include "include/s3_credentials.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include <fstream>
#include <iostream>

namespace duckdb {

// Helper function to create H5ReaderMultithreaded with S3 credentials if available
static unique_ptr<H5ReaderMultithreaded> CreateH5Reader(ClientContext &context, const string &file_path) {
	H5FileCache::RemoteConfig config;
	if (GetS3ConfigFromSecrets(context, file_path, config)) {
		return make_uniq<H5ReaderMultithreaded>(file_path, &config);
	}
	return make_uniq<H5ReaderMultithreaded>(file_path);
}

bool AnndataScanner::IsAnndataFile(const string &path) {
	// Check if this is a remote URL
	bool is_remote = (path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	                  path.rfind("s3a://", 0) == 0 || path.rfind("gs://", 0) == 0);

	// For local files, check if file exists and can be opened
	if (!is_remote) {
		std::ifstream file(path);
		if (!file.good()) {
			return false;
		}
		file.close();
	}

	// Try to open as HDF5 and validate it's an AnnData file
	// This allows files without .h5ad extension to work (e.g., UUID-named files)
	try {
		H5ReaderMultithreaded reader(path);
		return reader.IsValidAnnData();
	} catch (...) {
		return false;
	}
}

bool AnndataScanner::IsAnndataFile(ClientContext &context, const string &path) {
	// Check if this is a remote URL
	bool is_remote = (path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	                  path.rfind("s3a://", 0) == 0 || path.rfind("gs://", 0) == 0);

	// For local files, check if file exists and can be opened
	if (!is_remote) {
		std::ifstream file(path);
		if (!file.good()) {
			return false;
		}
		file.close();
	}

	// Try to open as HDF5 and validate it's an AnnData file
	// For S3 URLs, get credentials from DuckDB's secret manager
	try {
		H5FileCache::RemoteConfig config;
		if (GetS3ConfigFromSecrets(context, path, config)) {
			H5ReaderMultithreaded reader(path, &config);
			return reader.IsValidAnnData();
		}
		H5ReaderMultithreaded reader(path);
		return reader.IsValidAnnData();
	} catch (...) {
		return false;
	}
}

string AnndataScanner::GetAnndataInfo(const string &path) {
	if (!IsAnndataFile(path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + path);
	}

	H5ReaderMultithreaded reader(path);
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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

// Global state initialization with projection pushdown support
// Captures column_ids from DuckDB's optimizer to only read requested columns
static unique_ptr<GlobalTableFunctionState> AnndataInitGlobalWithProjection(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	auto state = make_uniq<AnndataGlobalState>();
	state->column_ids = input.column_ids;
	return std::move(state);
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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
	}

	// Calculate how many observations to read
	idx_t remaining = bind_data.n_obs - gstate.current_row;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (count == 0) {
		return;
	}

	// Check if projection pushdown is enabled (column_ids not empty)
	if (!gstate.column_ids.empty()) {
		// Projection pushdown: only read requested columns
		// column_ids[0] is typically obs_idx (column 0), rest are gene columns
		// We need to convert column_ids to matrix column indices
		// Column 0 in table = obs_idx, Column 1+ = matrix columns 0+

		// First, handle obs_idx column (fill it if requested)
		// Then read only the requested matrix columns
		vector<idx_t> matrix_col_indices;
		idx_t output_col_idx = 0;

		for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
			idx_t col_id = gstate.column_ids[i];
			if (col_id == 0) {
				// obs_idx column - fill with row indices
				auto &obs_idx_vec = output.data[output_col_idx];
				for (idx_t row = 0; row < count; row++) {
					obs_idx_vec.SetValue(row, Value::BIGINT(gstate.current_row + row));
				}
				output_col_idx++;
			} else {
				// Gene column - map to matrix column (col_id - 1)
				matrix_col_indices.push_back(col_id - 1);
			}
		}

		if (!matrix_col_indices.empty()) {
			// Build a mapping from output column index to matrix column index
			// for columns that are gene columns (not obs_idx)
			vector<idx_t> output_col_mapping;
			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				if (gstate.column_ids[i] != 0) {
					output_col_mapping.push_back(i);
				}
			}

			// Read matrix data directly into the output vectors
			// We need to read each column individually using hyperslab selection
			for (idx_t m = 0; m < matrix_col_indices.size(); m++) {
				idx_t out_col = output_col_mapping[m];
				idx_t matrix_col = matrix_col_indices[m];

				// Initialize the output column with zeros
				auto &vec = output.data[out_col];
				for (idx_t row = 0; row < count; row++) {
					vec.SetValue(row, Value(0.0));
				}

				// Read this specific column from the matrix
				// The ReadMatrixColumns function reads into a flat output, so we call it once with all columns
			}

			// Create a temporary DataChunk for the matrix columns
			DataChunk matrix_output;
			vector<LogicalType> matrix_types;
			for (idx_t i = 0; i < matrix_col_indices.size(); i++) {
				matrix_types.push_back(LogicalType::DOUBLE);
			}
			matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

			// Read only the requested matrix columns
			gstate.h5_reader->ReadMatrixColumns("/X", gstate.current_row, count, matrix_col_indices, matrix_output,
			                                    false);

			// Copy values from matrix_output to the correct output columns
			for (idx_t m = 0; m < matrix_col_indices.size(); m++) {
				idx_t out_col = output_col_mapping[m];
				auto &src = matrix_output.data[m];
				auto &dst = output.data[out_col];

				// Copy values row by row to handle type conversion
				for (idx_t row = 0; row < count; row++) {
					dst.SetValue(row, src.GetValue(row));
				}
			}
		}

		output.SetCardinality(count);
	} else {
		// No projection pushdown - read all columns (original behavior)
		gstate.h5_reader->ReadXMatrixBatch(gstate.current_row, count, 0, bind_data.n_var, output);
	}

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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get schema
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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
	auto reader_ptr = CreateH5Reader(context, result->file_path);
	auto &reader = *reader_ptr;
	auto layers = reader.GetLayers();

	// Find the requested layer
	bool found = false;
	H5ReaderMultithreaded::LayerInfo layer_info;
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

	// Validate dimensions
	if (result->n_var == 0) {
		throw InvalidInputException("Layer '%s' has 0 variables/columns", result->layer_name.c_str());
	}

	// Get variable names - allow custom column selection
	// Default to "_index" which is standard AnnData var_names column (same as XBind)
	string var_column = "_index";
	if (input.inputs.size() > 2) {
		var_column = input.inputs[2].GetValue<string>();
	}

	// Get variable names for column headers using the same method as XBind
	result->var_names = reader.GetVarNames(var_column);

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
		state.h5_reader = CreateH5Reader(context, bind_data.file_path);
	}

	// Calculate how many rows to read
	idx_t remaining = bind_data.row_count - state.current_row;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

	if (count == 0) {
		return;
	}

	// Check if projection pushdown is enabled (column_ids not empty)
	if (!state.column_ids.empty()) {
		// Projection pushdown: only read requested columns
		// column_ids[0] is typically obs_idx (column 0), rest are gene columns
		vector<idx_t> matrix_col_indices;

		for (idx_t i = 0; i < state.column_ids.size(); i++) {
			idx_t col_id = state.column_ids[i];
			if (col_id == 0) {
				// obs_idx column - fill with row indices
				auto &obs_idx_vec = output.data[i];
				for (idx_t row = 0; row < count; row++) {
					obs_idx_vec.SetValue(row, Value::BIGINT(state.current_row + row));
				}
			} else {
				// Gene column - map to matrix column (col_id - 1)
				matrix_col_indices.push_back(col_id - 1);
			}
		}

		if (!matrix_col_indices.empty()) {
			// Build a mapping from output column index to matrix column index
			vector<idx_t> output_col_mapping;
			for (idx_t i = 0; i < state.column_ids.size(); i++) {
				if (state.column_ids[i] != 0) {
					output_col_mapping.push_back(i);
				}
			}

			// Create a temporary DataChunk for the matrix columns
			DataChunk matrix_output;
			vector<LogicalType> matrix_types;
			for (idx_t i = 0; i < matrix_col_indices.size(); i++) {
				matrix_types.push_back(LogicalType::DOUBLE);
			}
			matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

			// Read only the requested matrix columns
			string layer_path = "/layers/" + bind_data.layer_name;
			state.h5_reader->ReadMatrixColumns(layer_path, state.current_row, count, matrix_col_indices, matrix_output,
			                                   true);

			// Copy values from matrix_output to the correct output columns
			for (idx_t m = 0; m < matrix_col_indices.size(); m++) {
				idx_t out_col = output_col_mapping[m];
				auto &src = matrix_output.data[m];
				auto &dst = output.data[out_col];

				// Copy values row by row to handle type conversion
				for (idx_t row = 0; row < count; row++) {
					dst.SetValue(row, src.GetValue(row));
				}
			}
		}

		output.SetCardinality(count);
	} else {
		// No projection pushdown - read all columns (original behavior)
		state.h5_reader->ReadLayerMatrixBatch(bind_data.layer_name, state.current_row, count, 0, bind_data.n_var,
		                                      output);
	}

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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get uns keys
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

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
		// UNION type: scalar VARCHAR or arr LIST(VARCHAR)
		child_list_t<LogicalType> union_members;
		union_members.emplace_back("scalar", LogicalType::VARCHAR);
		union_members.emplace_back("arr", LogicalType::LIST(LogicalType::VARCHAR));
		return_types.push_back(LogicalType::UNION(std::move(union_members)));

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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

		// Value column - UNION type with scalar or arr variant
		// Create members list for UNION value creation
		child_list_t<LogicalType> union_members;
		union_members.emplace_back("scalar", LogicalType::VARCHAR);
		union_members.emplace_back("arr", LogicalType::LIST(LogicalType::VARCHAR));

		if (uns_info.type == "scalar") {
			// Scalar value - use the "scalar" variant of the UNION (tag 0)
			string scalar_str;
			if (!uns_info.value_str.empty()) {
				scalar_str = uns_info.value_str;
			} else {
				Value scalar_value = gstate.h5_reader->ReadUnsScalar(uns_info.key);
				if (!scalar_value.IsNull()) {
					scalar_str = scalar_value.ToString();
				}
			}
			if (!scalar_str.empty()) {
				output.data[4].SetValue(i, Value::UNION(union_members, 0, Value(scalar_str)));
			} else {
				output.data[4].SetValue(i, Value());
			}
		} else if (uns_info.type == "array" && !uns_info.array_values.empty()) {
			// Array value - use the "arr" variant of the UNION (tag 1)
			vector<Value> list_values;
			for (const auto &val : uns_info.array_values) {
				list_values.emplace_back(val);
			}
			Value list_val = Value::LIST(LogicalType::VARCHAR, std::move(list_values));
			output.data[4].SetValue(i, Value::UNION(union_members, 1, std::move(list_val)));
		} else {
			output.data[4].SetValue(i, Value()); // NULL for empty arrays or other types
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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get matrix info
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get sparse matrix info
	try {
		H5ReaderMultithreaded::SparseMatrixInfo info = reader.GetObspMatrixInfo(bind_data->pairwise_matrix_name);
		bind_data->nnz = info.nnz;
		bind_data->row_count = info.nnz; // We return one row per non-zero element
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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

	gstate.h5_reader->ReadObspMatrix(bind_data.pairwise_matrix_name, row_vec, col_vec, val_vec, gstate.current_row,
	                                 count);

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

	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// Open the HDF5 file to get matrix info
	auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
	auto &reader = *reader_ptr;

	if (!reader.IsValidAnnData()) {
		throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
	}

	// Get sparse matrix info
	try {
		H5ReaderMultithreaded::SparseMatrixInfo info = reader.GetVarpMatrixInfo(bind_data->pairwise_matrix_name);
		bind_data->nnz = info.nnz;
		bind_data->row_count = info.nnz; // We return one row per non-zero element
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
		gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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

	gstate.h5_reader->ReadVarpMatrix(bind_data.pairwise_matrix_name, row_vec, col_vec, val_vec, gstate.current_row,
	                                 count);

	gstate.current_row += count;
	output.SetCardinality(count);
}

// Table function for info
unique_ptr<FunctionData> AnndataScanner::InfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw InvalidInputException("anndata_info requires at least 1 parameter: file_path");
	}

	auto file_path = input.inputs[0].GetValue<string>();
	auto bind_data = make_uniq<AnndataBindData>(file_path);

	// Basic file extension check
	if (!IsAnndataFile(context, bind_data->file_path)) {
		throw InvalidInputException("File is not a valid AnnData file: " + bind_data->file_path);
	}

	// We'll validate the file content when we actually open it in InfoScan
	// to avoid opening the file twice
	bind_data->is_info_scan = true;

	// Define output schema for info table
	names.emplace_back("property");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	// We'll create multiple rows with different properties
	bind_data->row_count = 10; // Approximate number of info rows

	return std::move(bind_data);
}

void AnndataScanner::InfoScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	// Initialize H5Reader if not already done
	if (!gstate.h5_reader) {
		try {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		} catch (const std::exception &e) {
			throw InvalidInputException("Failed to open AnnData file '%s': %s", bind_data.file_path.c_str(), e.what());
		} catch (...) {
			throw InvalidInputException("Failed to open AnnData file '%s'", bind_data.file_path.c_str());
		}
	}

	output.SetCardinality(0);
	idx_t result_idx = 0;

	// Prepare output vectors
	auto &property_vec = output.data[0];
	auto &value_vec = output.data[1];

	// Collect all info in one scan
	if (gstate.current_row == 0) {
		vector<pair<string, string>> info_rows;

		// Basic file info
		info_rows.emplace_back("file_path", bind_data.file_path);
		info_rows.emplace_back("n_obs", to_string(gstate.h5_reader->GetObsCount()));
		info_rows.emplace_back("n_vars", to_string(gstate.h5_reader->GetVarCount()));

		// X matrix info
		auto x_info = gstate.h5_reader->GetXMatrixInfo();
		info_rows.emplace_back("x_shape", to_string(x_info.n_obs) + " x " + to_string(x_info.n_var));
		info_rows.emplace_back("x_sparse", x_info.is_sparse ? "true" : "false");
		if (x_info.is_sparse) {
			info_rows.emplace_back("x_format", x_info.sparse_format);
		}

		// Count obsm matrices
		auto obsm_matrices = gstate.h5_reader->GetObsmMatrices();
		if (!obsm_matrices.empty()) {
			string obsm_list;
			for (size_t i = 0; i < obsm_matrices.size(); ++i) {
				if (i > 0)
					obsm_list += ", ";
				obsm_list += obsm_matrices[i].name;
			}
			info_rows.emplace_back("obsm_keys", obsm_list);
		}

		// Count varm matrices
		auto varm_matrices = gstate.h5_reader->GetVarmMatrices();
		if (!varm_matrices.empty()) {
			string varm_list;
			for (size_t i = 0; i < varm_matrices.size(); ++i) {
				if (i > 0)
					varm_list += ", ";
				varm_list += varm_matrices[i].name;
			}
			info_rows.emplace_back("varm_keys", varm_list);
		}

		// Count layers
		auto layers = gstate.h5_reader->GetLayers();
		if (!layers.empty()) {
			string layer_list;
			for (size_t i = 0; i < layers.size(); ++i) {
				if (i > 0)
					layer_list += ", ";
				layer_list += layers[i].name;
			}
			info_rows.emplace_back("layers", layer_list);
		}

		// Count obsp/varp
		auto obsp_keys = gstate.h5_reader->GetObspKeys();
		if (!obsp_keys.empty()) {
			string obsp_list;
			for (size_t i = 0; i < obsp_keys.size(); ++i) {
				if (i > 0)
					obsp_list += ", ";
				obsp_list += obsp_keys[i];
			}
			info_rows.emplace_back("obsp_keys", obsp_list);
		}

		auto varp_keys = gstate.h5_reader->GetVarpKeys();
		if (!varp_keys.empty()) {
			string varp_list;
			for (size_t i = 0; i < varp_keys.size(); ++i) {
				if (i > 0)
					varp_list += ", ";
				varp_list += varp_keys[i];
			}
			info_rows.emplace_back("varp_keys", varp_list);
		}

		// Check for uns data
		auto uns_keys = gstate.h5_reader->GetUnsKeys();

		// Build list of available groups (HDF5 top-level groups present in the file)
		{
			string groups_list;
			// obs and var are always present in valid AnnData
			groups_list = "obs, var";
			if (x_info.n_obs > 0 && x_info.n_var > 0) {
				groups_list += ", X";
			}
			if (!obsm_matrices.empty()) {
				groups_list += ", obsm";
			}
			if (!varm_matrices.empty()) {
				groups_list += ", varm";
			}
			if (!layers.empty()) {
				groups_list += ", layers";
			}
			if (!obsp_keys.empty()) {
				groups_list += ", obsp";
			}
			if (!varp_keys.empty()) {
				groups_list += ", varp";
			}
			if (!uns_keys.empty()) {
				groups_list += ", uns";
			}
			info_rows.emplace_back("groups", groups_list);
		}

		// Build list of available tables (SQL-accessible views)
		{
			string tables_list = "obs, var, info";
			if (x_info.n_obs > 0 && x_info.n_var > 0) {
				tables_list += ", X";
			}
			for (const auto &m : obsm_matrices) {
				tables_list += ", obsm_" + m.name;
			}
			for (const auto &m : varm_matrices) {
				tables_list += ", varm_" + m.name;
			}
			for (const auto &l : layers) {
				tables_list += ", layers_" + l.name;
			}
			for (const auto &k : obsp_keys) {
				tables_list += ", obsp_" + k;
			}
			for (const auto &k : varp_keys) {
				tables_list += ", varp_" + k;
			}
			if (!uns_keys.empty()) {
				tables_list += ", uns";
			}
			info_rows.emplace_back("tables", tables_list);
		}

		// Detect var columns (gene index)
		auto var_detection = gstate.h5_reader->DetectVarColumns();
		if (!var_detection.name_column.empty()) {
			info_rows.emplace_back("var_name_column", var_detection.name_column);
		}
		if (!var_detection.id_column.empty()) {
			info_rows.emplace_back("var_id_column", var_detection.id_column);
		}

		// Output rows
		for (const auto &row : info_rows) {
			if (result_idx >= STANDARD_VECTOR_SIZE) {
				break;
			}
			FlatVector::GetData<string_t>(property_vec)[result_idx] = StringVector::AddString(property_vec, row.first);
			FlatVector::GetData<string_t>(value_vec)[result_idx] = StringVector::AddString(value_vec, row.second);
			result_idx++;
		}

		gstate.current_row = info_rows.size();
	}

	output.SetCardinality(result_idx);

	// If we've output all rows, we're done
	if (result_idx == 0) {
		output.SetCardinality(0);
	}
}

// Register the table functions
void RegisterAnndataTableFunctions(ExtensionLoader &loader) {
	// Register anndata_scan_obs function
	TableFunction obs_func("anndata_scan_obs", {LogicalType::VARCHAR}, AnndataScanner::ObsScan, AnndataScanner::ObsBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	obs_func.name = "anndata_scan_obs";
	loader.RegisterFunction(obs_func);

	// Register anndata_scan_var function
	TableFunction var_func("anndata_scan_var", {LogicalType::VARCHAR}, AnndataScanner::VarScan, AnndataScanner::VarBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	var_func.name = "anndata_scan_var";
	loader.RegisterFunction(var_func);

	// Register anndata_scan_x function with projection pushdown enabled
	TableFunction x_func("anndata_scan_x", {LogicalType::VARCHAR}, AnndataScanner::XScan, AnndataScanner::XBind,
	                     AnndataInitGlobalWithProjection, AnndataInitLocal);
	x_func.name = "anndata_scan_x";
	x_func.projection_pushdown = true;
	loader.RegisterFunction(x_func);

	// Also register with optional var_name_column parameter
	TableFunction x_func_with_param("anndata_scan_x", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AnndataScanner::XScan, AnndataScanner::XBind, AnndataInitGlobalWithProjection,
	                                AnndataInitLocal);
	x_func_with_param.name = "anndata_scan_x";
	x_func_with_param.projection_pushdown = true;
	loader.RegisterFunction(x_func_with_param);

	// Register anndata_scan_obsm function (2 parameters - correct usage)
	TableFunction obsm_func("anndata_scan_obsm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::ObsmScan,
	                        AnndataScanner::ObsmBind, AnndataInitGlobal, AnndataInitLocal);
	obsm_func.name = "anndata_scan_obsm";
	loader.RegisterFunction(obsm_func);

	// Register anndata_scan_obsm function (1 parameter - error message)
	TableFunction obsm_func_error("anndata_scan_obsm", {LogicalType::VARCHAR}, DummyScan, ObsmBindError,
	                              AnndataInitGlobal, AnndataInitLocal);
	obsm_func_error.name = "anndata_scan_obsm";
	loader.RegisterFunction(obsm_func_error);

	// Register anndata_scan_varm function (2 parameters - correct usage)
	TableFunction varm_func("anndata_scan_varm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::VarmScan,
	                        AnndataScanner::VarmBind, AnndataInitGlobal, AnndataInitLocal);
	varm_func.name = "anndata_scan_varm";
	loader.RegisterFunction(varm_func);

	// Register anndata_scan_varm function (1 parameter - error message)
	TableFunction varm_func_error("anndata_scan_varm", {LogicalType::VARCHAR}, DummyScan, VarmBindError,
	                              AnndataInitGlobal, AnndataInitLocal);
	varm_func_error.name = "anndata_scan_varm";
	loader.RegisterFunction(varm_func_error);

	// Register anndata_scan_layers function (2 parameters) with projection pushdown enabled
	TableFunction layers_func("anndata_scan_layers", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          AnndataScanner::LayerScan, AnndataScanner::LayerBind, AnndataInitGlobalWithProjection,
	                          AnndataInitLocal);
	layers_func.name = "anndata_scan_layers";
	layers_func.projection_pushdown = true;
	loader.RegisterFunction(layers_func);

	// Register anndata_scan_layers function (3 parameters - with custom var column)
	TableFunction layers_func_custom(
	    "anndata_scan_layers", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    AnndataScanner::LayerScan, AnndataScanner::LayerBind, AnndataInitGlobalWithProjection, AnndataInitLocal);
	layers_func_custom.name = "anndata_scan_layers";
	layers_func_custom.projection_pushdown = true;
	loader.RegisterFunction(layers_func_custom);

	// Register anndata_scan_layers function (1 parameter - error message)
	TableFunction layers_func_error("anndata_scan_layers", {LogicalType::VARCHAR}, DummyScan, LayerBindError,
	                                AnndataInitGlobal, AnndataInitLocal);
	layers_func_error.name = "anndata_scan_layers";
	loader.RegisterFunction(layers_func_error);

	// Register anndata_scan_uns function
	TableFunction uns_func("anndata_scan_uns", {LogicalType::VARCHAR}, AnndataScanner::UnsScan, AnndataScanner::UnsBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	uns_func.name = "anndata_scan_uns";
	loader.RegisterFunction(uns_func);

	// Register anndata_scan_obsp function
	TableFunction obsp_func("anndata_scan_obsp", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::ObspScan,
	                        AnndataScanner::ObspBind, AnndataInitGlobal, AnndataInitLocal);
	obsp_func.name = "anndata_scan_obsp";
	loader.RegisterFunction(obsp_func);

	// Register anndata_scan_varp function
	TableFunction varp_func("anndata_scan_varp", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::VarpScan,
	                        AnndataScanner::VarpBind, AnndataInitGlobal, AnndataInitLocal);
	varp_func.name = "anndata_scan_varp";
	loader.RegisterFunction(varp_func);

	// Register anndata_info function (table function)
	TableFunction info_func("anndata_info", {LogicalType::VARCHAR}, AnndataScanner::InfoScan, AnndataScanner::InfoBind,
	                        AnndataInitGlobal, AnndataInitLocal);
	info_func.name = "anndata_info";
	loader.RegisterFunction(info_func);
}

} // namespace duckdb
