#include "include/anndata_scanner.hpp"
#include "include/s3_credentials.hpp"
#include "include/glob_handler.hpp"
#include "include/schema_harmonizer.hpp"
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

// Helper to parse schema_mode from named parameters
static SchemaMode ParseSchemaMode(TableFunctionBindInput &input) {
	auto it = input.named_parameters.find("schema_mode");
	if (it != input.named_parameters.end()) {
		string mode_str = StringUtil::Lower(it->second.GetValue<string>());
		if (mode_str == "union") {
			return SchemaMode::UNION;
		} else if (mode_str == "intersection") {
			return SchemaMode::INTERSECTION;
		} else {
			throw InvalidInputException("Invalid schema_mode: '" + mode_str + "'. Use 'intersection' or 'union'.");
		}
	}
	return SchemaMode::INTERSECTION; // Default
}

// Implementation of AnndataGlobalState methods for multi-file support
bool AnndataGlobalState::AdvanceToNextFile(ClientContext &context, const AnndataBindData &bind_data) {
	if (!bind_data.is_multi_file) {
		return false;
	}

	current_file_idx++;
	if (current_file_idx >= bind_data.file_paths.size()) {
		return false;
	}

	// Reset file-specific state
	current_row_in_file = 0;
	h5_reader.reset();

	// Open the new file
	OpenCurrentFile(context, bind_data);
	return true;
}

void AnndataGlobalState::OpenCurrentFile(ClientContext &context, const AnndataBindData &bind_data) {
	if (!bind_data.is_multi_file) {
		current_file_name = GlobHandler::GetBaseName(bind_data.file_path);
		h5_reader = CreateH5Reader(context, bind_data.file_path);
		return;
	}

	if (current_file_idx >= bind_data.file_paths.size()) {
		return;
	}

	const string &file_path = bind_data.file_paths[current_file_idx];
	current_file_name = GlobHandler::GetBaseName(file_path);
	h5_reader = CreateH5Reader(context, file_path);

	// Set up column mapping for this file
	if (current_file_idx < bind_data.harmonized_schema.file_column_mappings.size()) {
		current_column_mapping = bind_data.harmonized_schema.file_column_mappings[current_file_idx];
	}

	// Set up per-file original names for this file
	if (current_file_idx < bind_data.harmonized_schema.file_original_names.size()) {
		current_original_names = bind_data.harmonized_schema.file_original_names[current_file_idx];
	}

	// Set up var mapping for this file (for X/layers)
	if (current_file_idx < bind_data.harmonized_schema.file_var_mappings.size()) {
		current_var_mapping = bind_data.harmonized_schema.file_var_mappings[current_file_idx];
	}
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
	string file_pattern = input.inputs[0].GetValue<string>();
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	// Validate files
	for (const auto &file_path : glob_result.matched_files) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}
	}

	auto bind_data = make_uniq<AnndataBindData>(glob_result.matched_files, file_pattern);
	bind_data->schema_mode = schema_mode;

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode - use original logic for backward compatibility
		bind_data->is_multi_file = false;

		auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
		auto &reader = *reader_ptr;

		if (!reader.IsValidAnnData()) {
			throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
		}

		auto columns = reader.GetObsColumns();
		vector<string> original_names_vec;
		for (const auto &col : columns) {
			names.push_back(col.name);
			original_names_vec.push_back(col.original_name);
			return_types.push_back(col.type);
		}

		bind_data->row_count = reader.GetObsCount();
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->original_names = original_names_vec;
		bind_data->column_types = return_types;
	} else {
		// Multi-file mode
		bind_data->is_multi_file = true;

		// Collect schemas from all files
		vector<FileSchema> file_schemas;
		for (const auto &file_path : glob_result.matched_files) {
			file_schemas.push_back(SchemaHarmonizer::GetObsSchema(context, file_path));
		}

		// Compute harmonized schema
		bind_data->harmonized_schema = SchemaHarmonizer::ComputeObsVarSchema(file_schemas, schema_mode);

		// Add _file_name column first
		names.push_back("_file_name");
		return_types.push_back(LogicalType::VARCHAR);

		// Add harmonized columns
		vector<string> original_names_vec;
		original_names_vec.push_back("_file_name"); // Pseudo-column
		for (const auto &col : bind_data->harmonized_schema.columns) {
			names.push_back(col.name);
			original_names_vec.push_back(col.original_name);
			return_types.push_back(col.type);
		}

		bind_data->row_count = bind_data->harmonized_schema.total_row_count;
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->original_names = original_names_vec;
		bind_data->column_types = return_types;
	}

	return std::move(bind_data);
}

void AnndataScanner::ObsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (!bind_data.is_multi_file) {
		// Single file mode - original logic
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
		if (count == 0) {
			return;
		}

		for (idx_t col = 0; col < bind_data.column_count; col++) {
			auto &vec = output.data[col];
			gstate.h5_reader->ReadObsColumn(bind_data.original_names[col], vec, gstate.current_row, count);
		}

		gstate.current_row += count;
		output.SetCardinality(count);
		return;
	}

	// Multi-file mode
	if (!gstate.h5_reader) {
		gstate.OpenCurrentFile(context, bind_data);
	}

	// Check if we've exhausted all files
	if (gstate.current_file_idx >= bind_data.file_paths.size()) {
		output.SetCardinality(0);
		return;
	}

	// Get rows remaining in current file
	idx_t file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
	idx_t rows_remaining_in_file = file_row_count - gstate.current_row_in_file;

	// If current file is exhausted, move to next
	while (rows_remaining_in_file == 0) {
		if (!gstate.AdvanceToNextFile(context, bind_data)) {
			output.SetCardinality(0);
			return;
		}
		file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		rows_remaining_in_file = file_row_count - gstate.current_row_in_file;
	}

	idx_t rows_to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows_remaining_in_file);

	// Read _file_name column (column 0 in multi-file mode)
	auto &file_name_vec = output.data[0];
	for (idx_t i = 0; i < rows_to_read; i++) {
		FlatVector::GetData<string_t>(file_name_vec)[i] =
		    StringVector::AddString(file_name_vec, gstate.current_file_name);
	}

	// Read data columns with mapping
	for (idx_t col = 0; col < bind_data.harmonized_schema.columns.size(); col++) {
		idx_t output_col = col + 1; // +1 for _file_name
		auto &vec = output.data[output_col];

		int file_col_idx = gstate.current_column_mapping[col];
		if (file_col_idx >= 0) {
			// Column exists in this file - use per-file original name
			gstate.h5_reader->ReadObsColumn(gstate.current_original_names[col], vec, gstate.current_row_in_file,
			                                rows_to_read);
		} else {
			// Column doesn't exist - fill with NULL (union mode)
			auto &validity = FlatVector::Validity(vec);
			for (idx_t i = 0; i < rows_to_read; i++) {
				validity.SetInvalid(i);
			}
		}
	}

	gstate.current_row_in_file += rows_to_read;
	gstate.current_row += rows_to_read;
	output.SetCardinality(rows_to_read);
}

// Table function implementations for .var table
unique_ptr<FunctionData> AnndataScanner::VarBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	string file_pattern = input.inputs[0].GetValue<string>();
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	// Validate files
	for (const auto &file_path : glob_result.matched_files) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}
	}

	auto bind_data = make_uniq<AnndataBindData>(glob_result.matched_files, file_pattern);
	bind_data->schema_mode = schema_mode;

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode
		bind_data->is_multi_file = false;

		auto reader_ptr = CreateH5Reader(context, bind_data->file_path);
		auto &reader = *reader_ptr;

		if (!reader.IsValidAnnData()) {
			throw InvalidInputException("File is not a valid AnnData format: " + bind_data->file_path);
		}

		auto columns = reader.GetVarColumns();
		vector<string> original_names_vec;
		for (const auto &col : columns) {
			names.push_back(col.name);
			original_names_vec.push_back(col.original_name);
			return_types.push_back(col.type);
		}

		bind_data->row_count = reader.GetVarCount();
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->original_names = original_names_vec;
		bind_data->column_types = return_types;
	} else {
		// Multi-file mode
		bind_data->is_multi_file = true;

		// Collect schemas from all files
		vector<FileSchema> file_schemas;
		for (const auto &file_path : glob_result.matched_files) {
			file_schemas.push_back(SchemaHarmonizer::GetVarSchema(context, file_path));
		}

		// Compute harmonized schema
		bind_data->harmonized_schema = SchemaHarmonizer::ComputeObsVarSchema(file_schemas, schema_mode);

		// Add _file_name column first
		names.push_back("_file_name");
		return_types.push_back(LogicalType::VARCHAR);

		// Add harmonized columns
		vector<string> original_names_vec;
		original_names_vec.push_back("_file_name");
		for (const auto &col : bind_data->harmonized_schema.columns) {
			names.push_back(col.name);
			original_names_vec.push_back(col.original_name);
			return_types.push_back(col.type);
		}

		bind_data->row_count = bind_data->harmonized_schema.total_row_count;
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->original_names = original_names_vec;
		bind_data->column_types = return_types;
	}

	return std::move(bind_data);
}

void AnndataScanner::VarScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (!bind_data.is_multi_file) {
		// Single file mode - original logic
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
		if (count == 0) {
			return;
		}

		for (idx_t col = 0; col < bind_data.column_count; col++) {
			auto &vec = output.data[col];
			gstate.h5_reader->ReadVarColumn(bind_data.original_names[col], vec, gstate.current_row, count);
		}

		gstate.current_row += count;
		output.SetCardinality(count);
		return;
	}

	// Multi-file mode
	if (!gstate.h5_reader) {
		gstate.OpenCurrentFile(context, bind_data);
	}

	// Check if we've exhausted all files
	if (gstate.current_file_idx >= bind_data.file_paths.size()) {
		output.SetCardinality(0);
		return;
	}

	// Get rows remaining in current file
	idx_t file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
	idx_t rows_remaining_in_file = file_row_count - gstate.current_row_in_file;

	// If current file is exhausted, move to next
	while (rows_remaining_in_file == 0) {
		if (!gstate.AdvanceToNextFile(context, bind_data)) {
			output.SetCardinality(0);
			return;
		}
		file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		rows_remaining_in_file = file_row_count - gstate.current_row_in_file;
	}

	idx_t rows_to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows_remaining_in_file);

	// Read _file_name column (column 0)
	auto &file_name_vec = output.data[0];
	for (idx_t i = 0; i < rows_to_read; i++) {
		FlatVector::GetData<string_t>(file_name_vec)[i] =
		    StringVector::AddString(file_name_vec, gstate.current_file_name);
	}

	// Read data columns with mapping
	for (idx_t col = 0; col < bind_data.harmonized_schema.columns.size(); col++) {
		idx_t output_col = col + 1; // +1 for _file_name
		auto &vec = output.data[output_col];

		int file_col_idx = gstate.current_column_mapping[col];
		if (file_col_idx >= 0) {
			// Column exists in this file - use per-file original name
			gstate.h5_reader->ReadVarColumn(gstate.current_original_names[col], vec, gstate.current_row_in_file,
			                                rows_to_read);
		} else {
			// Column doesn't exist - fill with NULL (union mode)
			auto &validity = FlatVector::Validity(vec);
			for (idx_t i = 0; i < rows_to_read; i++) {
				validity.SetInvalid(i);
			}
		}
	}

	gstate.current_row_in_file += rows_to_read;
	gstate.current_row += rows_to_read;
	output.SetCardinality(rows_to_read);
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
	string file_pattern = input.inputs[0].GetValue<string>();
	string var_name_column = "_index"; // Default

	// Check for optional var_name_column parameter
	if (input.inputs.size() > 1) {
		var_name_column = input.inputs[1].GetValue<string>();
	}

	// Check for schema_mode parameter
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	if (glob_result.matched_files.empty()) {
		throw InvalidInputException("No files found matching pattern: " + file_pattern);
	}

	// Create bind data with multi-file support
	auto bind_data = make_uniq<AnndataBindData>(glob_result.matched_files, glob_result.is_pattern ? file_pattern : "");
	bind_data->var_name_column = var_name_column;
	bind_data->is_x_scan = true;
	bind_data->schema_mode = schema_mode;

	// Validate files and collect X schema info
	vector<FileSchema> file_schemas;
	for (const auto &file_path : glob_result.matched_files) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}
		file_schemas.push_back(SchemaHarmonizer::GetXSchema(context, file_path, var_name_column));
	}

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode
		bind_data->is_multi_file = false;
		bind_data->n_obs = file_schemas[0].n_obs;
		bind_data->n_var = file_schemas[0].n_var;
		bind_data->var_names = file_schemas[0].var_names;
		bind_data->row_count = bind_data->n_obs;

		// Set up columns: obs_idx + one column per gene
		names.emplace_back("obs_idx");
		return_types.emplace_back(LogicalType::BIGINT);

		for (size_t i = 0; i < bind_data->n_var && i < bind_data->var_names.size(); i++) {
			names.emplace_back(bind_data->var_names[i]);
			return_types.emplace_back(LogicalType::DOUBLE);
		}
	} else {
		// Multi-file mode - compute harmonized schema
		bind_data->is_multi_file = true;

		// Compute X schema with var name intersection/union
		bind_data->harmonized_schema = SchemaHarmonizer::ComputeXSchema(file_schemas, schema_mode, {});

		// Total row count across all files
		bind_data->row_count = bind_data->harmonized_schema.total_row_count;
		bind_data->n_obs = bind_data->harmonized_schema.total_row_count;
		bind_data->n_var = bind_data->harmonized_schema.common_var_names.size();
		bind_data->var_names = bind_data->harmonized_schema.common_var_names;

		// Set up columns: _file_name, obs_idx + one column per common gene
		names.emplace_back("_file_name");
		return_types.emplace_back(LogicalType::VARCHAR);

		names.emplace_back("obs_idx");
		return_types.emplace_back(LogicalType::BIGINT);

		for (const auto &var_name : bind_data->harmonized_schema.common_var_names) {
			names.emplace_back(var_name);
			return_types.emplace_back(LogicalType::DOUBLE);
		}
	}

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::XScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (bind_data.is_multi_file) {
		// Multi-file X scan with projection pushdown
		// Open first file if not already open
		if (!gstate.h5_reader) {
			gstate.OpenCurrentFile(context, bind_data);
			// Set up var mapping for current file
			gstate.current_var_mapping = bind_data.harmonized_schema.file_var_mappings[gstate.current_file_idx];
		}

		// Read from current file
		idx_t current_file_obs = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		idx_t remaining_in_file = current_file_obs - gstate.current_row_in_file;
		idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);

		if (to_read == 0) {
			// Try to advance to next file
			if (!gstate.AdvanceToNextFile(context, bind_data)) {
				return; // All files exhausted
			}
			// Update var mapping for new file
			gstate.current_var_mapping = bind_data.harmonized_schema.file_var_mappings[gstate.current_file_idx];
			current_file_obs = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
			remaining_in_file = current_file_obs - gstate.current_row_in_file;
			to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);
		}

		if (to_read == 0) {
			return;
		}

		idx_t count = to_read;

		// With projection pushdown, column_ids maps output positions to bind-time column indices
		// Column layout at bind time: 0=_file_name, 1=obs_idx, 2+=gene columns
		// When projection pushdown is active, output.data only has column_ids.size() columns
		bool has_projection = !gstate.column_ids.empty();

		if (has_projection) {
			// Build a map from bind-time column index to output position
			vector<idx_t> file_var_indices;
			vector<idx_t> output_gene_cols; // output positions for gene columns

			for (idx_t out_idx = 0; out_idx < gstate.column_ids.size(); out_idx++) {
				idx_t col_id = gstate.column_ids[out_idx];
				if (col_id == 0) {
					// _file_name column
					auto file_name_data = FlatVector::GetData<string_t>(output.data[out_idx]);
					for (idx_t i = 0; i < count; i++) {
						file_name_data[i] = StringVector::AddString(output.data[out_idx], gstate.current_file_name);
					}
				} else if (col_id == 1) {
					// obs_idx column
					auto obs_idx_data = FlatVector::GetData<int64_t>(output.data[out_idx]);
					for (idx_t i = 0; i < count; i++) {
						obs_idx_data[i] = static_cast<int64_t>(gstate.current_row_in_file + i);
					}
				} else {
					// Gene column: col_id - 2 is the index into common_var_names
					idx_t var_idx = col_id - 2;
					if (var_idx < gstate.current_var_mapping.size()) {
						idx_t file_col = gstate.current_var_mapping[var_idx];
						if (file_col != DConstants::INVALID_INDEX) {
							file_var_indices.push_back(file_col);
							output_gene_cols.push_back(out_idx);
						} else {
							// Union mode: column not in this file, set NULL
							auto &validity = FlatVector::Validity(output.data[out_idx]);
							for (idx_t i = 0; i < count; i++) {
								validity.SetInvalid(i);
							}
						}
					}
				}
			}

			if (!file_var_indices.empty()) {
				DataChunk matrix_output;
				vector<LogicalType> matrix_types(file_var_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				gstate.h5_reader->ReadMatrixColumns("/X", gstate.current_row_in_file, count, file_var_indices,
				                                    matrix_output, false);

				for (idx_t m = 0; m < file_var_indices.size(); m++) {
					auto &src = matrix_output.data[m];
					auto &dst = output.data[output_gene_cols[m]];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}
		} else {
			// No projection pushdown - write all columns
			idx_t col_offset = 0;

			// Fill _file_name column
			auto file_name_data = FlatVector::GetData<string_t>(output.data[0]);
			for (idx_t i = 0; i < count; i++) {
				file_name_data[i] = StringVector::AddString(output.data[0], gstate.current_file_name);
			}
			col_offset = 1;

			// Fill obs_idx column
			auto obs_idx_data = FlatVector::GetData<int64_t>(output.data[col_offset]);
			for (idx_t i = 0; i < count; i++) {
				obs_idx_data[i] = static_cast<int64_t>(gstate.current_row_in_file + i);
			}
			col_offset++;

			// Read all gene columns using var mapping
			vector<idx_t> file_var_indices;
			vector<idx_t> output_var_cols;
			for (idx_t v = 0; v < gstate.current_var_mapping.size(); v++) {
				idx_t file_idx = gstate.current_var_mapping[v];
				if (file_idx != DConstants::INVALID_INDEX) {
					file_var_indices.push_back(file_idx);
					output_var_cols.push_back(col_offset + v);
				}
			}

			if (!file_var_indices.empty()) {
				DataChunk matrix_output;
				vector<LogicalType> matrix_types(file_var_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				gstate.h5_reader->ReadMatrixColumns("/X", gstate.current_row_in_file, count, file_var_indices,
				                                    matrix_output, false);

				for (idx_t m = 0; m < file_var_indices.size(); m++) {
					auto &src = matrix_output.data[m];
					auto &dst = output.data[output_var_cols[m]];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}

			// For columns not in this file (union mode), set to NULL
			for (idx_t v = 0; v < gstate.current_var_mapping.size(); v++) {
				if (gstate.current_var_mapping[v] == DConstants::INVALID_INDEX) {
					auto &vec = output.data[col_offset + v];
					auto &validity = FlatVector::Validity(vec);
					for (idx_t row = 0; row < count; row++) {
						validity.SetInvalid(row);
					}
				}
			}
		}

		gstate.current_row_in_file += count;
		gstate.current_row += count;
		output.SetCardinality(count);
	} else {
		// Single file X scan
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t remaining = bind_data.n_obs - gstate.current_row;
		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

		if (count == 0) {
			return;
		}

		// Check if projection pushdown is enabled (column_ids not empty)
		if (!gstate.column_ids.empty()) {
			// Projection pushdown: only read requested columns
			vector<idx_t> matrix_col_indices;
			idx_t output_col_idx = 0;

			for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
				idx_t col_id = gstate.column_ids[i];
				if (col_id == 0) {
					// obs_idx column - fill with row indices
					auto &obs_idx_vec = output.data[output_col_idx];
					for (idx_t row = 0; row < count; row++) {
						obs_idx_vec.SetValue(row, Value::BIGINT(static_cast<int64_t>(gstate.current_row + row)));
					}
					output_col_idx++;
				} else {
					// Gene column - map to matrix column (col_id - 1)
					matrix_col_indices.push_back(col_id - 1);
				}
			}

			if (!matrix_col_indices.empty()) {
				vector<idx_t> output_col_mapping;
				for (idx_t i = 0; i < gstate.column_ids.size(); i++) {
					if (gstate.column_ids[i] != 0) {
						output_col_mapping.push_back(i);
					}
				}

				DataChunk matrix_output;
				vector<LogicalType> matrix_types(matrix_col_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				gstate.h5_reader->ReadMatrixColumns("/X", gstate.current_row, count, matrix_col_indices, matrix_output,
				                                    false);

				for (idx_t m = 0; m < matrix_col_indices.size(); m++) {
					idx_t out_col = output_col_mapping[m];
					auto &src = matrix_output.data[m];
					auto &dst = output.data[out_col];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}

			output.SetCardinality(count);
		} else {
			// No projection pushdown - read all columns
			gstate.h5_reader->ReadXMatrixBatch(gstate.current_row, count, 0, bind_data.n_var, output);
		}

		gstate.current_row += count;
	}
}

// Table function implementations for obsm matrices
unique_ptr<FunctionData> AnndataScanner::ObsmBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Require file path and matrix name
	if (input.inputs.size() < 2) {
		throw InvalidInputException("anndata_scan_obsm requires file path and matrix name");
	}

	string file_pattern = input.inputs[0].GetValue<string>();
	string matrix_name = input.inputs[1].GetValue<string>();
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	// Validate files and collect matrix info
	vector<FileSchema> file_schemas;
	for (const auto &file_path : glob_result.matched_files) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}
		file_schemas.push_back(SchemaHarmonizer::GetObsmSchema(context, file_path, matrix_name));
	}

	auto bind_data = make_uniq<AnndataBindData>(glob_result.matched_files, file_pattern);
	bind_data->obsm_varm_matrix_name = matrix_name;
	bind_data->is_obsm_scan = true;
	bind_data->schema_mode = schema_mode;

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode
		bind_data->is_multi_file = false;
		bind_data->matrix_rows = file_schemas[0].n_obs;
		bind_data->matrix_cols = file_schemas[0].n_var;
		bind_data->row_count = file_schemas[0].n_obs;

		// First column is obs_idx
		names.push_back("obs_idx");
		return_types.push_back(LogicalType::BIGINT);

		// Add columns for each dimension, preserving the matrix's dtype
		LogicalType matrix_dtype = file_schemas[0].matrix_dtype;
		for (idx_t i = 0; i < bind_data->matrix_cols; i++) {
			names.push_back(matrix_name + "_" + to_string(i));
			return_types.push_back(matrix_dtype);
		}

		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->column_types = return_types;
	} else {
		// Multi-file mode
		bind_data->is_multi_file = true;

		// Compute harmonized dimensions
		idx_t min_cols = file_schemas[0].n_var;
		idx_t max_cols = file_schemas[0].n_var;
		idx_t total_rows = 0;

		for (const auto &fs : file_schemas) {
			min_cols = MinValue(min_cols, fs.n_var);
			max_cols = MaxValue(max_cols, fs.n_var);
			total_rows += fs.n_obs;
		}

		idx_t result_cols = (schema_mode == SchemaMode::INTERSECTION) ? min_cols : max_cols;
		bind_data->matrix_cols = result_cols;

		// Build schema
		names.push_back("_file_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("obs_idx");
		return_types.push_back(LogicalType::BIGINT);

		for (idx_t i = 0; i < result_cols; i++) {
			names.push_back(matrix_name + "_" + to_string(i));
			return_types.push_back(LogicalType::DOUBLE);
		}

		// Set up harmonized schema for row counts
		bind_data->harmonized_schema.total_row_count = total_rows;
		for (const auto &fs : file_schemas) {
			bind_data->harmonized_schema.file_row_counts.push_back(fs.n_obs);
			// Store columns per file in column_mappings (reusing the structure)
			vector<int> mapping;
			mapping.push_back(static_cast<int>(fs.n_var)); // Store actual cols in first element
			bind_data->harmonized_schema.file_column_mappings.push_back(mapping);
		}

		bind_data->row_count = total_rows;
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->column_types = return_types;
	}

	return std::move(bind_data);
}

void AnndataScanner::ObsmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (!bind_data.is_multi_file) {
		// Single file mode - original logic
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
		if (count == 0) {
			return;
		}

		auto &obs_idx_vec = output.data[0];
		for (idx_t i = 0; i < count; i++) {
			obs_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row + i));
		}

		for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
			auto &vec = output.data[col + 1];
			gstate.h5_reader->ReadObsmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row, count, col, vec);
		}

		gstate.current_row += count;
		output.SetCardinality(count);
		return;
	}

	// Multi-file mode
	if (!gstate.h5_reader) {
		gstate.OpenCurrentFile(context, bind_data);
	}

	if (gstate.current_file_idx >= bind_data.file_paths.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
	idx_t rows_remaining = file_row_count - gstate.current_row_in_file;

	while (rows_remaining == 0) {
		if (!gstate.AdvanceToNextFile(context, bind_data)) {
			output.SetCardinality(0);
			return;
		}
		file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		rows_remaining = file_row_count - gstate.current_row_in_file;
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows_remaining);

	// Get actual columns in this file
	idx_t file_cols = static_cast<idx_t>(bind_data.harmonized_schema.file_column_mappings[gstate.current_file_idx][0]);

	// Column 0: _file_name
	auto &file_name_vec = output.data[0];
	for (idx_t i = 0; i < count; i++) {
		FlatVector::GetData<string_t>(file_name_vec)[i] =
		    StringVector::AddString(file_name_vec, gstate.current_file_name);
	}

	// Column 1: obs_idx
	auto &obs_idx_vec = output.data[1];
	for (idx_t i = 0; i < count; i++) {
		obs_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row_in_file + i));
	}

	// Dimension columns
	for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
		auto &vec = output.data[col + 2]; // +2 for _file_name and obs_idx
		if (col < file_cols) {
			gstate.h5_reader->ReadObsmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row_in_file, count, col,
			                                 vec);
		} else {
			// Column doesn't exist in this file (union mode)
			auto &validity = FlatVector::Validity(vec);
			for (idx_t i = 0; i < count; i++) {
				validity.SetInvalid(i);
			}
		}
	}

	gstate.current_row_in_file += count;
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

	string file_pattern = input.inputs[0].GetValue<string>();
	string matrix_name = input.inputs[1].GetValue<string>();
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	// Validate files and collect matrix info
	vector<FileSchema> file_schemas;
	for (const auto &file_path : glob_result.matched_files) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}
		file_schemas.push_back(SchemaHarmonizer::GetVarmSchema(context, file_path, matrix_name));
	}

	auto bind_data = make_uniq<AnndataBindData>(glob_result.matched_files, file_pattern);
	bind_data->obsm_varm_matrix_name = matrix_name;
	bind_data->is_varm_scan = true;
	bind_data->schema_mode = schema_mode;

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode
		bind_data->is_multi_file = false;
		bind_data->matrix_rows = file_schemas[0].n_obs;
		bind_data->matrix_cols = file_schemas[0].n_var;
		bind_data->row_count = file_schemas[0].n_obs;

		names.push_back("var_idx");
		return_types.push_back(LogicalType::BIGINT);

		// Preserve the matrix's dtype
		LogicalType matrix_dtype = file_schemas[0].matrix_dtype;
		for (idx_t i = 0; i < bind_data->matrix_cols; i++) {
			names.push_back(matrix_name + "_" + to_string(i));
			return_types.push_back(matrix_dtype);
		}

		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->column_types = return_types;
	} else {
		// Multi-file mode
		bind_data->is_multi_file = true;

		idx_t min_cols = file_schemas[0].n_var;
		idx_t max_cols = file_schemas[0].n_var;
		idx_t total_rows = 0;

		for (const auto &fs : file_schemas) {
			min_cols = MinValue(min_cols, fs.n_var);
			max_cols = MaxValue(max_cols, fs.n_var);
			total_rows += fs.n_obs;
		}

		idx_t result_cols = (schema_mode == SchemaMode::INTERSECTION) ? min_cols : max_cols;
		bind_data->matrix_cols = result_cols;

		names.push_back("_file_name");
		return_types.push_back(LogicalType::VARCHAR);
		names.push_back("var_idx");
		return_types.push_back(LogicalType::BIGINT);

		for (idx_t i = 0; i < result_cols; i++) {
			names.push_back(matrix_name + "_" + to_string(i));
			return_types.push_back(LogicalType::DOUBLE);
		}

		bind_data->harmonized_schema.total_row_count = total_rows;
		for (const auto &fs : file_schemas) {
			bind_data->harmonized_schema.file_row_counts.push_back(fs.n_obs);
			vector<int> mapping;
			mapping.push_back(static_cast<int>(fs.n_var));
			bind_data->harmonized_schema.file_column_mappings.push_back(mapping);
		}

		bind_data->row_count = total_rows;
		bind_data->column_count = names.size();
		bind_data->column_names = names;
		bind_data->column_types = return_types;
	}

	return std::move(bind_data);
}

void AnndataScanner::VarmScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (!bind_data.is_multi_file) {
		// Single file mode
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - gstate.current_row);
		if (count == 0) {
			return;
		}

		auto &var_idx_vec = output.data[0];
		for (idx_t i = 0; i < count; i++) {
			var_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row + i));
		}

		for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
			auto &vec = output.data[col + 1];
			gstate.h5_reader->ReadVarmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row, count, col, vec);
		}

		gstate.current_row += count;
		output.SetCardinality(count);
		return;
	}

	// Multi-file mode
	if (!gstate.h5_reader) {
		gstate.OpenCurrentFile(context, bind_data);
	}

	if (gstate.current_file_idx >= bind_data.file_paths.size()) {
		output.SetCardinality(0);
		return;
	}

	idx_t file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
	idx_t rows_remaining = file_row_count - gstate.current_row_in_file;

	while (rows_remaining == 0) {
		if (!gstate.AdvanceToNextFile(context, bind_data)) {
			output.SetCardinality(0);
			return;
		}
		file_row_count = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		rows_remaining = file_row_count - gstate.current_row_in_file;
	}

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, rows_remaining);
	idx_t file_cols = static_cast<idx_t>(bind_data.harmonized_schema.file_column_mappings[gstate.current_file_idx][0]);

	auto &file_name_vec = output.data[0];
	for (idx_t i = 0; i < count; i++) {
		FlatVector::GetData<string_t>(file_name_vec)[i] =
		    StringVector::AddString(file_name_vec, gstate.current_file_name);
	}

	auto &var_idx_vec = output.data[1];
	for (idx_t i = 0; i < count; i++) {
		var_idx_vec.SetValue(i, Value::BIGINT(gstate.current_row_in_file + i));
	}

	for (idx_t col = 0; col < bind_data.matrix_cols; col++) {
		auto &vec = output.data[col + 2];
		if (col < file_cols) {
			gstate.h5_reader->ReadVarmMatrix(bind_data.obsm_varm_matrix_name, gstate.current_row_in_file, count, col,
			                                 vec);
		} else {
			auto &validity = FlatVector::Validity(vec);
			for (idx_t i = 0; i < count; i++) {
				validity.SetInvalid(i);
			}
		}
	}

	gstate.current_row_in_file += count;
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
	string file_pattern = input.inputs[0].GetValue<string>();
	string layer_name = input.inputs[1].GetValue<string>();

	// Get variable name column - allow custom column selection
	// Default to "_index" which is standard AnnData var_names column (same as XBind)
	string var_column = "_index";
	if (input.inputs.size() > 2) {
		var_column = input.inputs[2].GetValue<string>();
	}

	// Check for schema_mode parameter
	SchemaMode schema_mode = ParseSchemaMode(input);

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_pattern);

	if (glob_result.matched_files.empty()) {
		throw InvalidInputException("No files found matching pattern: " + file_pattern);
	}

	// Create bind data with multi-file support
	auto result = make_uniq<AnndataBindData>(glob_result.matched_files, glob_result.is_pattern ? file_pattern : "");
	result->is_layer_scan = true;
	result->layer_name = layer_name;
	result->var_name_column = var_column;
	result->schema_mode = schema_mode;

	// Validate files and collect layer schema info
	vector<FileSchema> file_schemas;
	LogicalType layer_dtype = LogicalType::DOUBLE; // Default, will be updated

	for (const auto &file_path : glob_result.matched_files) {
		auto reader_ptr = CreateH5Reader(context, file_path);
		auto &reader = *reader_ptr;
		auto layers = reader.GetLayers();

		// Find the requested layer
		bool found = false;
		H5ReaderMultithreaded::LayerInfo layer_info;
		for (const auto &layer : layers) {
			if (layer.name == layer_name) {
				layer_info = layer;
				found = true;
				layer_dtype = layer_info.dtype;
				break;
			}
		}

		if (!found) {
			throw InvalidInputException("Layer '%s' not found in file %s", layer_name.c_str(), file_path.c_str());
		}

		file_schemas.push_back(SchemaHarmonizer::GetLayerSchema(context, file_path, layer_name, var_column));
	}

	if (glob_result.matched_files.size() == 1 && !glob_result.is_pattern) {
		// Single file mode
		result->is_multi_file = false;
		result->n_obs = file_schemas[0].n_obs;
		result->n_var = file_schemas[0].n_var;
		result->var_names = file_schemas[0].var_names;
		result->row_count = result->n_obs;

		// Set up column schema: obs_idx + all gene columns
		names.emplace_back("obs_idx");
		return_types.emplace_back(LogicalType::BIGINT);

		for (const auto &var_name : result->var_names) {
			names.emplace_back(var_name);
			return_types.emplace_back(layer_dtype);
		}
	} else {
		// Multi-file mode - compute harmonized schema
		result->is_multi_file = true;

		// Compute layer schema with var name intersection/union (reuse X schema logic)
		result->harmonized_schema = SchemaHarmonizer::ComputeXSchema(file_schemas, schema_mode, {});

		// Total row count across all files
		result->row_count = result->harmonized_schema.total_row_count;
		result->n_obs = result->harmonized_schema.total_row_count;
		result->n_var = result->harmonized_schema.common_var_names.size();
		result->var_names = result->harmonized_schema.common_var_names;

		// Set up columns: _file_name, obs_idx + one column per common gene
		names.emplace_back("_file_name");
		return_types.emplace_back(LogicalType::VARCHAR);

		names.emplace_back("obs_idx");
		return_types.emplace_back(LogicalType::BIGINT);

		for (const auto &var_name : result->harmonized_schema.common_var_names) {
			names.emplace_back(var_name);
			return_types.emplace_back(layer_dtype);
		}
	}

	result->column_names = names;
	result->column_types = return_types;
	result->column_count = names.size();

	return std::move(result);
}

void AnndataScanner::LayerScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AnndataBindData>();
	auto &state = data.global_state->Cast<AnndataGlobalState>();

	if (bind_data.is_multi_file) {
		// Multi-file layer scan with projection pushdown
		// Open first file if not already open
		if (!state.h5_reader) {
			state.OpenCurrentFile(context, bind_data);
			// Set up var mapping for current file
			state.current_var_mapping = bind_data.harmonized_schema.file_var_mappings[state.current_file_idx];
		}

		// Read from current file
		idx_t current_file_obs = bind_data.harmonized_schema.file_row_counts[state.current_file_idx];
		idx_t remaining_in_file = current_file_obs - state.current_row_in_file;
		idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);

		if (to_read == 0) {
			// Try to advance to next file
			if (!state.AdvanceToNextFile(context, bind_data)) {
				return; // All files exhausted
			}
			// Update var mapping for new file
			state.current_var_mapping = bind_data.harmonized_schema.file_var_mappings[state.current_file_idx];
			current_file_obs = bind_data.harmonized_schema.file_row_counts[state.current_file_idx];
			remaining_in_file = current_file_obs - state.current_row_in_file;
			to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);
		}

		if (to_read == 0) {
			return;
		}

		idx_t count = to_read;

		// With projection pushdown, column_ids maps output positions to bind-time column indices
		// Column layout at bind time: 0=_file_name, 1=obs_idx, 2+=gene columns
		bool has_projection = !state.column_ids.empty();

		if (has_projection) {
			vector<idx_t> file_var_indices;
			vector<idx_t> output_gene_cols;

			for (idx_t out_idx = 0; out_idx < state.column_ids.size(); out_idx++) {
				idx_t col_id = state.column_ids[out_idx];
				if (col_id == 0) {
					// _file_name column
					auto file_name_data = FlatVector::GetData<string_t>(output.data[out_idx]);
					for (idx_t i = 0; i < count; i++) {
						file_name_data[i] = StringVector::AddString(output.data[out_idx], state.current_file_name);
					}
				} else if (col_id == 1) {
					// obs_idx column
					auto obs_idx_data = FlatVector::GetData<int64_t>(output.data[out_idx]);
					for (idx_t i = 0; i < count; i++) {
						obs_idx_data[i] = static_cast<int64_t>(state.current_row_in_file + i);
					}
				} else {
					idx_t var_idx = col_id - 2;
					if (var_idx < state.current_var_mapping.size()) {
						idx_t file_col = state.current_var_mapping[var_idx];
						if (file_col != DConstants::INVALID_INDEX) {
							file_var_indices.push_back(file_col);
							output_gene_cols.push_back(out_idx);
						} else {
							auto &validity = FlatVector::Validity(output.data[out_idx]);
							for (idx_t i = 0; i < count; i++) {
								validity.SetInvalid(i);
							}
						}
					}
				}
			}

			if (!file_var_indices.empty()) {
				DataChunk matrix_output;
				vector<LogicalType> matrix_types(file_var_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				string layer_path = "/layers/" + bind_data.layer_name;
				state.h5_reader->ReadMatrixColumns(layer_path, state.current_row_in_file, count, file_var_indices,
				                                   matrix_output, true);

				for (idx_t m = 0; m < file_var_indices.size(); m++) {
					auto &src = matrix_output.data[m];
					auto &dst = output.data[output_gene_cols[m]];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}
		} else {
			// No projection pushdown - write all columns
			idx_t col_offset = 0;

			auto file_name_data = FlatVector::GetData<string_t>(output.data[0]);
			for (idx_t i = 0; i < count; i++) {
				file_name_data[i] = StringVector::AddString(output.data[0], state.current_file_name);
			}
			col_offset = 1;

			auto obs_idx_data = FlatVector::GetData<int64_t>(output.data[col_offset]);
			for (idx_t i = 0; i < count; i++) {
				obs_idx_data[i] = static_cast<int64_t>(state.current_row_in_file + i);
			}
			col_offset++;

			vector<idx_t> file_var_indices;
			vector<idx_t> output_var_cols;
			for (idx_t v = 0; v < state.current_var_mapping.size(); v++) {
				idx_t file_idx = state.current_var_mapping[v];
				if (file_idx != DConstants::INVALID_INDEX) {
					file_var_indices.push_back(file_idx);
					output_var_cols.push_back(col_offset + v);
				}
			}

			if (!file_var_indices.empty()) {
				DataChunk matrix_output;
				vector<LogicalType> matrix_types(file_var_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				string layer_path = "/layers/" + bind_data.layer_name;
				state.h5_reader->ReadMatrixColumns(layer_path, state.current_row_in_file, count, file_var_indices,
				                                   matrix_output, true);

				for (idx_t m = 0; m < file_var_indices.size(); m++) {
					auto &src = matrix_output.data[m];
					auto &dst = output.data[output_var_cols[m]];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}

			for (idx_t v = 0; v < state.current_var_mapping.size(); v++) {
				if (state.current_var_mapping[v] == DConstants::INVALID_INDEX) {
					auto &vec = output.data[col_offset + v];
					auto &validity = FlatVector::Validity(vec);
					for (idx_t row = 0; row < count; row++) {
						validity.SetInvalid(row);
					}
				}
			}
		}

		state.current_row_in_file += count;
		state.current_row += count;
		output.SetCardinality(count);
	} else {
		// Single file layer scan
		if (!state.h5_reader) {
			state.h5_reader = CreateH5Reader(context, bind_data.file_path);
		}

		idx_t remaining = bind_data.row_count - state.current_row;
		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);

		if (count == 0) {
			return;
		}

		// Check if projection pushdown is enabled (column_ids not empty)
		if (!state.column_ids.empty()) {
			vector<idx_t> matrix_col_indices;

			for (idx_t i = 0; i < state.column_ids.size(); i++) {
				idx_t col_id = state.column_ids[i];
				if (col_id == 0) {
					// obs_idx column - fill with row indices
					auto &obs_idx_vec = output.data[i];
					for (idx_t row = 0; row < count; row++) {
						obs_idx_vec.SetValue(row, Value::BIGINT(static_cast<int64_t>(state.current_row + row)));
					}
				} else {
					matrix_col_indices.push_back(col_id - 1);
				}
			}

			if (!matrix_col_indices.empty()) {
				vector<idx_t> output_col_mapping;
				for (idx_t i = 0; i < state.column_ids.size(); i++) {
					if (state.column_ids[i] != 0) {
						output_col_mapping.push_back(i);
					}
				}

				DataChunk matrix_output;
				vector<LogicalType> matrix_types(matrix_col_indices.size(), LogicalType::DOUBLE);
				matrix_output.Initialize(Allocator::DefaultAllocator(), matrix_types);

				string layer_path = "/layers/" + bind_data.layer_name;
				state.h5_reader->ReadMatrixColumns(layer_path, state.current_row, count, matrix_col_indices,
				                                   matrix_output, true);

				for (idx_t m = 0; m < matrix_col_indices.size(); m++) {
					idx_t out_col = output_col_mapping[m];
					auto &src = matrix_output.data[m];
					auto &dst = output.data[out_col];
					for (idx_t row = 0; row < count; row++) {
						dst.SetValue(row, src.GetValue(row));
					}
				}
			}

			output.SetCardinality(count);
		} else {
			// No projection pushdown - read all columns
			state.h5_reader->ReadLayerMatrixBatch(bind_data.layer_name, state.current_row, count, 0, bind_data.n_var,
			                                      output);
		}

		state.current_row += count;
	}
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

	auto file_path_input = input.inputs[0].GetValue<string>();
	auto matrix_name = input.inputs[1].GetValue<string>();

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_path_input);

	if (glob_result.matched_files.empty()) {
		throw InvalidInputException("No files found matching pattern: " + file_path_input);
	}

	// Create bind data with multi-file support
	auto bind_data =
	    make_uniq<AnndataBindData>(glob_result.matched_files, glob_result.is_pattern ? file_path_input : "");
	bind_data->is_obsp_scan = true;
	bind_data->pairwise_matrix_name = matrix_name;

	// Validate all files and collect nnz counts
	idx_t total_nnz = 0;
	for (const auto &file_path : bind_data->file_paths) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}

		auto reader_ptr = CreateH5Reader(context, file_path);
		auto &reader = *reader_ptr;

		if (!reader.IsValidAnnData()) {
			throw InvalidInputException("File is not a valid AnnData format: " + file_path);
		}

		// All files must have the named matrix (obsp pairs are file-scoped)
		H5ReaderMultithreaded::SparseMatrixInfo info = reader.GetObspMatrixInfo(matrix_name);
		bind_data->harmonized_schema.file_row_counts.push_back(info.nnz);
		total_nnz += info.nnz;
	}

	bind_data->nnz = total_nnz;
	bind_data->row_count = total_nnz;
	bind_data->harmonized_schema.total_row_count = total_nnz;

	// Set up column schema
	// For multi-file, add _file_name column first
	if (bind_data->is_multi_file) {
		names.emplace_back("_file_name");
		return_types.emplace_back(LogicalType::VARCHAR);
	}

	names.emplace_back("obs_idx_1");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("obs_idx_2");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::FLOAT);

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::ObspScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (bind_data.nnz == 0) {
		return;
	}

	if (bind_data.is_multi_file) {
		// Multi-file obsp scan
		// Open first file if not already open
		if (!gstate.h5_reader) {
			gstate.OpenCurrentFile(context, bind_data);
		}

		idx_t count = 0;
		idx_t col_offset = 0; // Column offset for _file_name

		// Read from current file
		idx_t current_file_nnz = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		idx_t remaining_in_file = current_file_nnz - gstate.current_row_in_file;
		idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);

		if (to_read == 0) {
			// Try to advance to next file
			if (!gstate.AdvanceToNextFile(context, bind_data)) {
				return; // All files exhausted
			}
			current_file_nnz = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
			remaining_in_file = current_file_nnz - gstate.current_row_in_file;
			to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);
		}

		if (to_read == 0) {
			return;
		}

		// Fill _file_name column
		auto file_name_data = FlatVector::GetData<string_t>(output.data[0]);
		for (idx_t i = 0; i < to_read; i++) {
			file_name_data[i] = StringVector::AddString(output.data[0], gstate.current_file_name);
		}
		col_offset = 1;

		// Read sparse matrix data (obs_idx_1, obs_idx_2, value)
		gstate.h5_reader->ReadObspMatrix(bind_data.pairwise_matrix_name, output.data[col_offset],
		                                 output.data[col_offset + 1], output.data[col_offset + 2],
		                                 gstate.current_row_in_file, to_read);

		count = to_read;
		gstate.current_row_in_file += to_read;
		gstate.current_row += to_read;
		output.SetCardinality(count);
	} else {
		// Single file obsp scan
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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
}

// Table function implementations for varp (variable pairwise matrices)
unique_ptr<FunctionData> AnndataScanner::VarpBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	// Validate input parameters
	if (input.inputs.size() != 2) {
		throw InvalidInputException("anndata_scan_varp requires 2 parameters: file_path and matrix_name");
	}

	auto file_path_input = input.inputs[0].GetValue<string>();
	auto matrix_name = input.inputs[1].GetValue<string>();

	// Expand glob pattern
	auto glob_result = GlobHandler::ExpandGlobPattern(context, file_path_input);

	if (glob_result.matched_files.empty()) {
		throw InvalidInputException("No files found matching pattern: " + file_path_input);
	}

	// Create bind data with multi-file support
	auto bind_data =
	    make_uniq<AnndataBindData>(glob_result.matched_files, glob_result.is_pattern ? file_path_input : "");
	bind_data->is_varp_scan = true;
	bind_data->pairwise_matrix_name = matrix_name;

	// Validate all files and collect nnz counts
	idx_t total_nnz = 0;
	for (const auto &file_path : bind_data->file_paths) {
		if (!IsAnndataFile(context, file_path)) {
			throw InvalidInputException("File is not a valid AnnData file: " + file_path);
		}

		auto reader_ptr = CreateH5Reader(context, file_path);
		auto &reader = *reader_ptr;

		if (!reader.IsValidAnnData()) {
			throw InvalidInputException("File is not a valid AnnData format: " + file_path);
		}

		// All files must have the named matrix (varp pairs are file-scoped)
		H5ReaderMultithreaded::SparseMatrixInfo info = reader.GetVarpMatrixInfo(matrix_name);
		bind_data->harmonized_schema.file_row_counts.push_back(info.nnz);
		total_nnz += info.nnz;
	}

	bind_data->nnz = total_nnz;
	bind_data->row_count = total_nnz;
	bind_data->harmonized_schema.total_row_count = total_nnz;

	// Set up column schema
	// For multi-file, add _file_name column first
	if (bind_data->is_multi_file) {
		names.emplace_back("_file_name");
		return_types.emplace_back(LogicalType::VARCHAR);
	}

	names.emplace_back("var_idx_1");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("var_idx_2");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::FLOAT);

	bind_data->column_count = names.size();
	bind_data->column_names = names;
	bind_data->column_types = return_types;

	return std::move(bind_data);
}

void AnndataScanner::VarpScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (AnndataBindData &)*data.bind_data;
	auto &gstate = (AnndataGlobalState &)*data.global_state;

	if (bind_data.nnz == 0) {
		return;
	}

	if (bind_data.is_multi_file) {
		// Multi-file varp scan
		// Open first file if not already open
		if (!gstate.h5_reader) {
			gstate.OpenCurrentFile(context, bind_data);
		}

		idx_t count = 0;
		idx_t col_offset = 0; // Column offset for _file_name

		// Read from current file
		idx_t current_file_nnz = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
		idx_t remaining_in_file = current_file_nnz - gstate.current_row_in_file;
		idx_t to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);

		if (to_read == 0) {
			// Try to advance to next file
			if (!gstate.AdvanceToNextFile(context, bind_data)) {
				return; // All files exhausted
			}
			current_file_nnz = bind_data.harmonized_schema.file_row_counts[gstate.current_file_idx];
			remaining_in_file = current_file_nnz - gstate.current_row_in_file;
			to_read = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining_in_file);
		}

		if (to_read == 0) {
			return;
		}

		// Fill _file_name column
		auto file_name_data = FlatVector::GetData<string_t>(output.data[0]);
		for (idx_t i = 0; i < to_read; i++) {
			file_name_data[i] = StringVector::AddString(output.data[0], gstate.current_file_name);
		}
		col_offset = 1;

		// Read sparse matrix data (var_idx_1, var_idx_2, value)
		gstate.h5_reader->ReadVarpMatrix(bind_data.pairwise_matrix_name, output.data[col_offset],
		                                 output.data[col_offset + 1], output.data[col_offset + 2],
		                                 gstate.current_row_in_file, to_read);

		count = to_read;
		gstate.current_row_in_file += to_read;
		gstate.current_row += to_read;
		output.SetCardinality(count);
	} else {
		// Single file varp scan
		if (!gstate.h5_reader) {
			gstate.h5_reader = CreateH5Reader(context, bind_data.file_path);
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
	// Register anndata_scan_obs function with schema_mode named parameter for multi-file support
	TableFunction obs_func("anndata_scan_obs", {LogicalType::VARCHAR}, AnndataScanner::ObsScan, AnndataScanner::ObsBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	obs_func.name = "anndata_scan_obs";
	obs_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(obs_func);

	// Register anndata_scan_var function with schema_mode named parameter for multi-file support
	TableFunction var_func("anndata_scan_var", {LogicalType::VARCHAR}, AnndataScanner::VarScan, AnndataScanner::VarBind,
	                       AnndataInitGlobal, AnndataInitLocal);
	var_func.name = "anndata_scan_var";
	var_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(var_func);

	// Register anndata_scan_x function with projection pushdown enabled
	TableFunction x_func("anndata_scan_x", {LogicalType::VARCHAR}, AnndataScanner::XScan, AnndataScanner::XBind,
	                     AnndataInitGlobalWithProjection, AnndataInitLocal);
	x_func.name = "anndata_scan_x";
	x_func.projection_pushdown = true;
	x_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(x_func);

	// Also register with optional var_name_column parameter
	TableFunction x_func_with_param("anndata_scan_x", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                AnndataScanner::XScan, AnndataScanner::XBind, AnndataInitGlobalWithProjection,
	                                AnndataInitLocal);
	x_func_with_param.name = "anndata_scan_x";
	x_func_with_param.projection_pushdown = true;
	x_func_with_param.named_parameters["schema_mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(x_func_with_param);

	// Register anndata_scan_obsm function (2 parameters - correct usage)
	TableFunction obsm_func("anndata_scan_obsm", {LogicalType::VARCHAR, LogicalType::VARCHAR}, AnndataScanner::ObsmScan,
	                        AnndataScanner::ObsmBind, AnndataInitGlobal, AnndataInitLocal);
	obsm_func.name = "anndata_scan_obsm";
	obsm_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
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
	varm_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
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
	layers_func.named_parameters["schema_mode"] = LogicalType::VARCHAR;
	loader.RegisterFunction(layers_func);

	// Register anndata_scan_layers function (3 parameters - with custom var column)
	TableFunction layers_func_custom(
	    "anndata_scan_layers", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    AnndataScanner::LayerScan, AnndataScanner::LayerBind, AnndataInitGlobalWithProjection, AnndataInitLocal);
	layers_func_custom.name = "anndata_scan_layers";
	layers_func_custom.projection_pushdown = true;
	layers_func_custom.named_parameters["schema_mode"] = LogicalType::VARCHAR;
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
