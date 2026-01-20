#include "include/schema_harmonizer.hpp"
#include "include/s3_credentials.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

unique_ptr<H5ReaderMultithreaded> SchemaHarmonizer::CreateReader(ClientContext &context, const string &file_path) {
	H5FileCache::RemoteConfig config;
	if (GetS3ConfigFromSecrets(context, file_path, config)) {
		return make_uniq<H5ReaderMultithreaded>(file_path, &config);
	}
	return make_uniq<H5ReaderMultithreaded>(file_path);
}

LogicalType SchemaHarmonizer::CoerceTypes(const LogicalType &type1, const LogicalType &type2) {
	if (type1 == type2) {
		return type1;
	}

	// VARCHAR is the universal fallback
	if (type1 == LogicalType::VARCHAR || type2 == LogicalType::VARCHAR) {
		return LogicalType::VARCHAR;
	}

	// Numeric type promotion
	if (type1.IsNumeric() && type2.IsNumeric()) {
		// DOUBLE is the safest numeric type
		if (type1 == LogicalType::DOUBLE || type2 == LogicalType::DOUBLE) {
			return LogicalType::DOUBLE;
		}
		if (type1 == LogicalType::FLOAT || type2 == LogicalType::FLOAT) {
			return LogicalType::DOUBLE;
		}
		if (type1 == LogicalType::BIGINT || type2 == LogicalType::BIGINT) {
			return LogicalType::BIGINT;
		}
		return LogicalType::BIGINT;
	}

	// Default to VARCHAR for incompatible types
	return LogicalType::VARCHAR;
}

HarmonizedSchema SchemaHarmonizer::ComputeObsVarSchema(const vector<FileSchema> &file_schemas, SchemaMode mode) {
	HarmonizedSchema result;

	if (file_schemas.empty()) {
		return result;
	}

	if (file_schemas.size() == 1) {
		// Single file - just copy the schema
		result.columns = file_schemas[0].columns;
		result.file_column_mappings.emplace_back();
		for (idx_t i = 0; i < result.columns.size(); i++) {
			result.file_column_mappings[0].push_back(static_cast<int>(i));
		}
		result.file_row_counts.push_back(file_schemas[0].n_obs > 0 ? file_schemas[0].n_obs : file_schemas[0].n_var);
		result.total_row_count = result.file_row_counts[0];
		return result;
	}

	// Build column name -> info map for each file
	vector<unordered_map<string, pair<idx_t, ColumnInfo>>> file_column_maps;
	for (const auto &fs : file_schemas) {
		unordered_map<string, pair<idx_t, ColumnInfo>> col_map;
		for (idx_t i = 0; i < fs.columns.size(); i++) {
			col_map[fs.columns[i].name] = {i, fs.columns[i]};
		}
		file_column_maps.push_back(std::move(col_map));
	}

	if (mode == SchemaMode::INTERSECTION) {
		// Find columns present in ALL files
		// Start with first file's columns
		unordered_set<string> common_cols;
		for (const auto &col : file_schemas[0].columns) {
			common_cols.insert(col.name);
		}

		// Intersect with each subsequent file
		for (size_t f = 1; f < file_schemas.size(); f++) {
			unordered_set<string> file_cols;
			for (const auto &col : file_schemas[f].columns) {
				file_cols.insert(col.name);
			}

			unordered_set<string> intersection;
			for (const auto &col : common_cols) {
				if (file_cols.count(col) > 0) {
					intersection.insert(col);
				}
			}
			common_cols = std::move(intersection);
		}

		if (common_cols.empty()) {
			throw InvalidInputException(
			    "No common columns found across files in intersection mode. "
			    "Hint: Use schema_mode := 'union' to include all columns.");
		}

		// Build result schema in order of first file's columns
		for (const auto &col : file_schemas[0].columns) {
			if (common_cols.count(col.name) > 0) {
				// Determine the coerced type across all files
				LogicalType coerced_type = col.type;
				for (size_t f = 1; f < file_schemas.size(); f++) {
					auto it = file_column_maps[f].find(col.name);
					if (it != file_column_maps[f].end()) {
						coerced_type = CoerceTypes(coerced_type, it->second.second.type);
					}
				}
				result.columns.emplace_back(col.name, col.original_name, coerced_type);
			}
		}
	} else {
		// Union mode - include all columns from all files
		unordered_map<string, ColumnInfo> all_columns;
		vector<string> column_order; // Preserve order

		for (const auto &fs : file_schemas) {
			for (const auto &col : fs.columns) {
				if (all_columns.count(col.name) == 0) {
					all_columns[col.name] = col;
					column_order.push_back(col.name);
				} else {
					// Coerce types
					all_columns[col.name].type = CoerceTypes(all_columns[col.name].type, col.type);
				}
			}
		}

		for (const auto &name : column_order) {
			result.columns.push_back(all_columns[name]);
		}
	}

	// Build per-file column mappings
	for (size_t f = 0; f < file_schemas.size(); f++) {
		vector<int> mapping;
		for (const auto &col : result.columns) {
			auto it = file_column_maps[f].find(col.name);
			if (it != file_column_maps[f].end()) {
				mapping.push_back(static_cast<int>(it->second.first));
			} else {
				mapping.push_back(-1); // Column not present in this file
			}
		}
		result.file_column_mappings.push_back(std::move(mapping));

		// Track row counts
		idx_t row_count = file_schemas[f].n_obs > 0 ? file_schemas[f].n_obs : file_schemas[f].n_var;
		result.file_row_counts.push_back(row_count);
		result.total_row_count += row_count;
	}

	return result;
}

HarmonizedSchema SchemaHarmonizer::ComputeXSchema(const vector<FileSchema> &file_schemas, SchemaMode mode,
                                                  const vector<string> &projected_var_names) {
	HarmonizedSchema result;

	if (file_schemas.empty()) {
		return result;
	}

	// For X matrix, schema is fixed: obs_idx, var_idx, var_name, value
	// What varies is which var_names (genes) are included

	// Compute var intersection/union
	if (projected_var_names.empty()) {
		// Full intersection/union of all var names
		if (mode == SchemaMode::INTERSECTION) {
			// Start with first file's vars
			unordered_set<string> common_vars(file_schemas[0].var_names.begin(), file_schemas[0].var_names.end());

			// Intersect with each subsequent file
			for (size_t f = 1; f < file_schemas.size(); f++) {
				unordered_set<string> file_vars(file_schemas[f].var_names.begin(), file_schemas[f].var_names.end());

				unordered_set<string> intersection;
				for (const auto &var : common_vars) {
					if (file_vars.count(var) > 0) {
						intersection.insert(var);
					}
				}
				common_vars = std::move(intersection);
			}

			if (common_vars.empty()) {
				throw InvalidInputException("No common genes/variables found across files in intersection mode. "
				                            "Hint: Use schema_mode := 'union' or filter to specific genes.");
			}

			// Preserve order from first file
			for (const auto &var : file_schemas[0].var_names) {
				if (common_vars.count(var) > 0) {
					result.common_var_names.push_back(var);
				}
			}
		} else {
			// Union of all vars
			unordered_set<string> all_vars;
			for (const auto &fs : file_schemas) {
				for (const auto &var : fs.var_names) {
					if (all_vars.count(var) == 0) {
						all_vars.insert(var);
						result.common_var_names.push_back(var);
					}
				}
			}
		}
	} else {
		// Only use projected var names
		if (mode == SchemaMode::INTERSECTION) {
			// Check that all projected vars exist in all files
			for (const auto &var : projected_var_names) {
				bool found_in_all = true;
				string missing_file;
				for (const auto &fs : file_schemas) {
					if (fs.var_name_to_idx.count(var) == 0) {
						found_in_all = false;
						missing_file = fs.file_path;
						break;
					}
				}
				if (found_in_all) {
					result.common_var_names.push_back(var);
				} else if (mode == SchemaMode::INTERSECTION) {
					throw InvalidInputException("Gene '" + var + "' not found in file '" + missing_file +
					                            "' (intersection mode). "
					                            "Hint: Use schema_mode := 'union' or remove this gene from the filter.");
				}
			}
		} else {
			// Union mode - include vars present in at least one file
			for (const auto &var : projected_var_names) {
				result.common_var_names.push_back(var);
			}
		}
	}

	// Build per-file var mappings
	for (size_t f = 0; f < file_schemas.size(); f++) {
		vector<idx_t> mapping;
		for (const auto &var : result.common_var_names) {
			auto it = file_schemas[f].var_name_to_idx.find(var);
			if (it != file_schemas[f].var_name_to_idx.end()) {
				mapping.push_back(it->second);
			} else {
				mapping.push_back(DConstants::INVALID_INDEX); // Var not in this file (union mode)
			}
		}
		result.file_var_mappings.push_back(std::move(mapping));

		// Row count for X is n_obs * n_common_vars (for dense) or nnz (for sparse)
		result.file_row_counts.push_back(file_schemas[f].n_obs);
		result.total_row_count += file_schemas[f].n_obs;
	}

	return result;
}

HarmonizedSchema SchemaHarmonizer::ComputeObsmVarmSchema(const vector<FileSchema> &file_schemas, SchemaMode mode,
                                                        idx_t expected_cols) {
	HarmonizedSchema result;

	if (file_schemas.empty()) {
		return result;
	}

	// For obsm/varm, columns are obs_idx/var_idx + dim_0, dim_1, ...
	// The number of dimensions may vary across files

	idx_t min_cols = UINT64_MAX;
	idx_t max_cols = 0;

	for (const auto &fs : file_schemas) {
		// n_var in FileSchema is used to store matrix columns for obsm/varm
		idx_t ncols = fs.n_var;
		min_cols = MinValue(min_cols, ncols);
		max_cols = MaxValue(max_cols, ncols);
	}

	idx_t result_cols = (mode == SchemaMode::INTERSECTION) ? min_cols : max_cols;

	// Build column schema
	result.columns.emplace_back("obs_idx", "obs_idx", LogicalType::BIGINT);
	for (idx_t i = 0; i < result_cols; i++) {
		result.columns.emplace_back("dim_" + std::to_string(i), "dim_" + std::to_string(i), LogicalType::DOUBLE);
	}

	// Build mappings (all files have the same column structure, just different lengths)
	for (size_t f = 0; f < file_schemas.size(); f++) {
		vector<int> mapping;
		mapping.push_back(0); // obs_idx always maps
		for (idx_t i = 0; i < result_cols; i++) {
			if (i < file_schemas[f].n_var) {
				mapping.push_back(static_cast<int>(i + 1));
			} else {
				mapping.push_back(-1); // Dimension not present
			}
		}
		result.file_column_mappings.push_back(std::move(mapping));
		result.file_row_counts.push_back(file_schemas[f].n_obs);
		result.total_row_count += file_schemas[f].n_obs;
	}

	return result;
}

FileSchema SchemaHarmonizer::GetObsSchema(ClientContext &context, const string &file_path) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto columns = reader->GetObsColumns();
	for (const auto &col : columns) {
		schema.columns.emplace_back(col.name, col.original_name, col.type);
	}

	schema.n_obs = reader->GetObsCount();
	return schema;
}

FileSchema SchemaHarmonizer::GetVarSchema(ClientContext &context, const string &file_path) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto columns = reader->GetVarColumns();
	for (const auto &col : columns) {
		schema.columns.emplace_back(col.name, col.original_name, col.type);
	}

	schema.n_var = reader->GetVarCount();
	return schema;
}

FileSchema SchemaHarmonizer::GetXSchema(ClientContext &context, const string &file_path,
                                        const string &var_name_column) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto x_info = reader->GetXMatrixInfo();
	schema.n_obs = x_info.n_obs;
	schema.n_var = x_info.n_var;

	// Get var names
	schema.var_names = reader->GetVarNames(var_name_column);
	for (idx_t i = 0; i < schema.var_names.size(); i++) {
		schema.var_name_to_idx[schema.var_names[i]] = i;
	}

	return schema;
}

FileSchema SchemaHarmonizer::GetLayerSchema(ClientContext &context, const string &file_path, const string &layer_name,
                                            const string &var_name_column) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto layer_info = reader->GetLayerInfo(layer_name);
	if (!layer_info.exists) {
		throw InvalidInputException("Layer '" + layer_name + "' not found in file '" + file_path + "'");
	}

	schema.n_obs = layer_info.rows;
	schema.n_var = layer_info.cols;

	// Get var names
	schema.var_names = reader->GetVarNames(var_name_column);
	for (idx_t i = 0; i < schema.var_names.size(); i++) {
		schema.var_name_to_idx[schema.var_names[i]] = i;
	}

	return schema;
}

FileSchema SchemaHarmonizer::GetObsmSchema(ClientContext &context, const string &file_path,
                                           const string &matrix_name) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto matrix_info = reader->GetMatrixInfo("obsm", matrix_name);
	if (!matrix_info.exists) {
		throw InvalidInputException("Matrix 'obsm/" + matrix_name + "' not found in file '" + file_path + "'");
	}

	schema.n_obs = matrix_info.rows;
	schema.n_var = matrix_info.cols; // Store cols in n_var for obsm/varm

	return schema;
}

FileSchema SchemaHarmonizer::GetVarmSchema(ClientContext &context, const string &file_path,
                                           const string &matrix_name) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto matrix_info = reader->GetMatrixInfo("varm", matrix_name);
	if (!matrix_info.exists) {
		throw InvalidInputException("Matrix 'varm/" + matrix_name + "' not found in file '" + file_path + "'");
	}

	schema.n_obs = matrix_info.rows; // Actually var count
	schema.n_var = matrix_info.cols;

	return schema;
}

FileSchema SchemaHarmonizer::GetObspSchema(ClientContext &context, const string &file_path,
                                           const string &matrix_name) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto obsp_info = reader->GetObspInfo(matrix_name);
	if (!obsp_info.exists) {
		throw InvalidInputException("Matrix 'obsp/" + matrix_name + "' not found in file '" + file_path + "'");
	}

	// For obsp, we track nnz as n_obs (it's the number of rows we'll output)
	schema.n_obs = obsp_info.nnz;

	return schema;
}

FileSchema SchemaHarmonizer::GetVarpSchema(ClientContext &context, const string &file_path,
                                           const string &matrix_name) {
	FileSchema schema(file_path);
	auto reader = CreateReader(context, file_path);

	auto varp_info = reader->GetVarpInfo(matrix_name);
	if (!varp_info.exists) {
		throw InvalidInputException("Matrix 'varp/" + matrix_name + "' not found in file '" + file_path + "'");
	}

	schema.n_obs = varp_info.nnz;

	return schema;
}

} // namespace duckdb
