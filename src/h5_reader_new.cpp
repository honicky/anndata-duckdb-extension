#include "include/h5_reader_new.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <cstring>
#include <unordered_set>
#include <algorithm>

namespace duckdb {

// ============================================================================
// Phase 2: Core Infrastructure - Constructor/Destructor and Helper Methods
// ============================================================================

H5ReaderNew::H5ReaderNew(const std::string &file_path) : file_path(file_path), file() {
	// Turn off HDF5 error printing (only needs to be done once per process)
	static bool error_printing_disabled = false;
	if (!error_printing_disabled) {
		H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);
		error_printing_disabled = true;
	}

	// Open the HDF5 file using the C API with thread-safe support
	// No need for concurrency tracking - HDF5 thread-safe mode handles it
	try {
		file = H5FileHandle(file_path, H5F_ACC_RDONLY);
	} catch (const std::exception &e) {
		throw IOException("Failed to open HDF5 file " + file_path + ": " + std::string(e.what()));
	}
}

H5ReaderNew::~H5ReaderNew() {
	// Destructor automatically handled by H5FileHandle RAII
}

// Helper method to check if a group exists
bool H5ReaderNew::IsGroupPresent(const std::string &group_name) {
	if (!H5LinkExists(file.get(), group_name)) {
		return false;
	}
	
	// Check if it's actually a group (not a dataset)
	return H5GetObjectType(file.get(), group_name) == H5O_TYPE_GROUP;
}

// Helper method to check if a dataset exists within a group
bool H5ReaderNew::IsDatasetPresent(const std::string &group_name, const std::string &dataset_name) {
	// First check if the group exists
	if (!IsGroupPresent(group_name)) {
		return false;
	}
	
	try {
		H5GroupHandle group(file.get(), group_name);
		
		if (!H5LinkExists(group.get(), dataset_name)) {
			return false;
		}
		
		// Check if it's actually a dataset
		return H5GetObjectType(group.get(), dataset_name) == H5O_TYPE_DATASET;
	} catch (...) {
		return false;
	}
}

// Callback function for H5Literate to get group members
struct IterateData {
	std::vector<std::string> *members;
};

static herr_t group_iterate_callback(hid_t group_id, const char *name, const H5L_info_t *info, void *op_data) {
	auto *data = static_cast<IterateData*>(op_data);
	data->members->push_back(std::string(name));
	return 0; // Continue iteration
}

// Helper method to get all members of a group
std::vector<std::string> H5ReaderNew::GetGroupMembers(const std::string &group_name) {
	std::vector<std::string> members;
	
	try {
		H5GroupHandle group(file.get(), group_name);
		
		IterateData data;
		data.members = &members;
		
		H5Literate(group.get(), H5_INDEX_NAME, H5_ITER_NATIVE, nullptr, 
		           group_iterate_callback, &data);
	} catch (...) {
		// Return empty vector on error
	}
	
	return members;
}

// Helper method to convert HDF5 type to DuckDB type
LogicalType H5ReaderNew::H5TypeToDuckDBType(hid_t h5_type) {
	H5T_class_t type_class = H5Tget_class(h5_type);
	
	switch (type_class) {
	case H5T_INTEGER: {
		size_t size = H5Tget_size(h5_type);
		if (size <= 1) {
			return LogicalType::TINYINT;
		}
		if (size <= 2) {
			return LogicalType::SMALLINT;
		}
		if (size <= 4) {
			return LogicalType::INTEGER;
		}
		return LogicalType::BIGINT;
	}
	case H5T_FLOAT: {
		size_t size = H5Tget_size(h5_type);
		if (size <= 4) {
			return LogicalType::FLOAT;
		}
		return LogicalType::DOUBLE;
	}
	case H5T_STRING: {
		return LogicalType::VARCHAR;
	}
	case H5T_ENUM: {
		// Enums are typically used for categorical data
		return LogicalType::VARCHAR;
	}
	default:
		// Default to VARCHAR for unknown types
		return LogicalType::VARCHAR;
	}
}

// Check if file is valid AnnData format
bool H5ReaderNew::IsValidAnnData() {
	// Check for required groups: /obs, /var, and either /X group or dataset
	return IsGroupPresent("/obs") && IsGroupPresent("/var") && 
	       (IsGroupPresent("/X") || H5LinkExists(file.get(), "/X"));
}

// Get number of observations (cells)
size_t H5ReaderNew::GetObsCount() {
	try {
		// Try to get shape from obs/_index first (standard location)
		if (IsDatasetPresent("/obs", "_index")) {
			H5DatasetHandle dataset(file.get(), "/obs/_index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}
		
		// Try index (alternative location)
		if (IsDatasetPresent("/obs", "index")) {
			H5DatasetHandle dataset(file.get(), "/obs/index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}
		
		// Try getting from X matrix shape
		if (H5LinkExists(file.get(), "/X")) {
			if (H5GetObjectType(file.get(), "/X") == H5O_TYPE_DATASET) {
				// Dense matrix
				H5DatasetHandle dataset(file.get(), "/X");
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[2];
				int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
				if (ndims == 2) {
					return dims[0];
				}
			} else if (H5GetObjectType(file.get(), "/X") == H5O_TYPE_GROUP) {
				// Sparse matrix - check indptr for CSR or shape attribute
				if (IsDatasetPresent("/X", "indptr")) {
					H5DatasetHandle indptr(file.get(), "/X/indptr");
					H5DataspaceHandle indptr_space(indptr.get());
					hsize_t indptr_dims[1];
					H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
					
					// For CSR format, n_obs = len(indptr) - 1
					// Check if there's a shape attribute to be sure
					if (H5Aexists(file.get(), "shape")) {
						H5AttributeHandle shape_attr(file.get(), "shape");
						hsize_t shape[2];
						H5Aread(shape_attr.get(), H5T_NATIVE_HSIZE, shape);
						return shape[0];
					}
					// Assume CSR if we can't determine
					if (indptr_dims[0] > 0) {
						return indptr_dims[0] - 1;
					}
				}
			}
		}
		
		return 0;
	} catch (...) {
		return 0;
	}
}

// Get number of variables (genes)
size_t H5ReaderNew::GetVarCount() {
	try {
		// Try to get shape from var/_index first (standard location)
		if (IsDatasetPresent("/var", "_index")) {
			H5DatasetHandle dataset(file.get(), "/var/_index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}
		
		// Try index (alternative location)
		if (IsDatasetPresent("/var", "index")) {
			H5DatasetHandle dataset(file.get(), "/var/index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}
		
		// Try getting from X matrix shape
		if (H5LinkExists(file.get(), "/X")) {
			if (H5GetObjectType(file.get(), "/X") == H5O_TYPE_DATASET) {
				// Dense matrix
				H5DatasetHandle dataset(file.get(), "/X");
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[2];
				int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
				if (ndims == 2) {
					return dims[1];
				}
			} else if (H5GetObjectType(file.get(), "/X") == H5O_TYPE_GROUP) {
				// Sparse matrix - check shape attribute or indptr for CSC
				if (H5Aexists(file.get(), "shape")) {
					H5AttributeHandle shape_attr(file.get(), "shape");
					hsize_t shape[2];
					H5Aread(shape_attr.get(), H5T_NATIVE_HSIZE, shape);
					return shape[1];
				}
				
				// Check indptr for CSC format
				if (IsDatasetPresent("/X", "indptr")) {
					H5DatasetHandle indptr(file.get(), "/X/indptr");
					H5DataspaceHandle indptr_space(indptr.get());
					hsize_t indptr_dims[1];
					H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
					
					// For CSC format, n_var = len(indptr) - 1
					size_t obs_count = GetObsCount();
					if (obs_count > 0 && indptr_dims[0] != obs_count + 1) {
						// Likely CSC
						return indptr_dims[0] - 1;
					}
				}
			}
		}
		
		return 0;
	} catch (...) {
		return 0;
	}
}

// ============================================================================
// Stub implementations for remaining methods (to be implemented in later phases)
// ============================================================================

std::vector<H5ReaderNew::ColumnInfo> H5ReaderNew::GetObsColumns() {
	std::vector<ColumnInfo> columns;
	std::unordered_set<std::string> seen_names;
	
	// Add obs_idx as the first column (row index)
	ColumnInfo idx_col;
	idx_col.name = "obs_idx";
	idx_col.original_name = "obs_idx";
	idx_col.type = LogicalType::BIGINT;
	idx_col.is_categorical = false;
	columns.push_back(idx_col);
	seen_names.insert("obs_idx");
	
	try {
		auto members = GetGroupMembers("/obs");
		
		for (const auto &member : members) {
			if (member == "__categories") {
				continue; // Skip metadata
			}
			
			ColumnInfo col;
			col.original_name = member;
			col.name = member;
			
			// Handle duplicate column names (case-insensitive) by mangling
			std::string lower_name = StringUtil::Lower(col.name);
			while (seen_names.count(lower_name) > 0) {
				col.name += "_";
				lower_name = StringUtil::Lower(col.name);
			}
			seen_names.insert(lower_name);
			
			// Check if it's categorical (has codes and categories)
			std::string member_path = "/obs/" + member;
			if (H5GetObjectType(file.get(), member_path) == H5O_TYPE_GROUP) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;
				
				// Load categories
				try {
					std::string cat_path = member_path + "/categories";
					if (H5LinkExists(file.get(), cat_path.c_str())) {
						H5DatasetHandle cat_dataset(file.get(), cat_path);
						H5DataspaceHandle cat_space(cat_dataset.get());
						hsize_t cat_dims[1];
						H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);
						
						// Read string categories
						H5TypeHandle dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
						if (H5Tget_class(dtype.get()) == H5T_STRING) {
							// Fixed-length strings
							size_t str_len = H5Tget_size(dtype.get());
							std::vector<char> buffer(cat_dims[0] * str_len);
							H5Dread(cat_dataset.get(), dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
							
							// Extract strings
							for (hsize_t i = 0; i < cat_dims[0]; i++) {
								std::string cat_value(buffer.data() + i * str_len, strnlen(buffer.data() + i * str_len, str_len));
								col.categories.push_back(cat_value);
							}
						}
					}
				} catch (...) {
					// Continue without categories
				}
			} else if (IsDatasetPresent("/obs", member)) {
				// Direct dataset
				H5DatasetHandle dataset(file.get(), "/obs/" + member);
				H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
				col.type = H5TypeToDuckDBType(dtype.get());
			}
			
			columns.push_back(col);
		}
	} catch (const std::exception &e) {
		// Return what we have so far
	}
	
	return columns;
}

std::vector<H5ReaderNew::ColumnInfo> H5ReaderNew::GetVarColumns() {
	std::vector<ColumnInfo> columns;
	std::unordered_set<std::string> seen_names;
	
	// Add var_idx as the first column (row index)
	ColumnInfo idx_col;
	idx_col.name = "var_idx";
	idx_col.original_name = "var_idx";
	idx_col.type = LogicalType::BIGINT;
	idx_col.is_categorical = false;
	columns.push_back(idx_col);
	seen_names.insert("var_idx");
	
	try {
		auto members = GetGroupMembers("/var");
		
		for (const auto &member : members) {
			if (member == "__categories") {
				continue; // Skip metadata
			}
			
			ColumnInfo col;
			col.original_name = member;
			col.name = member;
			
			// Handle duplicate column names (case-insensitive) by mangling
			std::string lower_name = StringUtil::Lower(col.name);
			while (seen_names.count(lower_name) > 0) {
				col.name += "_";
				lower_name = StringUtil::Lower(col.name);
			}
			seen_names.insert(lower_name);
			
			// Check if it's categorical
			std::string member_path = "/var/" + member;
			if (H5GetObjectType(file.get(), member_path) == H5O_TYPE_GROUP) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;
				
				// Load categories similar to obs
				try {
					std::string cat_path = member_path + "/categories";
					if (H5LinkExists(file.get(), cat_path.c_str())) {
						H5DatasetHandle cat_dataset(file.get(), cat_path);
						H5DataspaceHandle cat_space(cat_dataset.get());
						hsize_t cat_dims[1];
						H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);
						
						// Read string categories
						H5TypeHandle dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
						if (H5Tget_class(dtype.get()) == H5T_STRING) {
							// Fixed-length strings
							size_t str_len = H5Tget_size(dtype.get());
							std::vector<char> buffer(cat_dims[0] * str_len);
							H5Dread(cat_dataset.get(), dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
							
							// Extract strings
							for (hsize_t i = 0; i < cat_dims[0]; i++) {
								std::string cat_value(buffer.data() + i * str_len, strnlen(buffer.data() + i * str_len, str_len));
								col.categories.push_back(cat_value);
							}
						}
					}
				} catch (...) {
					// Continue without categories
				}
			} else if (IsDatasetPresent("/var", member)) {
				// Direct dataset
				H5DatasetHandle dataset(file.get(), "/var/" + member);
				H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
				col.type = H5TypeToDuckDBType(dtype.get());
			}
			
			columns.push_back(col);
		}
	} catch (const std::exception &e) {
		// Return what we have so far
	}
	
	return columns;
}

void H5ReaderNew::ReadObsColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
	try {
		// Handle obs_idx column (row index)
		if (column_name == "obs_idx") {
			for (idx_t i = 0; i < count; i++) {
				result.SetValue(i, Value::BIGINT(offset + i));
			}
			return;
		}
		
		// Check if it's a categorical column
		std::string group_path = "/obs/" + column_name;
		if (H5GetObjectType(file.get(), group_path) == H5O_TYPE_GROUP) {
			// Handle categorical columns
			try {
				// Read codes
				H5DatasetHandle codes_dataset(file.get(), group_path + "/codes");
				H5DataspaceHandle codes_space(codes_dataset.get());
				
				// Read categories into memory (usually small)
				H5DatasetHandle cat_dataset(file.get(), group_path + "/categories");
				H5DataspaceHandle cat_space(cat_dataset.get());
				hsize_t cat_dims[1];
				H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);
				
				// Read all categories first
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);
				
				H5TypeHandle cat_dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				if (H5Tget_class(cat_dtype.get()) == H5T_STRING) {
					if (H5Tis_variable_str(cat_dtype.get())) {
						// Variable-length strings
						std::vector<char*> str_buffer(cat_dims[0]);
						H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, str_buffer.data());
						
						for (size_t i = 0; i < cat_dims[0]; i++) {
							if (str_buffer[i]) {
								categories.emplace_back(str_buffer[i]);
							} else {
								categories.emplace_back("");
							}
						}
						
						// Clean up variable-length strings
						H5Dvlen_reclaim(cat_dtype.get(), cat_space.get(), H5P_DEFAULT, str_buffer.data());
					} else {
						// Fixed-length strings
						size_t str_size = H5Tget_size(cat_dtype.get());
						std::vector<char> buffer(cat_dims[0] * str_size);
						H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
						
						for (size_t i = 0; i < cat_dims[0]; i++) {
							char *str_ptr = buffer.data() + i * str_size;
							size_t len = strnlen(str_ptr, str_size);
							categories.emplace_back(str_ptr, len);
						}
					}
				}
				
				// Now read the codes for the requested range
				std::vector<int8_t> codes(count);
				hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
				hsize_t h_count[1] = {static_cast<hsize_t>(count)};
				
				H5Sselect_hyperslab(codes_space.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
				H5DataspaceHandle mem_space(1, h_count);
				
				H5Dread(codes_dataset.get(), H5T_NATIVE_INT8, mem_space.get(), codes_space.get(), H5P_DEFAULT, codes.data());
				
				// Map codes to categories and set in result vector
				for (idx_t i = 0; i < count; i++) {
					int8_t code = codes[i];
					if (code >= 0 && code < static_cast<int8_t>(categories.size())) {
						result.SetValue(i, Value(categories[code]));
					} else {
						result.SetValue(i, Value()); // NULL for invalid codes
					}
				}
			} catch (...) {
				// If reading as categorical fails, try as regular dataset
			}
		} else if (IsDatasetPresent("/obs", column_name)) {
			// Direct dataset (non-categorical)
			H5DatasetHandle dataset(file.get(), "/obs/" + column_name);
			H5DataspaceHandle dataspace(dataset.get());
			H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
			
			// Set up hyperslab for partial read
			hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
			hsize_t h_count[1] = {static_cast<hsize_t>(count)};
			H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
			
			// Create memory dataspace
			H5DataspaceHandle mem_space(1, h_count);
			
			// Read based on data type
			H5T_class_t type_class = H5Tget_class(dtype.get());
			
			if (type_class == H5T_STRING) {
				if (H5Tis_variable_str(dtype.get())) {
					// Variable-length strings
					std::vector<char*> str_buffer(count);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, str_buffer.data());
					
					for (idx_t i = 0; i < count; i++) {
						if (str_buffer[i]) {
							result.SetValue(i, Value(std::string(str_buffer[i])));
						} else {
							result.SetValue(i, Value());
						}
					}
					
					H5Dvlen_reclaim(dtype.get(), mem_space.get(), H5P_DEFAULT, str_buffer.data());
				} else {
					// Fixed-length strings
					size_t str_size = H5Tget_size(dtype.get());
					std::vector<char> buffer(count * str_size);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					
					for (idx_t i = 0; i < count; i++) {
						char *str_ptr = buffer.data() + i * str_size;
						size_t len = strnlen(str_ptr, str_size);
						result.SetValue(i, Value(std::string(str_ptr, len)));
					}
				}
			} else if (type_class == H5T_INTEGER) {
				// Read integer data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 1) {
					std::vector<int8_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::TINYINT(buffer[i]));
					}
				} else if (size <= 2) {
					std::vector<int16_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT16, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::SMALLINT(buffer[i]));
					}
				} else if (size <= 4) {
					std::vector<int32_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT32, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(buffer[i]));
					}
				} else {
					std::vector<int64_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT64, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(buffer[i]));
					}
				}
			} else if (type_class == H5T_FLOAT) {
				// Read floating-point data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 4) {
					std::vector<float> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_FLOAT, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(buffer[i]));
					}
				} else {
					std::vector<double> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(buffer[i]));
					}
				}
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to read obs column '" + column_name + "': " + e.what());
	}
}

void H5ReaderNew::ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
	try {
		// Handle var_idx column (row index)
		if (column_name == "var_idx") {
			for (idx_t i = 0; i < count; i++) {
				result.SetValue(i, Value::BIGINT(offset + i));
			}
			return;
		}
		
		// Check if it's a categorical column
		std::string group_path = "/var/" + column_name;
		if (H5GetObjectType(file.get(), group_path) == H5O_TYPE_GROUP) {
			// Handle categorical columns - same logic as obs
			try {
				// Read codes
				H5DatasetHandle codes_dataset(file.get(), group_path + "/codes");
				H5DataspaceHandle codes_space(codes_dataset.get());
				
				// Read categories into memory
				H5DatasetHandle cat_dataset(file.get(), group_path + "/categories");
				H5DataspaceHandle cat_space(cat_dataset.get());
				hsize_t cat_dims[1];
				H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);
				
				// Read all categories first
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);
				
				H5TypeHandle cat_dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				if (H5Tget_class(cat_dtype.get()) == H5T_STRING) {
					if (H5Tis_variable_str(cat_dtype.get())) {
						// Variable-length strings
						std::vector<char*> str_buffer(cat_dims[0]);
						H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, str_buffer.data());
						
						for (size_t i = 0; i < cat_dims[0]; i++) {
							if (str_buffer[i]) {
								categories.emplace_back(str_buffer[i]);
							} else {
								categories.emplace_back("");
							}
						}
						
						H5Dvlen_reclaim(cat_dtype.get(), cat_space.get(), H5P_DEFAULT, str_buffer.data());
					} else {
						// Fixed-length strings
						size_t str_size = H5Tget_size(cat_dtype.get());
						std::vector<char> buffer(cat_dims[0] * str_size);
						H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
						
						for (size_t i = 0; i < cat_dims[0]; i++) {
							char *str_ptr = buffer.data() + i * str_size;
							size_t len = strnlen(str_ptr, str_size);
							categories.emplace_back(str_ptr, len);
						}
					}
				}
				
				// Now read the codes for the requested range
				std::vector<int8_t> codes(count);
				hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
				hsize_t h_count[1] = {static_cast<hsize_t>(count)};
				
				H5Sselect_hyperslab(codes_space.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
				H5DataspaceHandle mem_space(1, h_count);
				
				H5Dread(codes_dataset.get(), H5T_NATIVE_INT8, mem_space.get(), codes_space.get(), H5P_DEFAULT, codes.data());
				
				// Map codes to categories
				for (idx_t i = 0; i < count; i++) {
					int8_t code = codes[i];
					if (code >= 0 && code < static_cast<int8_t>(categories.size())) {
						result.SetValue(i, Value(categories[code]));
					} else {
						result.SetValue(i, Value()); // NULL for invalid codes
					}
				}
			} catch (...) {
				// If reading as categorical fails, try as regular dataset
			}
		} else if (IsDatasetPresent("/var", column_name)) {
			// Direct dataset (non-categorical) - same logic as obs
			H5DatasetHandle dataset(file.get(), "/var/" + column_name);
			H5DataspaceHandle dataspace(dataset.get());
			H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
			
			// Set up hyperslab for partial read
			hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
			hsize_t h_count[1] = {static_cast<hsize_t>(count)};
			H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
			
			// Create memory dataspace
			H5DataspaceHandle mem_space(1, h_count);
			
			// Read based on data type
			H5T_class_t type_class = H5Tget_class(dtype.get());
			
			if (type_class == H5T_STRING) {
				if (H5Tis_variable_str(dtype.get())) {
					// Variable-length strings
					std::vector<char*> str_buffer(count);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, str_buffer.data());
					
					for (idx_t i = 0; i < count; i++) {
						if (str_buffer[i]) {
							result.SetValue(i, Value(std::string(str_buffer[i])));
						} else {
							result.SetValue(i, Value());
						}
					}
					
					H5Dvlen_reclaim(dtype.get(), mem_space.get(), H5P_DEFAULT, str_buffer.data());
				} else {
					// Fixed-length strings
					size_t str_size = H5Tget_size(dtype.get());
					std::vector<char> buffer(count * str_size);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					
					for (idx_t i = 0; i < count; i++) {
						char *str_ptr = buffer.data() + i * str_size;
						size_t len = strnlen(str_ptr, str_size);
						result.SetValue(i, Value(std::string(str_ptr, len)));
					}
				}
			} else if (type_class == H5T_INTEGER) {
				// Read integer data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 1) {
					std::vector<int8_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::TINYINT(buffer[i]));
					}
				} else if (size <= 2) {
					std::vector<int16_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT16, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::SMALLINT(buffer[i]));
					}
				} else if (size <= 4) {
					std::vector<int32_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT32, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(buffer[i]));
					}
				} else {
					std::vector<int64_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT64, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(buffer[i]));
					}
				}
			} else if (type_class == H5T_FLOAT) {
				// Read floating-point data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 4) {
					std::vector<float> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_FLOAT, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(buffer[i]));
					}
				} else {
					std::vector<double> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(buffer[i]));
					}
				}
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to read var column '" + column_name + "': " + e.what());
	}
}

std::string H5ReaderNew::ReadVarColumnString(const std::string &column_name, idx_t index) {
	try {
		// Special handling for var_idx
		if (column_name == "var_idx") {
			return std::to_string(index);
		}
		
		Vector result(LogicalType::VARCHAR, 1);
		ReadVarColumn(column_name, result, index, 1);
		
		Value val = result.GetValue(0);
		if (val.IsNull()) {
			return "";
		}
		return val.ToString();
	} catch (...) {
		return "";
	}
}

std::string H5ReaderNew::GetCategoricalValue(const std::string &group_path, const std::string &column_name, idx_t index) {
	// TODO: Implement in Phase 6
	return "";
}

H5ReaderNew::XMatrixInfo H5ReaderNew::GetXMatrixInfo() {
	XMatrixInfo info;
	info.n_obs = GetObsCount();
	info.n_var = GetVarCount();
	
	// Check if X is sparse or dense
	if (H5LinkExists(file.get(), "/X")) {
		if (H5GetObjectType(file.get(), "/X") == H5O_TYPE_GROUP) {
			// Sparse matrix
			info.is_sparse = true;
			// Try to determine format (CSR vs CSC)
			if (IsDatasetPresent("/X", "indptr") && IsDatasetPresent("/X", "indices")) {
				// Check indptr length to determine format
				H5DatasetHandle indptr(file.get(), "/X/indptr");
				H5DataspaceHandle indptr_space(indptr.get());
				hsize_t indptr_dims[1];
				H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
				
				if (indptr_dims[0] == info.n_obs + 1) {
					info.sparse_format = "csr";
				} else if (indptr_dims[0] == info.n_var + 1) {
					info.sparse_format = "csc";
				} else {
					info.sparse_format = "unknown";
				}
			}
		} else {
			// Dense matrix
			info.is_sparse = false;
		}
	}
	
	return info;
}

// Helper function to check if an attribute exists
static bool H5AttributeExists(hid_t loc_id, const std::string &obj_name, const std::string &attr_name) {
	try {
		// Open the object (group or dataset)
		hid_t obj_id = H5Oopen(loc_id, obj_name.c_str(), H5P_DEFAULT);
		if (obj_id < 0) {
			return false;
		}
		
		// Check if attribute exists
		htri_t exists = H5Aexists(obj_id, attr_name.c_str());
		H5Oclose(obj_id);
		
		return exists > 0;
	} catch (...) {
		return false;
	}
}

std::vector<std::string> H5ReaderNew::GetVarNames(const std::string &column_name) {
	std::vector<std::string> names;
	try {
		auto var_count = GetVarCount();
		names.reserve(var_count);
		
		if (column_name.empty() || column_name == "var_names") {
			// Try to get default var names from _index attribute
			if (H5AttributeExists(file.get(), "/var", "_index")) {
				// Open the /var group
				H5GroupHandle var_group(file.get(), "/var");
				H5AttributeHandle attr(var_group.get(), "_index");
				
				// Get attribute dataspace
				hid_t attr_space_id = H5Aget_space(attr.get());
				H5DataspaceHandle attr_space = H5DataspaceHandle::from_handle(attr_space_id);
				
				hsize_t dims[1];
				H5Sget_simple_extent_dims(attr_space.get(), dims, nullptr);
				
				if (dims[0] == var_count) {
					H5TypeHandle dtype(attr.get(), H5TypeHandle::TypeClass::ATTRIBUTE);
					if (H5Tget_class(dtype.get()) == H5T_STRING) {
						if (H5Tis_variable_str(dtype.get())) {
							// Variable-length strings
							std::vector<char*> str_data(var_count);
							H5Aread(attr.get(), dtype.get(), str_data.data());
							for (idx_t i = 0; i < var_count; i++) {
								names.push_back(str_data[i] ? std::string(str_data[i]) : "");
							}
							H5Dvlen_reclaim(dtype.get(), attr_space.get(), H5P_DEFAULT, str_data.data());
						} else {
							// Fixed-length strings
							size_t str_size = H5Tget_size(dtype.get());
							std::vector<char> buffer(var_count * str_size);
							H5Aread(attr.get(), dtype.get(), buffer.data());
							
							for (idx_t i = 0; i < var_count; i++) {
								char *str_ptr = buffer.data() + i * str_size;
								size_t len = strnlen(str_ptr, str_size);
								names.push_back(std::string(str_ptr, len));
							}
						}
						return names;
					}
				}
			}
		} else {
			// Try to get specific column from var DataFrame
			std::string dataset_path = "/var/" + column_name;
			if (IsDatasetPresent("/var", column_name)) {
				H5DatasetHandle dataset(file.get(), dataset_path);
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[1];
				H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
				
				if (dims[0] == var_count) {
					H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
					if (H5Tget_class(dtype.get()) == H5T_STRING) {
						if (H5Tis_variable_str(dtype.get())) {
							// Variable-length strings
							std::vector<char*> str_data(var_count);
							H5Dread(dataset.get(), dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, str_data.data());
							for (idx_t i = 0; i < var_count; i++) {
								names.push_back(str_data[i] ? std::string(str_data[i]) : "");
							}
							H5Dvlen_reclaim(dtype.get(), dataspace.get(), H5P_DEFAULT, str_data.data());
						} else {
							// Fixed-length strings
							size_t str_size = H5Tget_size(dtype.get());
							std::vector<char> buffer(var_count * str_size);
							H5Dread(dataset.get(), dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
							
							for (idx_t i = 0; i < var_count; i++) {
								char *str_ptr = buffer.data() + i * str_size;
								size_t len = strnlen(str_ptr, str_size);
								names.push_back(std::string(str_ptr, len));
							}
						}
						return names;
					}
				}
			}
		}
		
		// Fallback: generate generic names
		for (idx_t i = 0; i < var_count; i++) {
			names.push_back("var_" + std::to_string(i));
		}
	} catch (const std::exception &e) {
		// On error, return generic names
		auto var_count = GetVarCount();
		for (idx_t i = 0; i < var_count; i++) {
			names.push_back("var_" + std::to_string(i));
		}
	}
	return names;
}

void H5ReaderNew::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadXMatrix not yet implemented in C API version");
}

void H5ReaderNew::ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	try {
		auto x_info = GetXMatrixInfo();
		
		if (x_info.is_sparse) {
			// For now, throw an exception for sparse matrices (Phase 5)
			throw NotImplementedException("Sparse matrix reading not yet implemented in C API version");
		}
		
		// Read dense matrix
		if (!H5LinkExists(file.get(), "/X")) {
			throw IOException("X matrix not found in file");
		}
		
		H5DatasetHandle dataset(file.get(), "/X");
		H5DataspaceHandle dataspace(dataset.get());
		
		// Get dimensions
		hsize_t dims[2];
		int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
		if (ndims != 2) {
			throw IOException("Expected 2D matrix for X");
		}
		
		// Validate bounds
		if (row_start + row_count > dims[0] || col_start + col_count > dims[1]) {
			throw IOException("Matrix read out of bounds");
		}
		
		// Set up hyperslab for partial read
		hsize_t h_offset[2] = {static_cast<hsize_t>(row_start), static_cast<hsize_t>(col_start)};
		hsize_t h_count[2] = {static_cast<hsize_t>(row_count), static_cast<hsize_t>(col_count)};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
		
		// Create memory dataspace
		H5DataspaceHandle mem_space(2, h_count);
		
		// Read data
		std::vector<double> buffer(row_count * col_count);
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());
		
		// Fill output DataChunk
		// The output should have columns: obs_id, var_id, var_name, value
		output.SetCardinality(row_count * col_count);
		
		auto &obs_id_vec = output.data[0];
		auto &var_id_vec = output.data[1];
		auto &var_name_vec = output.data[2];
		auto &value_vec = output.data[3];
		
		// Get var names for this batch
		auto all_var_names = GetVarNames("");
		
		idx_t out_idx = 0;
		for (idx_t row = 0; row < row_count; row++) {
			for (idx_t col = 0; col < col_count; col++) {
				idx_t global_row = row_start + row;
				idx_t global_col = col_start + col;
				
				obs_id_vec.SetValue(out_idx, Value::BIGINT(global_row));
				var_id_vec.SetValue(out_idx, Value::BIGINT(global_col));
				
				// Set var name if available
				if (global_col < all_var_names.size()) {
					var_name_vec.SetValue(out_idx, Value(all_var_names[global_col]));
				} else {
					var_name_vec.SetValue(out_idx, Value("var_" + std::to_string(global_col)));
				}
				
				// Set value from buffer (row-major layout)
				double val = buffer[row * col_count + col];
				value_vec.SetValue(out_idx, Value::DOUBLE(val));
				
				out_idx++;
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to read X matrix batch: " + std::string(e.what()));
	}
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseXMatrixCSR(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseXMatrixCSC(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

std::vector<H5ReaderNew::MatrixInfo> H5ReaderNew::GetObsmMatrices() {
	// TODO: Implement in Phase 6
	std::vector<MatrixInfo> matrices;
	return matrices;
}

std::vector<H5ReaderNew::MatrixInfo> H5ReaderNew::GetVarmMatrices() {
	// TODO: Implement in Phase 6
	std::vector<MatrixInfo> matrices;
	return matrices;
}

void H5ReaderNew::ReadObsmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx, Vector &result) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadObsmMatrix not yet implemented in C API version");
}

void H5ReaderNew::ReadVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx, Vector &result) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadVarmMatrix not yet implemented in C API version");
}

std::vector<H5ReaderNew::LayerInfo> H5ReaderNew::GetLayers() {
	// TODO: Implement in Phase 6
	std::vector<LayerInfo> layers;
	return layers;
}

void H5ReaderNew::ReadLayerMatrix(const std::string &layer_name, idx_t row_idx, idx_t start_col, idx_t count, DataChunk &output, const std::vector<std::string> &var_names) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadLayerMatrix not yet implemented in C API version");
}

void H5ReaderNew::ReadLayerMatrixBatch(const std::string &layer_name, idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadLayerMatrixBatch not yet implemented in C API version");
}

void H5ReaderNew::ReadMatrixBatch(const std::string &path, idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output, bool is_layer) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadMatrixBatch not yet implemented in C API version");
}

std::vector<H5ReaderNew::UnsInfo> H5ReaderNew::GetUnsKeys() {
	// TODO: Implement in Phase 6
	std::vector<UnsInfo> keys;
	return keys;
}

Value H5ReaderNew::ReadUnsScalar(const std::string &key) {
	// TODO: Implement in Phase 6
	return Value();
}

void H5ReaderNew::ReadUnsArray(const std::string &key, Vector &result, idx_t offset, idx_t count) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadUnsArray not yet implemented in C API version");
}

std::vector<std::string> H5ReaderNew::GetObspKeys() {
	// TODO: Implement in Phase 6
	std::vector<std::string> keys;
	return keys;
}

std::vector<std::string> H5ReaderNew::GetVarpKeys() {
	// TODO: Implement in Phase 6
	std::vector<std::string> keys;
	return keys;
}

H5ReaderNew::SparseMatrixInfo H5ReaderNew::GetObspMatrixInfo(const std::string &key) {
	// TODO: Implement in Phase 6
	SparseMatrixInfo info;
	return info;
}

H5ReaderNew::SparseMatrixInfo H5ReaderNew::GetVarpMatrixInfo(const std::string &key) {
	// TODO: Implement in Phase 6
	SparseMatrixInfo info;
	return info;
}

void H5ReaderNew::ReadObspMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result, idx_t offset, idx_t count) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadObspMatrix not yet implemented in C API version");
}

void H5ReaderNew::ReadVarpMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result, idx_t offset, idx_t count) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadVarpMatrix not yet implemented in C API version");
}

// Static helper methods
void H5ReaderNew::SetTypedValue(Vector &vec, idx_t row, double value) {
	switch (vec.GetType().id()) {
	case LogicalTypeId::TINYINT:
		vec.SetValue(row, Value::TINYINT(static_cast<int8_t>(value)));
		break;
	case LogicalTypeId::SMALLINT:
		vec.SetValue(row, Value::SMALLINT(static_cast<int16_t>(value)));
		break;
	case LogicalTypeId::INTEGER:
		vec.SetValue(row, Value::INTEGER(static_cast<int32_t>(value)));
		break;
	case LogicalTypeId::BIGINT:
		vec.SetValue(row, Value::BIGINT(static_cast<int64_t>(value)));
		break;
	case LogicalTypeId::FLOAT:
		vec.SetValue(row, Value::FLOAT(static_cast<float>(value)));
		break;
	case LogicalTypeId::DOUBLE:
		vec.SetValue(row, Value::DOUBLE(value));
		break;
	default:
		vec.SetValue(row, Value::DOUBLE(value));
		break;
	}
}

void H5ReaderNew::InitializeZeros(Vector &vec, idx_t count) {
	switch (vec.GetType().id()) {
	case LogicalTypeId::TINYINT:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::TINYINT(0));
		}
		break;
	case LogicalTypeId::SMALLINT:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::SMALLINT(0));
		}
		break;
	case LogicalTypeId::INTEGER:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::INTEGER(0));
		}
		break;
	case LogicalTypeId::BIGINT:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::BIGINT(0));
		}
		break;
	case LogicalTypeId::FLOAT:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::FLOAT(0.0f));
		}
		break;
	case LogicalTypeId::DOUBLE:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::DOUBLE(0.0));
		}
		break;
	default:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value());
		}
		break;
	}
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseMatrixAtPath(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseMatrixCSR(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

H5ReaderNew::SparseMatrixData H5ReaderNew::ReadSparseMatrixCSC(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// TODO: Implement in Phase 5
	SparseMatrixData data;
	return data;
}

void H5ReaderNew::ReadDenseMatrix(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values) {
	try {
		// Open the dataset
		H5DatasetHandle dataset(file.get(), path);
		H5DataspaceHandle dataspace(dataset.get());
		
		// Get dimensions
		hsize_t dims[2];
		int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
		if (ndims != 2) {
			throw IOException("Expected 2D matrix at " + path);
		}
		
		// Set up hyperslab for partial read
		hsize_t h_offset[2] = {static_cast<hsize_t>(obs_start), static_cast<hsize_t>(var_start)};
		hsize_t h_count[2] = {static_cast<hsize_t>(obs_count), static_cast<hsize_t>(var_count)};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
		
		// Create memory dataspace
		H5DataspaceHandle mem_space(2, h_count);
		
		// Resize output vector
		values.resize(obs_count * var_count);
		
		// Read data as double
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT, values.data());
	} catch (const std::exception &e) {
		throw IOException("Failed to read dense matrix at " + path + ": " + e.what());
	}
}


} // namespace duckdb
