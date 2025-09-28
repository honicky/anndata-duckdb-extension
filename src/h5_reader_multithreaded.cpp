#include "include/h5_reader_multithreaded.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <cstring>
#include <unordered_set>
#include <algorithm>

namespace duckdb {

// ============================================================================
// Phase 2: Core Infrastructure - Constructor/Destructor and Helper Methods
// ============================================================================

H5ReaderMultithreaded::H5ReaderMultithreaded(const std::string &file_path) : file_path(file_path), file() {
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

H5ReaderMultithreaded::~H5ReaderMultithreaded() {
	// Destructor automatically handled by H5FileHandle RAII
}

// Helper method to check if a group exists
bool H5ReaderMultithreaded::IsGroupPresent(const std::string &group_name) {
	if (!H5LinkExists(file.get(), group_name)) {
		return false;
	}
	
	// Check if it's actually a group (not a dataset)
	return H5GetObjectType(file.get(), group_name) == H5O_TYPE_GROUP;
}

// Helper method to check if a dataset exists within a group
bool H5ReaderMultithreaded::IsDatasetPresent(const std::string &group_name, const std::string &dataset_name) {
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
std::vector<std::string> H5ReaderMultithreaded::GetGroupMembers(const std::string &group_name) {
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
LogicalType H5ReaderMultithreaded::H5TypeToDuckDBType(hid_t h5_type) {
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
bool H5ReaderMultithreaded::IsValidAnnData() {
	// Check for required groups: /obs, /var, and either /X group or dataset
	return IsGroupPresent("/obs") && IsGroupPresent("/var") && 
	       (IsGroupPresent("/X") || H5LinkExists(file.get(), "/X"));
}

// Get number of observations (cells)
size_t H5ReaderMultithreaded::GetObsCount() {
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
size_t H5ReaderMultithreaded::GetVarCount() {
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

std::vector<H5ReaderMultithreaded::ColumnInfo> H5ReaderMultithreaded::GetObsColumns() {
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

std::vector<H5ReaderMultithreaded::ColumnInfo> H5ReaderMultithreaded::GetVarColumns() {
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

void H5ReaderMultithreaded::ReadObsColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
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

void H5ReaderMultithreaded::ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
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

std::string H5ReaderMultithreaded::ReadVarColumnString(const std::string &column_name, idx_t index) {
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

std::string H5ReaderMultithreaded::GetCategoricalValue(const std::string &group_path, const std::string &column_name, idx_t index) {
	// TODO: Implement in Phase 6
	return "";
}

H5ReaderMultithreaded::XMatrixInfo H5ReaderMultithreaded::GetXMatrixInfo() {
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

std::vector<std::string> H5ReaderMultithreaded::GetVarNames(const std::string &column_name) {
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

void H5ReaderMultithreaded::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadXMatrix not yet implemented in C API version");
}

void H5ReaderMultithreaded::ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	try {
		auto x_info = GetXMatrixInfo();
		
		if (x_info.is_sparse) {
			// Handle sparse matrix
			auto sparse_data = ReadSparseXMatrix(row_start, row_count, col_start, col_count);
			
			// Convert sparse data to dense format for output
			output.SetCardinality(row_count);
			
			// Set obs_idx column (first column)
			auto &obs_idx_vec = output.data[0];
			for (idx_t row = 0; row < row_count; row++) {
				obs_idx_vec.SetValue(row, Value::BIGINT(row_start + row));
			}
			
			// Initialize all gene columns with zeros
			for (idx_t col = 0; col < col_count; col++) {
				auto &gene_vec = output.data[col + 1];
				for (idx_t row = 0; row < row_count; row++) {
					gene_vec.SetValue(row, Value::DOUBLE(0.0));
				}
			}
			
			// Fill in non-zero values from sparse data
			for (size_t i = 0; i < sparse_data.row_indices.size(); i++) {
				idx_t row = sparse_data.row_indices[i];
				idx_t col = sparse_data.col_indices[i];
				double val = sparse_data.values[i];
				
				// Check if this value is within our requested range
				if (row >= row_start && row < row_start + row_count &&
				    col >= col_start && col < col_start + col_count) {
					idx_t local_row = row - row_start;
					idx_t local_col = col - col_start;
					auto &gene_vec = output.data[local_col + 1];
					gene_vec.SetValue(local_row, Value::DOUBLE(val));
				}
			}
			return;
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
		
		// Fill output DataChunk in WIDE format
		// The output should have columns: obs_idx + one column per gene
		// First column is obs_idx, rest are gene values
		output.SetCardinality(row_count);
		
		// Set obs_idx column (first column)
		auto &obs_idx_vec = output.data[0];
		for (idx_t row = 0; row < row_count; row++) {
			obs_idx_vec.SetValue(row, Value::BIGINT(row_start + row));
		}
		
		// Set gene value columns (one column per gene)
		for (idx_t col = 0; col < col_count; col++) {
			auto &gene_vec = output.data[col + 1]; // +1 because first column is obs_idx
			for (idx_t row = 0; row < row_count; row++) {
				// Set value from buffer (row-major layout)
				double val = buffer[row * col_count + col];
				gene_vec.SetValue(row, Value::DOUBLE(val));
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to read X matrix batch: " + std::string(e.what()));
	}
}

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	// Detect the sparse matrix format and dispatch to appropriate reader
	auto x_info = GetXMatrixInfo();
	if (!x_info.is_sparse) {
		throw IOException("X matrix is not sparse");
	}
	
	if (x_info.sparse_format == "csr") {
		return ReadSparseXMatrixCSR(obs_start, obs_count, var_start, var_count);
	} else if (x_info.sparse_format == "csc") {
		return ReadSparseXMatrixCSC(obs_start, obs_count, var_start, var_count);
	} else {
		throw NotImplementedException("Unsupported sparse matrix format: " + x_info.sparse_format);
	}
}

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrixCSR(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	return ReadSparseMatrixCSR("/X", obs_start, obs_count, var_start, var_count);
}

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrixCSC(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	return ReadSparseMatrixCSC("/X", obs_start, obs_count, var_start, var_count);
}

std::vector<H5ReaderMultithreaded::MatrixInfo> H5ReaderMultithreaded::GetObsmMatrices() {
	std::vector<MatrixInfo> matrices;
	
	try {
		if (IsGroupPresent("/obsm")) {
			// Get list of members in the obsm group
			auto members = GetGroupMembers("/obsm");
			
			for (const auto &matrix_name : members) {
				// Check if it's a dataset
				std::string path = "/obsm/" + matrix_name;
				if (H5LinkExists(file.get(), path) && H5GetObjectType(file.get(), path) == H5O_TYPE_DATASET) {
					// Open dataset and get dimensions
					H5DatasetHandle dataset(file.get(), path);
					H5DataspaceHandle dataspace(dataset.get());
					
					int ndims = H5Sget_simple_extent_ndims(dataspace.get());
					if (ndims == 2) {
						hsize_t dims[2];
						H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
						
						MatrixInfo info;
						info.name = matrix_name;
						info.rows = dims[0];
						info.cols = dims[1];
						
						// Get data type
						H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
						H5T_class_t type_class = H5Tget_class(dtype.get());
						size_t type_size = H5Tget_size(dtype.get());
						
						if (type_class == H5T_FLOAT) {
							info.dtype = (type_size <= 4) ? LogicalType::FLOAT : LogicalType::DOUBLE;
						} else if (type_class == H5T_INTEGER) {
							info.dtype = (type_size <= 4) ? LogicalType::INTEGER : LogicalType::BIGINT;
						} else {
							info.dtype = LogicalType::DOUBLE; // Default
						}
						
						matrices.push_back(info);
					}
				}
			}
		}
	} catch (const std::exception &e) {
		// Return empty list on error
	}
	
	return matrices;
}

std::vector<H5ReaderMultithreaded::MatrixInfo> H5ReaderMultithreaded::GetVarmMatrices() {
	std::vector<MatrixInfo> matrices;
	
	try {
		if (IsGroupPresent("/varm")) {
			// Get list of members in the varm group
			auto members = GetGroupMembers("/varm");
			
			for (const auto &matrix_name : members) {
				// Check if it's a dataset
				std::string path = "/varm/" + matrix_name;
				if (H5LinkExists(file.get(), path) && H5GetObjectType(file.get(), path) == H5O_TYPE_DATASET) {
					// Open dataset and get dimensions
					H5DatasetHandle dataset(file.get(), path);
					H5DataspaceHandle dataspace(dataset.get());
					
					int ndims = H5Sget_simple_extent_ndims(dataspace.get());
					if (ndims == 2) {
						hsize_t dims[2];
						H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
						
						MatrixInfo info;
						info.name = matrix_name;
						info.rows = dims[0];
						info.cols = dims[1];
						
						// Get data type
						H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
						H5T_class_t type_class = H5Tget_class(dtype.get());
						size_t type_size = H5Tget_size(dtype.get());
						
						if (type_class == H5T_FLOAT) {
							info.dtype = (type_size <= 4) ? LogicalType::FLOAT : LogicalType::DOUBLE;
						} else if (type_class == H5T_INTEGER) {
							info.dtype = (type_size <= 4) ? LogicalType::INTEGER : LogicalType::BIGINT;
						} else {
							info.dtype = LogicalType::DOUBLE; // Default
						}
						
						matrices.push_back(info);
					}
				}
			}
		}
	} catch (const std::exception &e) {
		// Return empty list on error
	}
	
	return matrices;
}

void H5ReaderMultithreaded::ReadObsmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx, Vector &result) {
	try {
		std::string dataset_path = "/obsm/" + matrix_name;
		H5DatasetHandle dataset(file.get(), dataset_path);
		H5DataspaceHandle dataspace(dataset.get());
		
		// Get dimensions
		hsize_t dims[2];
		H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
		
		// Ensure col_idx is valid
		if (col_idx >= dims[1]) {
			throw InvalidInputException("Column index out of bounds for obsm matrix %s", matrix_name.c_str());
		}
		
		// Select hyperslab for the requested column
		hsize_t offset[2] = {static_cast<hsize_t>(row_start), static_cast<hsize_t>(col_idx)};
		hsize_t count[2] = {static_cast<hsize_t>(row_count), 1};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, offset, nullptr, count, nullptr);
		
		// Create memory dataspace
		hsize_t mem_dims[1] = {static_cast<hsize_t>(row_count)};
		H5DataspaceHandle memspace(1, mem_dims);
		
		// Read data based on type
		H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
		H5T_class_t type_class = H5Tget_class(dtype.get());
		size_t type_size = H5Tget_size(dtype.get());
		
		if (type_class == H5T_FLOAT) {
			if (type_size <= 4) {
				std::vector<float> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_FLOAT, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::FLOAT(values[i]));
				}
			} else {
				std::vector<double> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::DOUBLE(values[i]));
				}
			}
		} else if (type_class == H5T_INTEGER) {
			if (type_size <= 4) {
				std::vector<int32_t> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_INT32, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::INTEGER(values[i]));
				}
			} else {
				std::vector<int64_t> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_INT64, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::BIGINT(values[i]));
				}
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to read obsm matrix %s: %s", matrix_name.c_str(), e.what());
	}
}

void H5ReaderMultithreaded::ReadVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx, Vector &result) {
	try {
		std::string dataset_path = "/varm/" + matrix_name;
		H5DatasetHandle dataset(file.get(), dataset_path);
		H5DataspaceHandle dataspace(dataset.get());
		
		// Get dimensions
		hsize_t dims[2];
		H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
		
		// Ensure col_idx is valid
		if (col_idx >= dims[1]) {
			throw InvalidInputException("Column index out of bounds for varm matrix %s", matrix_name.c_str());
		}
		
		// Select hyperslab for the requested column
		hsize_t offset[2] = {static_cast<hsize_t>(row_start), static_cast<hsize_t>(col_idx)};
		hsize_t count[2] = {static_cast<hsize_t>(row_count), 1};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, offset, nullptr, count, nullptr);
		
		// Create memory dataspace
		hsize_t mem_dims[1] = {static_cast<hsize_t>(row_count)};
		H5DataspaceHandle memspace(1, mem_dims);
		
		// Read data based on type
		H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
		H5T_class_t type_class = H5Tget_class(dtype.get());
		size_t type_size = H5Tget_size(dtype.get());
		
		if (type_class == H5T_FLOAT) {
			if (type_size <= 4) {
				std::vector<float> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_FLOAT, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::FLOAT(values[i]));
				}
			} else {
				std::vector<double> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::DOUBLE(values[i]));
				}
			}
		} else if (type_class == H5T_INTEGER) {
			if (type_size <= 4) {
				std::vector<int32_t> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_INT32, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::INTEGER(values[i]));
				}
			} else {
				std::vector<int64_t> values(row_count);
				H5Dread(dataset.get(), H5T_NATIVE_INT64, memspace.get(), dataspace.get(), H5P_DEFAULT, values.data());
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::BIGINT(values[i]));
				}
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to read varm matrix %s: %s", matrix_name.c_str(), e.what());
	}
}

std::vector<H5ReaderMultithreaded::LayerInfo> H5ReaderMultithreaded::GetLayers() {
	// TODO: Implement in Phase 6
	std::vector<LayerInfo> layers;
	return layers;
}

void H5ReaderMultithreaded::ReadLayerMatrix(const std::string &layer_name, idx_t row_idx, idx_t start_col, idx_t count, DataChunk &output, const std::vector<std::string> &var_names) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadLayerMatrix not yet implemented in C API version");
}

void H5ReaderMultithreaded::ReadLayerMatrixBatch(const std::string &layer_name, idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadLayerMatrixBatch not yet implemented in C API version");
}

void H5ReaderMultithreaded::ReadMatrixBatch(const std::string &path, idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output, bool is_layer) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadMatrixBatch not yet implemented in C API version");
}

std::vector<H5ReaderMultithreaded::UnsInfo> H5ReaderMultithreaded::GetUnsKeys() {
	// TODO: Implement in Phase 6
	std::vector<UnsInfo> keys;
	return keys;
}

Value H5ReaderMultithreaded::ReadUnsScalar(const std::string &key) {
	// TODO: Implement in Phase 6
	return Value();
}

void H5ReaderMultithreaded::ReadUnsArray(const std::string &key, Vector &result, idx_t offset, idx_t count) {
	// TODO: Implement in Phase 6
	throw NotImplementedException("ReadUnsArray not yet implemented in C API version");
}

std::vector<std::string> H5ReaderMultithreaded::GetObspKeys() {
	std::vector<std::string> keys;
	
	// Check if obsp group exists
	if (!IsGroupPresent("/obsp")) {
		return keys;
	}
	
	try {
		// Get list of members in the obsp group
		auto members = GetGroupMembers("/obsp");
		
		for (const auto &key_name : members) {
			// obsp matrices are stored as groups (sparse format)
			std::string path = "/obsp/" + key_name;
			if (H5LinkExists(file.get(), path) && H5GetObjectType(file.get(), path) == H5O_TYPE_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const std::exception &e) {
		// Return what we have
	}
	
	return keys;
}

std::vector<std::string> H5ReaderMultithreaded::GetVarpKeys() {
	std::vector<std::string> keys;
	
	// Check if varp group exists
	if (!IsGroupPresent("/varp")) {
		return keys;
	}
	
	try {
		// Get list of members in the varp group
		auto members = GetGroupMembers("/varp");
		
		for (const auto &key_name : members) {
			// varp matrices are stored as groups (sparse format)
			std::string path = "/varp/" + key_name;
			if (H5LinkExists(file.get(), path) && H5GetObjectType(file.get(), path) == H5O_TYPE_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const std::exception &e) {
		// Return what we have
	}
	
	return keys;
}

H5ReaderMultithreaded::SparseMatrixInfo H5ReaderMultithreaded::GetObspMatrixInfo(const std::string &key) {
	SparseMatrixInfo info;
	std::string matrix_path = "/obsp/" + key;
	
	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}
	
	try {
		// Check for indptr to determine format
		if (IsDatasetPresent(matrix_path, "indptr")) {
			H5DatasetHandle indptr_ds(file.get(), matrix_path + "/indptr");
			H5DataspaceHandle indptr_space(indptr_ds.get());
			hsize_t indptr_dims[1];
			H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
			
			// Get obs count to determine if CSR or CSC
			idx_t n_obs = GetObsCount();
			
			if (indptr_dims[0] == static_cast<hsize_t>(n_obs + 1)) {
				// CSR format
				info.format = "csr";
				info.nrows = n_obs;
				info.ncols = n_obs; // obsp is obs x obs
			} else {
				// CSC format
				info.format = "csc";
				info.nrows = n_obs;
				info.ncols = n_obs;
			}
			
			// Get number of non-zero elements
			if (IsDatasetPresent(matrix_path, "data")) {
				H5DatasetHandle data_ds(file.get(), matrix_path + "/data");
				H5DataspaceHandle data_space(data_ds.get());
				hsize_t data_dims[1];
				H5Sget_simple_extent_dims(data_space.get(), data_dims, nullptr);
				info.nnz = data_dims[0];
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to get obsp matrix info for '%s': %s", key.c_str(), e.what());
	}
	
	return info;
}

H5ReaderMultithreaded::SparseMatrixInfo H5ReaderMultithreaded::GetVarpMatrixInfo(const std::string &key) {
	SparseMatrixInfo info;
	std::string matrix_path = "/varp/" + key;
	
	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}
	
	try {
		// Check for indptr to determine format
		if (IsDatasetPresent(matrix_path, "indptr")) {
			H5DatasetHandle indptr_ds(file.get(), matrix_path + "/indptr");
			H5DataspaceHandle indptr_space(indptr_ds.get());
			hsize_t indptr_dims[1];
			H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
			
			// Get var count to determine if CSR or CSC
			idx_t n_var = GetVarCount();
			
			if (indptr_dims[0] == static_cast<hsize_t>(n_var + 1)) {
				// CSR format
				info.format = "csr";
				info.nrows = n_var;
				info.ncols = n_var; // varp is var x var
			} else {
				// CSC format
				info.format = "csc";
				info.nrows = n_var;
				info.ncols = n_var;
			}
			
			// Get number of non-zero elements
			if (IsDatasetPresent(matrix_path, "data")) {
				H5DatasetHandle data_ds(file.get(), matrix_path + "/data");
				H5DataspaceHandle data_space(data_ds.get());
				hsize_t data_dims[1];
				H5Sget_simple_extent_dims(data_space.get(), data_dims, nullptr);
				info.nnz = data_dims[0];
			}
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to get varp matrix info for '%s': %s", key.c_str(), e.what());
	}
	
	return info;
}

void H5ReaderMultithreaded::ReadObspMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result, idx_t offset, idx_t count) {
	std::string matrix_path = "/obsp/" + key;
	
	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}
	
	try {
		SparseMatrixInfo info = GetObspMatrixInfo(key);
		
		// Read the sparse matrix components
		H5DatasetHandle data_ds(file.get(), matrix_path + "/data");
		H5DatasetHandle indices_ds(file.get(), matrix_path + "/indices");
		H5DatasetHandle indptr_ds(file.get(), matrix_path + "/indptr");
		
		if (info.format == "csr") {
			// Read all indptr to find which rows contain our offset range
			H5DataspaceHandle indptr_space(indptr_ds.get());
			hsize_t indptr_dims[1];
			H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
			std::vector<int32_t> indptr(indptr_dims[0]);
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, indptr.data());
			
			// Find which non-zero elements to read
			idx_t current_nnz = 0;
			idx_t result_idx = 0;
			
			for (idx_t row = 0; row < info.nrows && result_idx < count; row++) {
				idx_t row_start = indptr[row];
				idx_t row_end = indptr[row + 1];
				
				for (idx_t j = row_start; j < row_end && result_idx < count; j++) {
					if (current_nnz >= offset) {
						// Read column index
						int32_t col_idx;
						hsize_t indices_offset[1] = {j};
						hsize_t indices_count[1] = {1};
						H5DataspaceHandle indices_mem_space(1, indices_count);
						H5DataspaceHandle indices_file_space(indices_ds.get());
						H5Sselect_hyperslab(indices_file_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
						H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_file_space.get(), H5P_DEFAULT, &col_idx);
						
						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5DataspaceHandle data_mem_space(1, data_count);
						H5DataspaceHandle data_file_space(data_ds.get());
						H5Sselect_hyperslab(data_file_space.get(), H5S_SELECT_SET, data_offset, nullptr, data_count, nullptr);
						H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_file_space.get(), H5P_DEFAULT, &val);
						
						// Set results
						row_result.SetValue(result_idx, Value::BIGINT(row));
						col_result.SetValue(result_idx, Value::BIGINT(col_idx));
						value_result.SetValue(result_idx, Value::FLOAT(val));
						
						result_idx++;
					}
					current_nnz++;
				}
			}
		} else {
			// CSC format - similar but iterate by columns
			throw NotImplementedException("CSC format for obsp not yet implemented");
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to read obsp matrix '%s': %s", key.c_str(), e.what());
	}
}

void H5ReaderMultithreaded::ReadVarpMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result, idx_t offset, idx_t count) {
	std::string matrix_path = "/varp/" + key;
	
	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}
	
	try {
		SparseMatrixInfo info = GetVarpMatrixInfo(key);
		
		// Read the sparse matrix components
		H5DatasetHandle data_ds(file.get(), matrix_path + "/data");
		H5DatasetHandle indices_ds(file.get(), matrix_path + "/indices");
		H5DatasetHandle indptr_ds(file.get(), matrix_path + "/indptr");
		
		if (info.format == "csr") {
			// Read all indptr to find which rows contain our offset range
			H5DataspaceHandle indptr_space(indptr_ds.get());
			hsize_t indptr_dims[1];
			H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
			std::vector<int32_t> indptr(indptr_dims[0]);
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, indptr.data());
			
			// Find which non-zero elements to read
			idx_t current_nnz = 0;
			idx_t result_idx = 0;
			
			for (idx_t row = 0; row < info.nrows && result_idx < count; row++) {
				idx_t row_start = indptr[row];
				idx_t row_end = indptr[row + 1];
				
				for (idx_t j = row_start; j < row_end && result_idx < count; j++) {
					if (current_nnz >= offset) {
						// Read column index
						int32_t col_idx;
						hsize_t indices_offset[1] = {j};
						hsize_t indices_count[1] = {1};
						H5DataspaceHandle indices_mem_space(1, indices_count);
						H5DataspaceHandle indices_file_space(indices_ds.get());
						H5Sselect_hyperslab(indices_file_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
						H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_file_space.get(), H5P_DEFAULT, &col_idx);
						
						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5DataspaceHandle data_mem_space(1, data_count);
						H5DataspaceHandle data_file_space(data_ds.get());
						H5Sselect_hyperslab(data_file_space.get(), H5S_SELECT_SET, data_offset, nullptr, data_count, nullptr);
						H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_file_space.get(), H5P_DEFAULT, &val);
						
						// Set results
						row_result.SetValue(result_idx, Value::BIGINT(row));
						col_result.SetValue(result_idx, Value::BIGINT(col_idx));
						value_result.SetValue(result_idx, Value::FLOAT(val));
						
						result_idx++;
					}
					current_nnz++;
				}
			}
		} else {
			// CSC format - similar but iterate by columns
			throw NotImplementedException("CSC format for varp not yet implemented");
		}
	} catch (const std::exception &e) {
		throw InvalidInputException("Failed to read varp matrix '%s': %s", key.c_str(), e.what());
	}
}

// Helper methods for sparse matrix reading
H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseMatrixCSR(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;
	
	try {
		// Read CSR components from the specified path
		H5DatasetHandle data_ds(file.get(), path + "/data");
		H5DatasetHandle indices_ds(file.get(), path + "/indices");
		H5DatasetHandle indptr_ds(file.get(), path + "/indptr");
		
		// Get dataspace for indptr
		H5DataspaceHandle indptr_space(indptr_ds.get());
		hsize_t indptr_dims[1];
		H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
		
		// Read indptr for the requested observation range
		std::vector<int64_t> indptr(obs_count + 1);
		hsize_t indptr_offset[1] = {static_cast<hsize_t>(obs_start)};
		hsize_t indptr_count[1] = {static_cast<hsize_t>(obs_count + 1)};
		
		H5Sselect_hyperslab(indptr_space.get(), H5S_SELECT_SET, indptr_offset, nullptr, indptr_count, nullptr);
		H5DataspaceHandle indptr_mem_space(1, indptr_count);
		
		// Check indptr datatype size and read accordingly
		H5TypeHandle indptr_dtype(indptr_ds.get(), H5TypeHandle::TypeClass::DATASET);
		size_t dtype_size = H5Tget_size(indptr_dtype.get());
		
		if (dtype_size <= 4) {
			std::vector<int32_t> indptr32(obs_count + 1);
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT, indptr32.data());
			for (size_t i = 0; i < indptr32.size(); i++) {
				indptr[i] = indptr32[i];
			}
		} else {
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT64, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT, indptr.data());
		}
		
		// For each observation, read its sparse data
		for (idx_t obs_idx = 0; obs_idx < obs_count; obs_idx++) {
			int64_t row_start_idx = indptr[obs_idx];
			int64_t row_end_idx = indptr[obs_idx + 1];
			int64_t nnz = row_end_idx - row_start_idx;
			
			if (nnz == 0) continue;
			
			// Read column indices
			std::vector<int32_t> col_indices(nnz);
			hsize_t indices_offset[1] = {static_cast<hsize_t>(row_start_idx)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
			
			H5DataspaceHandle indices_space(indices_ds.get());
			H5Sselect_hyperslab(indices_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle indices_mem_space(1, indices_count);
			H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_space.get(), H5P_DEFAULT, col_indices.data());
			
			// Read data values
			std::vector<double> row_data(nnz);
			H5DataspaceHandle data_space(data_ds.get());
			H5Sselect_hyperslab(data_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle data_mem_space(1, indices_count);
			
			// Check data type and read accordingly
			H5TypeHandle data_dtype(data_ds.get(), H5TypeHandle::TypeClass::DATASET);
			H5T_class_t type_class = H5Tget_class(data_dtype.get());
			
			if (type_class == H5T_FLOAT) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<float> float_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_space.get(), H5P_DEFAULT, float_data.data());
					for (size_t i = 0; i < float_data.size(); i++) {
						row_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					H5Dread(data_ds.get(), H5T_NATIVE_DOUBLE, data_mem_space.get(), data_space.get(), H5P_DEFAULT, row_data.data());
				}
			} else if (type_class == H5T_INTEGER) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<int32_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT32, data_mem_space.get(), data_space.get(), H5P_DEFAULT, int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						row_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT64, data_mem_space.get(), data_space.get(), H5P_DEFAULT, int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						row_data[i] = static_cast<double>(int_data[i]);
					}
				}
			}
			
			// Add to sparse data structure only values in the requested var range
			for (size_t i = 0; i < col_indices.size(); i++) {
				int32_t col = col_indices[i];
				if (col >= var_start && col < var_start + var_count) {
					sparse_data.row_indices.push_back(obs_start + obs_idx);
					sparse_data.col_indices.push_back(col);
					sparse_data.values.push_back(row_data[i]);
				}
			}
		}
	} catch (const std::exception &e) {
		// Return empty sparse data on error
	}
	
	return sparse_data;
}

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseMatrixCSC(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;
	
	try {
		// Read CSC components from the specified path
		H5DatasetHandle data_ds(file.get(), path + "/data");
		H5DatasetHandle indices_ds(file.get(), path + "/indices");
		H5DatasetHandle indptr_ds(file.get(), path + "/indptr");
		
		// Get dataspace for indptr
		H5DataspaceHandle indptr_space(indptr_ds.get());
		hsize_t indptr_dims[1];
		H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);
		size_t total_var = indptr_dims[0] - 1; // indptr has n_var + 1 elements for CSC
		
		// For CSC, iterate through requested columns
		for (idx_t var_idx = var_start; var_idx < var_start + var_count && var_idx < total_var; var_idx++) {
			// Read indptr for this column
			std::vector<int64_t> col_indptr(2);
			hsize_t indptr_offset[1] = {static_cast<hsize_t>(var_idx)};
			hsize_t indptr_count[1] = {2};
			
			H5Sselect_hyperslab(indptr_space.get(), H5S_SELECT_SET, indptr_offset, nullptr, indptr_count, nullptr);
			H5DataspaceHandle indptr_mem_space(1, indptr_count);
			
			// Check indptr datatype size
			H5TypeHandle indptr_dtype(indptr_ds.get(), H5TypeHandle::TypeClass::DATASET);
			size_t dtype_size = H5Tget_size(indptr_dtype.get());
			
			if (dtype_size <= 4) {
				std::vector<int32_t> indptr32(2);
				H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT, indptr32.data());
				col_indptr[0] = indptr32[0];
				col_indptr[1] = indptr32[1];
			} else {
				H5Dread(indptr_ds.get(), H5T_NATIVE_INT64, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT, col_indptr.data());
			}
			
			int64_t col_start_idx = col_indptr[0];
			int64_t col_end_idx = col_indptr[1];
			int64_t nnz = col_end_idx - col_start_idx;
			
			if (nnz == 0) continue;
			
			// Read row indices
			std::vector<int32_t> row_indices(nnz);
			hsize_t indices_offset[1] = {static_cast<hsize_t>(col_start_idx)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
			
			H5DataspaceHandle indices_space(indices_ds.get());
			H5Sselect_hyperslab(indices_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle indices_mem_space(1, indices_count);
			H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_space.get(), H5P_DEFAULT, row_indices.data());
			
			// Read data values
			std::vector<double> col_data(nnz);
			H5DataspaceHandle data_space(data_ds.get());
			H5Sselect_hyperslab(data_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle data_mem_space(1, indices_count);
			
			// Check data type and read accordingly
			H5TypeHandle data_dtype(data_ds.get(), H5TypeHandle::TypeClass::DATASET);
			H5T_class_t type_class = H5Tget_class(data_dtype.get());
			
			if (type_class == H5T_FLOAT) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<float> float_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_space.get(), H5P_DEFAULT, float_data.data());
					for (size_t i = 0; i < float_data.size(); i++) {
						col_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					H5Dread(data_ds.get(), H5T_NATIVE_DOUBLE, data_mem_space.get(), data_space.get(), H5P_DEFAULT, col_data.data());
				}
			} else if (type_class == H5T_INTEGER) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<int32_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT32, data_mem_space.get(), data_space.get(), H5P_DEFAULT, int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						col_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT64, data_mem_space.get(), data_space.get(), H5P_DEFAULT, int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						col_data[i] = static_cast<double>(int_data[i]);
					}
				}
			}
			
			// Add to sparse data structure only values in the requested obs range
			for (size_t i = 0; i < row_indices.size(); i++) {
				int32_t row = row_indices[i];
				if (row >= obs_start && row < obs_start + obs_count) {
					sparse_data.row_indices.push_back(row);
					sparse_data.col_indices.push_back(var_idx);
					sparse_data.values.push_back(col_data[i]);
				}
			}
		}
	} catch (const std::exception &e) {
		// Return empty sparse data on error
	}
	
	return sparse_data;
}

// Static helper methods
void H5ReaderMultithreaded::SetTypedValue(Vector &vec, idx_t row, double value) {
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

void H5ReaderMultithreaded::InitializeZeros(Vector &vec, idx_t count) {
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



void H5ReaderMultithreaded::ReadDenseMatrix(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values) {
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
