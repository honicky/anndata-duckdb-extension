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
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadObsColumn not yet implemented in C API version");
}

void H5ReaderNew::ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadVarColumn not yet implemented in C API version");
}

std::string H5ReaderNew::ReadVarColumnString(const std::string &column_name, idx_t index) {
	// TODO: Implement in Phase 4
	return "";
}

std::string H5ReaderNew::GetCategoricalValue(const std::string &group_path, const std::string &column_name, idx_t index) {
	// TODO: Implement in Phase 6
	return "";
}

H5ReaderNew::XMatrixInfo H5ReaderNew::GetXMatrixInfo() {
	// TODO: Implement in Phase 4
	XMatrixInfo info;
	info.n_obs = GetObsCount();
	info.n_var = GetVarCount();
	return info;
}

std::vector<std::string> H5ReaderNew::GetVarNames(const std::string &column_name) {
	// TODO: Implement in Phase 3
	std::vector<std::string> names;
	return names;
}

void H5ReaderNew::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadXMatrix not yet implemented in C API version");
}

void H5ReaderNew::ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadXMatrixBatch not yet implemented in C API version");
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
	// TODO: Implement in Phase 4
}

void H5ReaderNew::InitializeZeros(Vector &vec, idx_t count) {
	// TODO: Implement in Phase 4
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
	// TODO: Implement in Phase 4
}

// ============================================================================
// Helper Methods Implementation
// ============================================================================

std::vector<std::string> H5ReaderNew::GetGroupMembers(const std::string &group_name) {
	std::vector<std::string> members;
	
	if (!H5LinkExists(file.get(), group_name.c_str())) {
		return members;
	}
	
	H5GroupHandle group(file.get(), group_name);
	
	// Get the number of objects in the group
	hsize_t num_objs;
	H5Gget_num_objs(group.get(), &num_objs);
	
	// Iterate through objects
	for (hsize_t i = 0; i < num_objs; i++) {
		// Get object name length
		ssize_t name_len = H5Gget_objname_by_idx(group.get(), i, nullptr, 0);
		if (name_len > 0) {
			// Allocate buffer and get name
			std::vector<char> name_buffer(name_len + 1);
			H5Gget_objname_by_idx(group.get(), i, name_buffer.data(), name_len + 1);
			members.push_back(std::string(name_buffer.data()));
		}
	}
	
	return members;
}

} // namespace duckdb