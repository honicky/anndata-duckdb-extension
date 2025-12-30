#include "include/h5_reader_multithreaded.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <mutex>

namespace duckdb {

// ============================================================================
// Phase 2: Core Infrastructure - Constructor/Destructor and Helper Methods
// ============================================================================

H5ReaderMultithreaded::H5ReaderMultithreaded(const std::string &file_path) : file_path(file_path) {
	// Initialize HDF5 library and set up thread safety
	// Use a function-level static to ensure thread-safe initialization (C++11 guarantees this)
	static std::once_flag hdf5_init_flag;
	std::call_once(hdf5_init_flag, []() {
		// Initialize HDF5 library explicitly
		if (H5open() < 0) {
			throw IOException("Failed to initialize HDF5 library");
		}

		// Check if HDF5 was built with thread-safety
		hbool_t is_threadsafe;
		herr_t err = H5is_library_threadsafe(&is_threadsafe);
		if (err >= 0 && !is_threadsafe) {
			// Warn if HDF5 is not thread-safe
			fprintf(stderr, "WARNING: HDF5 library is not thread-safe. UNION queries may fail.\n");
		}

		// Turn off HDF5 error printing to avoid stderr spam
		H5Eset_auto2(H5E_DEFAULT, nullptr, nullptr);

		// Disable atexit handling to avoid shutdown races
		H5dont_atexit();
	});

	// Get shared file handle from cache
	try {
		file_handle = H5FileCache::Open(file_path);
		if (!file_handle || *file_handle < 0) {
			throw IOException("Failed to get valid file handle from cache");
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to open HDF5 file " + file_path + ": " + std::string(e.what()));
	}
}

H5ReaderMultithreaded::~H5ReaderMultithreaded() {
	// File handle will be closed by shared_ptr when last reference is released
	file_handle.reset();
}

// Helper method to check if a group exists
bool H5ReaderMultithreaded::IsGroupPresent(const std::string &group_name) {
	if (!H5LinkExists(*file_handle, group_name)) {
		return false;
	}

	// Check if it's actually a group (not a dataset)
	return H5GetObjectType(*file_handle, group_name) == H5O_TYPE_GROUP;
}

// Helper method to check if a dataset exists within a group
bool H5ReaderMultithreaded::IsDatasetPresent(const std::string &group_name, const std::string &dataset_name) {
	// First check if the group exists
	if (!IsGroupPresent(group_name)) {
		return false;
	}

	try {
		H5GroupHandle group(*file_handle, group_name);

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
	auto *data = static_cast<IterateData *>(op_data);
	data->members->push_back(std::string(name));
	return 0; // Continue iteration
}

// Helper method to get all members of a group
std::vector<std::string> H5ReaderMultithreaded::GetGroupMembers(const std::string &group_name) {
	std::vector<std::string> members;

	try {
		H5GroupHandle group(*file_handle, group_name);

		IterateData data;
		data.members = &members;

		H5Literate(group.get(), H5_INDEX_NAME, H5_ITER_NATIVE, nullptr, group_iterate_callback, &data);
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	// Check for required groups: /obs, /var, and either /X group or dataset
	return IsGroupPresent("/obs") && IsGroupPresent("/var") &&
	       (IsGroupPresent("/X") || H5LinkExists(*file_handle, "/X"));
}

// Get number of observations (cells)
size_t H5ReaderMultithreaded::GetObsCount() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	try {
		// Try to get shape from obs/_index first (standard location)
		if (IsDatasetPresent("/obs", "_index")) {
			H5DatasetHandle dataset(*file_handle, "/obs/_index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}

		// Try index (alternative location)
		if (IsDatasetPresent("/obs", "index")) {
			H5DatasetHandle dataset(*file_handle, "/obs/index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}

		// Try getting from X matrix shape
		if (H5LinkExists(*file_handle, "/X")) {
			if (H5GetObjectType(*file_handle, "/X") == H5O_TYPE_DATASET) {
				// Dense matrix
				H5DatasetHandle dataset(*file_handle, "/X");
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[2];
				int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
				if (ndims == 2) {
					return dims[0];
				}
			} else if (H5GetObjectType(*file_handle, "/X") == H5O_TYPE_GROUP) {
				// Sparse matrix - check indptr for CSR or shape attribute
				if (IsDatasetPresent("/X", "indptr")) {
					H5DatasetHandle indptr(*file_handle, "/X/indptr");
					H5DataspaceHandle indptr_space(indptr.get());
					hsize_t indptr_dims[1];
					H5Sget_simple_extent_dims(indptr_space.get(), indptr_dims, nullptr);

					// For CSR format, n_obs = len(indptr) - 1
					// Check if there's a shape attribute to be sure
					if (H5Aexists(*file_handle, "shape")) {
						H5AttributeHandle shape_attr(*file_handle, "shape");
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	try {
		// Try to get shape from var/_index first (standard location)
		if (IsDatasetPresent("/var", "_index")) {
			H5DatasetHandle dataset(*file_handle, "/var/_index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}

		// Try index (alternative location)
		if (IsDatasetPresent("/var", "index")) {
			H5DatasetHandle dataset(*file_handle, "/var/index");
			H5DataspaceHandle dataspace(dataset.get());
			hsize_t dims[1];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			return dims[0];
		}

		// Try getting from X matrix shape
		if (H5LinkExists(*file_handle, "/X")) {
			if (H5GetObjectType(*file_handle, "/X") == H5O_TYPE_DATASET) {
				// Dense matrix
				H5DatasetHandle dataset(*file_handle, "/X");
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[2];
				int ndims = H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
				if (ndims == 2) {
					return dims[1];
				}
			} else if (H5GetObjectType(*file_handle, "/X") == H5O_TYPE_GROUP) {
				// Sparse matrix - check shape attribute or indptr for CSC
				if (H5Aexists(*file_handle, "shape")) {
					H5AttributeHandle shape_attr(*file_handle, "shape");
					hsize_t shape[2];
					H5Aread(shape_attr.get(), H5T_NATIVE_HSIZE, shape);
					return shape[1];
				}

				// Check indptr for CSC format
				if (IsDatasetPresent("/X", "indptr")) {
					H5DatasetHandle indptr(*file_handle, "/X/indptr");
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
			if (H5GetObjectType(*file_handle, member_path) == H5O_TYPE_GROUP) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;

				// Load categories
				try {
					std::string cat_path = member_path + "/categories";
					if (H5LinkExists(*file_handle, cat_path.c_str())) {
						H5DatasetHandle cat_dataset(*file_handle, cat_path);
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
								std::string cat_value(buffer.data() + i * str_len,
								                      strnlen(buffer.data() + i * str_len, str_len));
								col.categories.push_back(cat_value);
							}
						}
					}
				} catch (...) {
					// Continue without categories
				}
			} else if (IsDatasetPresent("/obs", member)) {
				// Direct dataset
				H5DatasetHandle dataset(*file_handle, "/obs/" + member);
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
			if (H5GetObjectType(*file_handle, member_path) == H5O_TYPE_GROUP) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;

				// Load categories similar to obs
				try {
					std::string cat_path = member_path + "/categories";
					if (H5LinkExists(*file_handle, cat_path.c_str())) {
						H5DatasetHandle cat_dataset(*file_handle, cat_path);
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
								std::string cat_value(buffer.data() + i * str_len,
								                      strnlen(buffer.data() + i * str_len, str_len));
								col.categories.push_back(cat_value);
							}
						}
					}
				} catch (...) {
					// Continue without categories
				}
			} else if (IsDatasetPresent("/var", member)) {
				// Direct dataset
				H5DatasetHandle dataset(*file_handle, "/var/" + member);
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
		if (H5GetObjectType(*file_handle, group_path) == H5O_TYPE_GROUP) {
			// Handle categorical columns
			try {
				// Read codes
				H5DatasetHandle codes_dataset(*file_handle, group_path + "/codes");
				H5DataspaceHandle codes_space(codes_dataset.get());

				// Read categories into memory (usually small)
				H5DatasetHandle cat_dataset(*file_handle, group_path + "/categories");
				H5DataspaceHandle cat_space(cat_dataset.get());
				hsize_t cat_dims[1];
				H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);

				// Read all categories first (as strings for output)
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);

				H5TypeHandle cat_dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				H5T_class_t cat_class = H5Tget_class(cat_dtype.get());
				if (cat_class == H5T_STRING) {
					if (H5Tis_variable_str(cat_dtype.get())) {
						// Variable-length strings
						std::vector<char *> str_buffer(cat_dims[0]);
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
				} else if (cat_class == H5T_INTEGER) {
					// Integer categories
					size_t int_size = H5Tget_size(cat_dtype.get());
					if (int_size <= 4) {
						std::vector<int32_t> int_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, int_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(int_cats[i]));
						}
					} else {
						std::vector<int64_t> int_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, int_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(int_cats[i]));
						}
					}
				} else if (cat_class == H5T_FLOAT) {
					// Float categories
					size_t float_size = H5Tget_size(cat_dtype.get());
					if (float_size <= 4) {
						std::vector<float> float_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, float_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(float_cats[i]));
						}
					} else {
						std::vector<double> double_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
						        double_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(double_cats[i]));
						}
					}
				}

				// Now read the codes for the requested range - detect code dtype
				hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
				hsize_t h_count[1] = {static_cast<hsize_t>(count)};

				H5Sselect_hyperslab(codes_space.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
				H5DataspaceHandle mem_space(1, h_count);

				H5TypeHandle codes_dtype(codes_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				size_t code_size = H5Tget_size(codes_dtype.get());

				// Read codes with appropriate size
				std::vector<int32_t> codes_i32(count);
				if (code_size == 1) {
					std::vector<int8_t> codes_i8(count);
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT8, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i8.data());
					for (idx_t i = 0; i < count; i++) {
						codes_i32[i] = codes_i8[i];
					}
				} else if (code_size == 2) {
					std::vector<int16_t> codes_i16(count);
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT16, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i16.data());
					for (idx_t i = 0; i < count; i++) {
						codes_i32[i] = codes_i16[i];
					}
				} else {
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT32, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i32.data());
				}

				// Map codes to categories and set in result vector
				for (idx_t i = 0; i < count; i++) {
					int32_t code = codes_i32[i];
					if (code >= 0 && static_cast<size_t>(code) < categories.size()) {
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
			H5DatasetHandle dataset(*file_handle, "/obs/" + column_name);
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
					std::vector<char *> str_buffer(count);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        str_buffer.data());

					// Ensure vector is properly initialized
					result.SetVectorType(VectorType::FLAT_VECTOR);
					auto string_vec = FlatVector::GetData<string_t>(result);
					auto &validity = FlatVector::Validity(result);
					validity.SetAllValid(count); // Start with all valid

					for (idx_t i = 0; i < count; i++) {
						if (str_buffer[i] != nullptr) {
							string_vec[i] = StringVector::AddString(result, str_buffer[i]);
						} else {
							// Mark as NULL in the validity mask
							validity.SetInvalid(i);
						}
					}

					// Reclaim HDF5 memory after we've copied the strings
					H5Dvlen_reclaim(dtype.get(), mem_space.get(), H5P_DEFAULT, str_buffer.data());

#ifdef DEBUG
					// Verify the vector is valid
					Vector::Verify(result, count);
#endif
				} else {
					// Fixed-length strings
					size_t str_size = H5Tget_size(dtype.get());
					std::vector<char> buffer(count * str_size);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

					// Ensure vector is properly initialized
					result.SetVectorType(VectorType::FLAT_VECTOR);
					auto string_vec = FlatVector::GetData<string_t>(result);
					auto &validity = FlatVector::Validity(result);
					validity.SetAllValid(count);

					for (idx_t i = 0; i < count; i++) {
						char *str_ptr = buffer.data() + i * str_size;
						size_t len = strnlen(str_ptr, str_size);
						string_vec[i] = StringVector::AddString(result, str_ptr, len);
					}

#ifdef DEBUG
					// Verify the vector is valid
					Vector::Verify(result, count);
#endif
				}
			} else if (type_class == H5T_INTEGER) {
				// Read integer data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 1) {
					std::vector<int8_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::TINYINT(buffer[i]));
					}
				} else if (size <= 2) {
					std::vector<int16_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT16, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::SMALLINT(buffer[i]));
					}
				} else if (size <= 4) {
					std::vector<int32_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT32, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(buffer[i]));
					}
				} else {
					std::vector<int64_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT64, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(buffer[i]));
					}
				}
			} else if (type_class == H5T_FLOAT) {
				// Read floating-point data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 4) {
					std::vector<float> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_FLOAT, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(buffer[i]));
					}
				} else {
					std::vector<double> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(buffer[i]));
					}
				}
			} else if (type_class == H5T_ENUM) {
				// HDF5 ENUM is often used for boolean types in AnnData
				// Read as int8 and convert to string "true"/"false"
				std::vector<int8_t> buffer(count);
				H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

				// Ensure vector is properly initialized for strings
				result.SetVectorType(VectorType::FLAT_VECTOR);
				auto string_vec = FlatVector::GetData<string_t>(result);
				auto &validity = FlatVector::Validity(result);
				validity.SetAllValid(count);

				for (idx_t i = 0; i < count; i++) {
					if (buffer[i] == 0) {
						string_vec[i] = StringVector::AddString(result, "False");
					} else {
						string_vec[i] = StringVector::AddString(result, "True");
					}
				}
			} else {
				// Unknown type - set all values to NULL
				result.SetVectorType(VectorType::FLAT_VECTOR);
				auto &validity = FlatVector::Validity(result);
				for (idx_t i = 0; i < count; i++) {
					validity.SetInvalid(i);
				}
			}
		}
	} catch (const std::exception &e) {
		throw IOException("Failed to read obs column '" + column_name + "': " + e.what());
	}
}

void H5ReaderMultithreaded::ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
		if (H5GetObjectType(*file_handle, group_path) == H5O_TYPE_GROUP) {
			// Handle categorical columns - same logic as obs
			try {
				// Read codes
				H5DatasetHandle codes_dataset(*file_handle, group_path + "/codes");
				H5DataspaceHandle codes_space(codes_dataset.get());

				// Read categories into memory
				H5DatasetHandle cat_dataset(*file_handle, group_path + "/categories");
				H5DataspaceHandle cat_space(cat_dataset.get());
				hsize_t cat_dims[1];
				H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);

				// Read all categories first (as strings for output)
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);

				H5TypeHandle cat_dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				H5T_class_t cat_class = H5Tget_class(cat_dtype.get());
				if (cat_class == H5T_STRING) {
					if (H5Tis_variable_str(cat_dtype.get())) {
						// Variable-length strings
						std::vector<char *> str_buffer(cat_dims[0]);
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
				} else if (cat_class == H5T_INTEGER) {
					// Integer categories (e.g., feature_length)
					size_t int_size = H5Tget_size(cat_dtype.get());
					if (int_size <= 4) {
						std::vector<int32_t> int_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, int_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(int_cats[i]));
						}
					} else {
						std::vector<int64_t> int_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, int_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(int_cats[i]));
						}
					}
				} else if (cat_class == H5T_FLOAT) {
					// Float categories
					size_t float_size = H5Tget_size(cat_dtype.get());
					if (float_size <= 4) {
						std::vector<float> float_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, float_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(float_cats[i]));
						}
					} else {
						std::vector<double> double_cats(cat_dims[0]);
						H5Dread(cat_dataset.get(), H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT,
						        double_cats.data());
						for (size_t i = 0; i < cat_dims[0]; i++) {
							categories.emplace_back(std::to_string(double_cats[i]));
						}
					}
				}

				// Now read the codes for the requested range - detect code dtype
				hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
				hsize_t h_count[1] = {static_cast<hsize_t>(count)};

				H5Sselect_hyperslab(codes_space.get(), H5S_SELECT_SET, h_offset, nullptr, h_count, nullptr);
				H5DataspaceHandle mem_space(1, h_count);

				H5TypeHandle codes_dtype(codes_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				size_t code_size = H5Tget_size(codes_dtype.get());

				// Read codes with appropriate size
				std::vector<int32_t> codes_i32(count);
				if (code_size == 1) {
					std::vector<int8_t> codes_i8(count);
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT8, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i8.data());
					for (idx_t i = 0; i < count; i++) {
						codes_i32[i] = codes_i8[i];
					}
				} else if (code_size == 2) {
					std::vector<int16_t> codes_i16(count);
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT16, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i16.data());
					for (idx_t i = 0; i < count; i++) {
						codes_i32[i] = codes_i16[i];
					}
				} else {
					H5Dread(codes_dataset.get(), H5T_NATIVE_INT32, mem_space.get(), codes_space.get(), H5P_DEFAULT,
					        codes_i32.data());
				}

				// Map codes to categories
				for (idx_t i = 0; i < count; i++) {
					int32_t code = codes_i32[i];
					if (code >= 0 && static_cast<size_t>(code) < categories.size()) {
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
			H5DatasetHandle dataset(*file_handle, "/var/" + column_name);
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
					std::vector<char *> str_buffer(count);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        str_buffer.data());

					// Ensure vector is properly initialized
					result.SetVectorType(VectorType::FLAT_VECTOR);
					auto string_vec = FlatVector::GetData<string_t>(result);
					auto &validity = FlatVector::Validity(result);
					validity.SetAllValid(count); // Start with all valid

					for (idx_t i = 0; i < count; i++) {
						if (str_buffer[i] != nullptr) {
							string_vec[i] = StringVector::AddString(result, str_buffer[i]);
						} else {
							// Mark as NULL in the validity mask
							validity.SetInvalid(i);
						}
					}

					// Reclaim HDF5 memory after we've copied the strings
					H5Dvlen_reclaim(dtype.get(), mem_space.get(), H5P_DEFAULT, str_buffer.data());

#ifdef DEBUG
					// Verify the vector is valid
					Vector::Verify(result, count);
#endif
				} else {
					// Fixed-length strings
					size_t str_size = H5Tget_size(dtype.get());
					std::vector<char> buffer(count * str_size);
					H5Dread(dataset.get(), dtype.get(), mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

					// Ensure vector is properly initialized
					result.SetVectorType(VectorType::FLAT_VECTOR);
					auto string_vec = FlatVector::GetData<string_t>(result);
					auto &validity = FlatVector::Validity(result);
					validity.SetAllValid(count);

					for (idx_t i = 0; i < count; i++) {
						char *str_ptr = buffer.data() + i * str_size;
						size_t len = strnlen(str_ptr, str_size);
						string_vec[i] = StringVector::AddString(result, str_ptr, len);
					}

#ifdef DEBUG
					// Verify the vector is valid
					Vector::Verify(result, count);
#endif
				}
			} else if (type_class == H5T_INTEGER) {
				// Read integer data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 1) {
					std::vector<int8_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::TINYINT(buffer[i]));
					}
				} else if (size <= 2) {
					std::vector<int16_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT16, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::SMALLINT(buffer[i]));
					}
				} else if (size <= 4) {
					std::vector<int32_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT32, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(buffer[i]));
					}
				} else {
					std::vector<int64_t> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_INT64, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(buffer[i]));
					}
				}
			} else if (type_class == H5T_FLOAT) {
				// Read floating-point data
				size_t size = H5Tget_size(dtype.get());
				if (size <= 4) {
					std::vector<float> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_FLOAT, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(buffer[i]));
					}
				} else {
					std::vector<double> buffer(count);
					H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, mem_space.get(), dataspace.get(), H5P_DEFAULT,
					        buffer.data());
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(buffer[i]));
					}
				}
			} else if (type_class == H5T_ENUM) {
				// HDF5 ENUM is often used for boolean types in AnnData
				// Read as int8 and convert to string "true"/"false"
				std::vector<int8_t> buffer(count);
				H5Dread(dataset.get(), H5T_NATIVE_INT8, mem_space.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

				// Ensure vector is properly initialized for strings
				result.SetVectorType(VectorType::FLAT_VECTOR);
				auto string_vec = FlatVector::GetData<string_t>(result);
				auto &validity = FlatVector::Validity(result);
				validity.SetAllValid(count);

				for (idx_t i = 0; i < count; i++) {
					if (buffer[i] == 0) {
						string_vec[i] = StringVector::AddString(result, "False");
					} else {
						string_vec[i] = StringVector::AddString(result, "True");
					}
				}
			} else {
				// Unknown type - set all values to NULL
				result.SetVectorType(VectorType::FLAT_VECTOR);
				auto &validity = FlatVector::Validity(result);
				for (idx_t i = 0; i < count; i++) {
					validity.SetInvalid(i);
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

std::string H5ReaderMultithreaded::GetCategoricalValue(const std::string &group_path, const std::string &column_name,
                                                       idx_t index) {
	// TODO: Implement in Phase 6
	return "";
}

H5ReaderMultithreaded::XMatrixInfo H5ReaderMultithreaded::GetXMatrixInfo() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	XMatrixInfo info;
	info.n_obs = GetObsCount();
	info.n_var = GetVarCount();

	// Check if X is sparse or dense
	if (H5LinkExists(*file_handle, "/X")) {
		if (H5GetObjectType(*file_handle, "/X") == H5O_TYPE_GROUP) {
			// Sparse matrix
			info.is_sparse = true;
			// Try to determine format (CSR vs CSC)
			if (IsDatasetPresent("/X", "indptr") && IsDatasetPresent("/X", "indices")) {
				// Check indptr length to determine format
				H5DatasetHandle indptr(*file_handle, "/X/indptr");
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::vector<std::string> names;
	try {
		auto var_count = GetVarCount();
		names.reserve(var_count);

		if (column_name.empty() || column_name == "var_names") {
			// Try to get default var names from _index attribute
			if (H5AttributeExists(*file_handle, "/var", "_index")) {
				// Open the /var group
				H5GroupHandle var_group(*file_handle, "/var");
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
							std::vector<char *> str_data(var_count);
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
				H5DatasetHandle dataset(*file_handle, dataset_path);
				H5DataspaceHandle dataspace(dataset.get());
				hsize_t dims[1];
				H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);

				if (dims[0] == var_count) {
					H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
					if (H5Tget_class(dtype.get()) == H5T_STRING) {
						if (H5Tis_variable_str(dtype.get())) {
							// Variable-length strings
							std::vector<char *> str_data(var_count);
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
			} else {
				// Check if it's a categorical column (Group with codes/categories)
				std::string group_path = "/var/" + column_name;
				std::string codes_path = group_path + "/codes";
				std::string categories_path = group_path + "/categories";

				// Check if codes dataset exists
				htri_t codes_exists = H5Lexists(*file_handle, codes_path.c_str(), H5P_DEFAULT);
				htri_t categories_exists = H5Lexists(*file_handle, categories_path.c_str(), H5P_DEFAULT);

				if (codes_exists > 0 && categories_exists > 0) {
					// Read categories first
					H5DatasetHandle cat_dataset(*file_handle, categories_path);
					H5DataspaceHandle cat_space(cat_dataset.get());
					hsize_t cat_dims[1];
					H5Sget_simple_extent_dims(cat_space.get(), cat_dims, nullptr);

					std::vector<std::string> categories;
					categories.reserve(cat_dims[0]);

					H5TypeHandle cat_dtype(cat_dataset.get(), H5TypeHandle::TypeClass::DATASET);
					if (H5Tget_class(cat_dtype.get()) == H5T_STRING) {
						if (H5Tis_variable_str(cat_dtype.get())) {
							std::vector<char *> str_buffer(cat_dims[0]);
							H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT,
							        str_buffer.data());
							for (hsize_t i = 0; i < cat_dims[0]; i++) {
								if (str_buffer[i]) {
									categories.emplace_back(str_buffer[i]);
								} else {
									categories.emplace_back("");
								}
							}
							H5Dvlen_reclaim(cat_dtype.get(), cat_space.get(), H5P_DEFAULT, str_buffer.data());
						} else {
							size_t str_size = H5Tget_size(cat_dtype.get());
							std::vector<char> buffer(cat_dims[0] * str_size);
							H5Dread(cat_dataset.get(), cat_dtype.get(), H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
							for (hsize_t i = 0; i < cat_dims[0]; i++) {
								char *str_ptr = buffer.data() + i * str_size;
								size_t len = strnlen(str_ptr, str_size);
								categories.emplace_back(str_ptr, len);
							}
						}
					}

					// Read codes and map to categories
					H5DatasetHandle codes_dataset(*file_handle, codes_path);
					H5DataspaceHandle codes_space(codes_dataset.get());
					hsize_t codes_dims[1];
					H5Sget_simple_extent_dims(codes_space.get(), codes_dims, nullptr);

					if (codes_dims[0] == var_count && !categories.empty()) {
						// Determine code type and read
						H5TypeHandle codes_dtype(codes_dataset.get(), H5TypeHandle::TypeClass::DATASET);
						size_t code_size = H5Tget_size(codes_dtype.get());

						if (code_size == 1) {
							std::vector<int8_t> codes(var_count);
							H5Dread(codes_dataset.get(), H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, codes.data());
							for (idx_t i = 0; i < var_count; i++) {
								int8_t code = codes[i];
								if (code >= 0 && static_cast<size_t>(code) < categories.size()) {
									names.push_back(categories[code]);
								} else {
									names.push_back("var_" + std::to_string(i));
								}
							}
						} else if (code_size == 2) {
							std::vector<int16_t> codes(var_count);
							H5Dread(codes_dataset.get(), H5T_NATIVE_INT16, H5S_ALL, H5S_ALL, H5P_DEFAULT, codes.data());
							for (idx_t i = 0; i < var_count; i++) {
								int16_t code = codes[i];
								if (code >= 0 && static_cast<size_t>(code) < categories.size()) {
									names.push_back(categories[code]);
								} else {
									names.push_back("var_" + std::to_string(i));
								}
							}
						} else {
							std::vector<int32_t> codes(var_count);
							H5Dread(codes_dataset.get(), H5T_NATIVE_INT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, codes.data());
							for (idx_t i = 0; i < var_count; i++) {
								int32_t code = codes[i];
								if (code >= 0 && static_cast<size_t>(code) < categories.size()) {
									names.push_back(categories[code]);
								} else {
									names.push_back("var_" + std::to_string(i));
								}
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

H5ReaderMultithreaded::VarColumnDetection H5ReaderMultithreaded::DetectVarColumns() {
	VarColumnDetection result;

	// Get list of var column names
	auto columns = GetVarColumns();
	std::vector<std::string> column_names;
	for (const auto &col : columns) {
		column_names.push_back(col.name);
	}

	// Phase 1: Check known column names (case-insensitive)
	// Priority order from spec (user modified)
	std::vector<std::string> name_preferred = {"gene_symbols", "gene_symbol",  "gene_names", "gene_name", "symbol",
	                                           "symbols",      "feature_name", "name",       "names"};
	std::vector<std::string> id_preferred = {"gene_ids", "gene_id", "ensembl_id", "ensembl", "feature_id", "id", "ids"};

	// Find by known names first
	for (const auto &preferred : name_preferred) {
		for (const auto &col : column_names) {
			if (StringUtil::CIEquals(col, preferred)) {
				result.name_column = col;
				break;
			}
		}
		if (!result.name_column.empty()) {
			break;
		}
	}

	for (const auto &preferred : id_preferred) {
		for (const auto &col : column_names) {
			if (StringUtil::CIEquals(col, preferred)) {
				result.id_column = col;
				break;
			}
		}
		if (!result.id_column.empty()) {
			break;
		}
	}

	// Phase 2: If not found, score columns by content sampling
	if (result.name_column.empty() || result.id_column.empty()) {
		// Sample up to 100 values from each string column and score them
		int best_name_score = 0;
		int best_id_score = 0;
		std::string best_name_col;
		std::string best_id_col;

		for (const auto &col : columns) {
			// Only check string columns
			if (col.type != LogicalType::VARCHAR) {
				continue;
			}

			// Sample values from this column
			int gene_symbol_count = 0;
			int ensembl_count = 0;
			int numeric_count = 0;

			try {
				auto sample_size = std::min(static_cast<size_t>(100), GetVarCount());
				for (size_t i = 0; i < sample_size; i++) {
					auto value = ReadVarColumnString(col.original_name, i);
					if (value.empty()) {
						continue;
					}

					// Check if looks like Ensembl ID (starts with ENS)
					if (value.size() >= 4 && value.substr(0, 3) == "ENS") {
						ensembl_count++;
					}
					// Check if looks like gene symbol (uppercase, 2-12 chars, alphanumeric with hyphens)
					else if (value.size() >= 2 && value.size() <= 12) {
						bool is_gene_symbol = true;
						bool has_letter = false;
						for (char c : value) {
							if (std::isalpha(c)) {
								has_letter = true;
								if (!std::isupper(c)) {
									is_gene_symbol = false;
									break;
								}
							} else if (!std::isdigit(c) && c != '-') {
								is_gene_symbol = false;
								break;
							}
						}
						if (is_gene_symbol && has_letter) {
							gene_symbol_count++;
						}
					}
					// Check if purely numeric
					bool is_numeric = true;
					for (char c : value) {
						if (!std::isdigit(c)) {
							is_numeric = false;
							break;
						}
					}
					if (is_numeric) {
						numeric_count++;
					}
				}

				// Score: gene symbols get 2 points, ensembl gets 1 point
				// Avoid columns that are mostly numeric
				if (numeric_count < static_cast<int>(sample_size) / 2) {
					int name_score = gene_symbol_count * 2;
					int id_score = ensembl_count;

					if (result.name_column.empty() && name_score > best_name_score) {
						best_name_score = name_score;
						best_name_col = col.name;
					}
					if (result.id_column.empty() && id_score > best_id_score) {
						best_id_score = id_score;
						best_id_col = col.name;
					}
				}
			} catch (...) {
				// Skip columns that can't be read
				continue;
			}
		}

		if (result.name_column.empty() && !best_name_col.empty()) {
			result.name_column = best_name_col;
		}
		if (result.id_column.empty() && !best_id_col.empty()) {
			result.id_column = best_id_col;
		}
	}

	// Phase 3: Fallback to _index
	if (result.name_column.empty()) {
		result.name_column = "_index";
	}
	if (result.id_column.empty()) {
		result.id_column = "_index";
	}

	return result;
}

void H5ReaderMultithreaded::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count,
                                        std::vector<double> &values) {
	// TODO: Implement in Phase 4
	throw NotImplementedException("ReadXMatrix not yet implemented in C API version");
}

void H5ReaderMultithreaded::ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count,
                                             DataChunk &output) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
				if (row >= row_start && row < row_start + row_count && col >= col_start &&
				    col < col_start + col_count) {
					idx_t local_row = row - row_start;
					idx_t local_col = col - col_start;
					auto &gene_vec = output.data[local_col + 1];
					gene_vec.SetValue(local_row, Value::DOUBLE(val));
				}
			}
			return;
		}

		// Read dense matrix
		if (!H5LinkExists(*file_handle, "/X")) {
			throw IOException("X matrix not found in file");
		}

		H5DatasetHandle dataset(*file_handle, "/X");
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

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrix(idx_t obs_start, idx_t obs_count,
                                                                                 idx_t var_start, idx_t var_count) {
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

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrixCSR(idx_t obs_start, idx_t obs_count,
                                                                                    idx_t var_start, idx_t var_count) {
	return ReadSparseMatrixCSR("/X", obs_start, obs_count, var_start, var_count);
}

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseXMatrixCSC(idx_t obs_start, idx_t obs_count,
                                                                                    idx_t var_start, idx_t var_count) {
	return ReadSparseMatrixCSC("/X", obs_start, obs_count, var_start, var_count);
}

std::vector<H5ReaderMultithreaded::MatrixInfo> H5ReaderMultithreaded::GetObsmMatrices() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::vector<MatrixInfo> matrices;

	try {
		if (IsGroupPresent("/obsm")) {
			// Get list of members in the obsm group
			auto members = GetGroupMembers("/obsm");

			for (const auto &matrix_name : members) {
				// Check if it's a dataset
				std::string path = "/obsm/" + matrix_name;
				if (H5LinkExists(*file_handle, path) && H5GetObjectType(*file_handle, path) == H5O_TYPE_DATASET) {
					// Open dataset and get dimensions
					H5DatasetHandle dataset(*file_handle, path);
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::vector<MatrixInfo> matrices;

	try {
		if (IsGroupPresent("/varm")) {
			// Get list of members in the varm group
			auto members = GetGroupMembers("/varm");

			for (const auto &matrix_name : members) {
				// Check if it's a dataset
				std::string path = "/varm/" + matrix_name;
				if (H5LinkExists(*file_handle, path) && H5GetObjectType(*file_handle, path) == H5O_TYPE_DATASET) {
					// Open dataset and get dimensions
					H5DatasetHandle dataset(*file_handle, path);
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

void H5ReaderMultithreaded::ReadObsmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count,
                                           idx_t col_idx, Vector &result) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	try {
		std::string dataset_path = "/obsm/" + matrix_name;
		H5DatasetHandle dataset(*file_handle, dataset_path);
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

void H5ReaderMultithreaded::ReadVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count,
                                           idx_t col_idx, Vector &result) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	try {
		std::string dataset_path = "/varm/" + matrix_name;
		H5DatasetHandle dataset(*file_handle, dataset_path);
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::vector<LayerInfo> layers;

	if (!IsGroupPresent("/layers")) {
		return layers; // No layers in this file
	}

	H5GroupHandle layers_group(*file_handle, "/layers");

	// Get number of objects in the layers group
	hsize_t num_objs;
	H5_CHECK(H5Gget_num_objs(layers_group.get(), &num_objs));

	for (hsize_t i = 0; i < num_objs; i++) {
		// Get layer name
		ssize_t name_size = H5Gget_objname_by_idx(layers_group.get(), i, nullptr, 0);
		if (name_size < 0)
			continue;

		std::vector<char> name_buffer(name_size + 1);
		H5Gget_objname_by_idx(layers_group.get(), i, name_buffer.data(), name_size + 1);
		std::string layer_name(name_buffer.data());

		LayerInfo info;
		info.name = layer_name;
		std::string layer_path = "/layers/" + layer_name;

		// Check object type
		H5O_info_t obj_info;
		H5_CHECK(H5Oget_info_by_name(*file_handle, layer_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT));

		if (obj_info.type == H5O_TYPE_DATASET) {
			// Dense layer
			info.is_sparse = false;
			H5DatasetHandle dataset(*file_handle, layer_path);
			H5DataspaceHandle dataspace(dataset.get());

			// Get dimensions
			hsize_t dims[2];
			H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);
			info.rows = dims[0];
			info.cols = dims[1];

			// Get data type
			H5TypeHandle dtype(dataset.get(), H5TypeHandle::TypeClass::DATASET);
			info.dtype = H5TypeToDuckDBType(dtype.get());

		} else if (obj_info.type == H5O_TYPE_GROUP) {
			// Sparse layer
			info.is_sparse = true;

			// Check for indptr to determine format and dimensions
			std::string indptr_path = layer_path + "/indptr";
			if (IsDatasetPresent(layer_path, "indptr")) {
				H5DatasetHandle indptr(*file_handle, indptr_path);
				H5DataspaceHandle indptr_space(indptr.get());

				hsize_t indptr_dims;
				H5Sget_simple_extent_dims(indptr_space.get(), &indptr_dims, nullptr);

				// Check encoding attribute to determine CSR vs CSC
				bool format_determined = false;
				if (H5Aexists(indptr.get(), "encoding-type") > 0) {
					H5AttributeHandle encoding_attr(indptr.get(), "encoding-type");
					hid_t atype = H5Aget_type(encoding_attr.get());
					size_t size = H5Tget_size(atype);
					std::vector<char> buffer(size + 1, 0);
					H5Aread(encoding_attr.get(), atype, buffer.data());
					std::string encoding(buffer.data());
					H5Tclose(atype);

					if (encoding == "csr_matrix") {
						info.sparse_format = "CSR";
						info.rows = indptr_dims - 1;
						format_determined = true;
						// Get cols from shape attribute
						if (H5Aexists(indptr.get(), "shape") > 0) {
							H5AttributeHandle shape_attr(indptr.get(), "shape");
							int64_t shape[2];
							H5Aread(shape_attr.get(), H5T_NATIVE_INT64, shape);
							info.cols = shape[1];
						}
					} else if (encoding == "csc_matrix") {
						info.sparse_format = "CSC";
						info.cols = indptr_dims - 1;
						format_determined = true;
						// Get rows from shape attribute
						if (H5Aexists(indptr.get(), "shape") > 0) {
							H5AttributeHandle shape_attr(indptr.get(), "shape");
							int64_t shape[2];
							H5Aread(shape_attr.get(), H5T_NATIVE_INT64, shape);
							info.rows = shape[0];
						}
					}
				}

				// If no encoding-type attribute, try to infer from context
				// Layers typically match X matrix dimensions
				if (!format_determined) {
					// Get X matrix dimensions for reference
					auto x_info = GetXMatrixInfo();

					// If indptr size matches rows+1, it's likely CSR
					// If indptr size matches cols+1, it's likely CSC
					if (indptr_dims - 1 == x_info.n_obs) {
						info.sparse_format = "CSR";
						info.rows = indptr_dims - 1;
						info.cols = x_info.n_var;
					} else if (indptr_dims - 1 == x_info.n_var) {
						info.sparse_format = "CSC";
						info.cols = indptr_dims - 1;
						info.rows = x_info.n_obs;
					}
				}
			}

			// Get data type from data array
			std::string data_path = layer_path + "/data";
			if (IsDatasetPresent(layer_path, "data")) {
				H5DatasetHandle data_dataset(*file_handle, data_path);
				H5TypeHandle dtype(data_dataset.get(), H5TypeHandle::TypeClass::DATASET);
				info.dtype = H5TypeToDuckDBType(dtype.get());
			}
		}

		layers.push_back(info);
	}

	return layers;
}

void H5ReaderMultithreaded::ReadLayerMatrix(const std::string &layer_name, idx_t row_idx, idx_t start_col, idx_t count,
                                            DataChunk &output, const std::vector<std::string> &var_names) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::string layer_path = "/layers/" + layer_name;

	// Check object type
	H5O_info_t obj_info;
	H5_CHECK(H5Oget_info_by_name(*file_handle, layer_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT));

	if (obj_info.type == H5O_TYPE_DATASET) {
		// Dense layer - read directly
		H5DatasetHandle dataset(*file_handle, layer_path);
		H5DataspaceHandle dataspace(dataset.get());

		// Get dimensions
		hsize_t dims[2];
		H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);

		// Set up hyperslab selection
		hsize_t offset[2] = {row_idx, start_col};
		hsize_t count_dims[2] = {1, count};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, offset, nullptr, count_dims, nullptr);

		// Create memory dataspace
		H5DataspaceHandle memspace(count);

		// Allocate buffer and read data
		std::vector<double> buffer(count);
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

		// Set obs_idx column
		auto &obs_idx_vec = output.data[0];
		for (idx_t i = 0; i < count; i++) {
			obs_idx_vec.SetValue(0, Value::BIGINT(row_idx));
		}

		// Set gene columns
		for (idx_t i = 0; i < count && i < var_names.size(); i++) {
			auto &gene_vec = output.data[i + 1];
			gene_vec.SetValue(0, Value::DOUBLE(buffer[i]));
		}

		output.SetCardinality(1);

	} else if (obj_info.type == H5O_TYPE_GROUP) {
		// Sparse layer
		H5GroupHandle layer_group(*file_handle, layer_path);

		// Determine format
		std::string indptr_path = layer_path + "/indptr";
		H5DatasetHandle indptr(*file_handle, indptr_path);

		// Check encoding
		std::string format = "CSR"; // default
		if (H5Aexists(indptr.get(), "encoding-type") > 0) {
			H5AttributeHandle encoding_attr(indptr.get(), "encoding-type");
			hid_t atype = H5Aget_type(encoding_attr.get());
			size_t size = H5Tget_size(atype);
			std::vector<char> buffer(size + 1, 0);
			H5Aread(encoding_attr.get(), atype, buffer.data());
			std::string encoding(buffer.data());
			H5Tclose(atype);

			if (encoding == "csc_matrix") {
				format = "CSC";
			}
		}

		if (format == "CSR") {
			// Read CSR sparse matrix for a single row
			auto sparse_data = ReadSparseMatrixCSR(layer_path, row_idx, 1, start_col, count);

			// Initialize output with zeros
			auto &obs_idx_vec = output.data[0];
			obs_idx_vec.SetValue(0, Value::BIGINT(row_idx));

			for (idx_t i = 0; i < count && i < var_names.size(); i++) {
				auto &gene_vec = output.data[i + 1];
				gene_vec.SetValue(0, Value::DOUBLE(0.0));
			}

			// Fill in non-zero values
			for (size_t i = 0; i < sparse_data.col_indices.size(); i++) {
				idx_t col = sparse_data.col_indices[i];
				if (col >= start_col && col < start_col + count) {
					auto &gene_vec = output.data[col - start_col + 1];
					gene_vec.SetValue(0, Value::DOUBLE(sparse_data.values[i]));
				}
			}
		} else {
			// CSC format - need to read columns
			auto sparse_data = ReadSparseMatrixCSC(layer_path, row_idx, 1, start_col, count);

			// Initialize output with zeros
			auto &obs_idx_vec = output.data[0];
			obs_idx_vec.SetValue(0, Value::BIGINT(row_idx));

			for (idx_t i = 0; i < count && i < var_names.size(); i++) {
				auto &gene_vec = output.data[i + 1];
				gene_vec.SetValue(0, Value::DOUBLE(0.0));
			}

			// Fill in non-zero values
			for (size_t i = 0; i < sparse_data.col_indices.size(); i++) {
				idx_t col = sparse_data.col_indices[i];
				if (col >= start_col && col < start_col + count) {
					auto &gene_vec = output.data[col - start_col + 1];
					gene_vec.SetValue(0, Value::DOUBLE(sparse_data.values[i]));
				}
			}
		}

		output.SetCardinality(1);
	}
}

void H5ReaderMultithreaded::ReadLayerMatrixBatch(const std::string &layer_name, idx_t row_start, idx_t row_count,
                                                 idx_t col_start, idx_t col_count, DataChunk &output) {
	std::string layer_path = "/layers/" + layer_name;
	ReadMatrixBatch(layer_path, row_start, row_count, col_start, col_count, output, true);
}

void H5ReaderMultithreaded::ReadMatrixBatch(const std::string &path, idx_t row_start, idx_t row_count, idx_t col_start,
                                            idx_t col_count, DataChunk &output, bool is_layer) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	// First column is always obs_idx
	auto &obs_idx_vec = output.data[0];
	for (idx_t i = 0; i < row_count; i++) {
		obs_idx_vec.SetValue(i, Value::BIGINT(row_start + i));
	}

	// Initialize all data columns with zeros
	for (idx_t col = 1; col <= col_count && col < output.data.size(); col++) {
		for (idx_t i = 0; i < row_count; i++) {
			output.data[col].SetValue(i, Value::DOUBLE(0.0));
		}
	}

	// Check if matrix is sparse or dense
	bool is_sparse = false;
	bool is_dense = false;

	// Check object type
	H5O_info_t obj_info;
	H5_CHECK(H5Oget_info_by_name(*file_handle, path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT));

	if (is_layer) {
		// For layers, check the structure
		is_dense = (obj_info.type == H5O_TYPE_DATASET);
		is_sparse = (obj_info.type == H5O_TYPE_GROUP);
	} else {
		// For X matrix
		is_dense = IsDatasetPresent("/", "X");
		is_sparse = !is_dense && IsGroupPresent("/X");
	}

	if (is_dense) {
		// Read dense matrix
		H5DatasetHandle dataset(*file_handle, path);
		H5DataspaceHandle dataspace(dataset.get());

		// Get dimensions
		hsize_t dims[2];
		H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);

		// Set up hyperslab selection for the rows and columns we want
		hsize_t offset[2] = {row_start, col_start};
		hsize_t count_dims[2] = {row_count, col_count};
		H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, offset, nullptr, count_dims, nullptr);

		// Create memory dataspace
		hsize_t mem_dims[2] = {row_count, col_count};
		H5DataspaceHandle memspace(2, mem_dims);

		// Read the data
		std::vector<double> buffer(row_count * col_count);
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

		// Fill the output columns (column 0 is obs_idx, data starts at column 1)
		for (idx_t row = 0; row < row_count; row++) {
			for (idx_t col = 0; col < col_count && col + 1 < output.data.size(); col++) {
				double val = buffer[row * col_count + col];
				output.data[col + 1].SetValue(row, Value::DOUBLE(val));
			}
		}
	} else if (is_sparse) {
		// Determine if CSR or CSC format
		std::string format = "CSR"; // default
		std::string indptr_path = path + "/indptr";

		// Check encoding attribute on the GROUP first (AnnData stores it on the sparse group, not datasets)
		bool format_determined = false;
		H5GroupHandle group(*file_handle, path);
		htri_t attr_exists = H5Aexists(group.get(), "encoding-type");
		if (attr_exists > 0) {
			H5AttributeHandle encoding_attr(group.get(), "encoding-type");
			hid_t atype = H5Aget_type(encoding_attr.get());

			std::string encoding;
			// Check if it's a variable-length string
			if (H5Tis_variable_str(atype)) {
				// Variable-length string - read into a char* pointer
				char *vlen_str = nullptr;
				H5Aread(encoding_attr.get(), atype, &vlen_str);
				if (vlen_str) {
					encoding = vlen_str;
					// Free the memory allocated by HDF5
					hid_t space = H5Aget_space(encoding_attr.get());
					H5Dvlen_reclaim(atype, space, H5P_DEFAULT, &vlen_str);
					H5Sclose(space);
				}
			} else {
				// Fixed-length string
				size_t size = H5Tget_size(atype);
				std::vector<char> buffer(size + 1, 0);
				H5Aread(encoding_attr.get(), atype, buffer.data());
				encoding = std::string(buffer.data());
			}
			H5Tclose(atype);

			if (encoding == "csc_matrix") {
				format = "CSC";
				format_determined = true;
			} else if (encoding == "csr_matrix") {
				format = "CSR";
				format_determined = true;
			}
		}

		// If no encoding attribute on group, try to infer from dimensions
		if (!format_determined && IsDatasetPresent(path, "indptr")) {
			H5DatasetHandle indptr(*file_handle, indptr_path);
			H5DataspaceHandle indptr_space(indptr.get());

			// Get indptr dimensions to help infer format
			hsize_t indptr_dims;
			H5Sget_simple_extent_dims(indptr_space.get(), &indptr_dims, nullptr);

			if (is_layer) {
				// For layers, they should match X matrix dimensions
				auto x_info = GetXMatrixInfo();
				if (indptr_dims - 1 == x_info.n_obs) {
					format = "CSR";
				} else if (indptr_dims - 1 == x_info.n_var) {
					format = "CSC";
				}
			}
		}

		if (format == "CSR") {
			// Read CSR sparse matrix
			auto sparse_data = ReadSparseMatrixCSR(path, row_start, row_count, col_start, col_count);

			// Fill in the non-zero values
			for (size_t i = 0; i < sparse_data.row_indices.size(); i++) {
				idx_t row = sparse_data.row_indices[i] - row_start;
				idx_t col = sparse_data.col_indices[i] - col_start;

				if (row < row_count && col < col_count && col + 1 < output.data.size()) {
					output.data[col + 1].SetValue(row, Value::DOUBLE(sparse_data.values[i]));
				}
			}
		} else {
			// Read CSC sparse matrix
			auto sparse_data = ReadSparseMatrixCSC(path, row_start, row_count, col_start, col_count);

			// Fill in the non-zero values
			for (size_t i = 0; i < sparse_data.row_indices.size(); i++) {
				idx_t row = sparse_data.row_indices[i] - row_start;
				idx_t col = sparse_data.col_indices[i] - col_start;

				if (row < row_count && col < col_count && col + 1 < output.data.size()) {
					output.data[col + 1].SetValue(row, Value::DOUBLE(sparse_data.values[i]));
				}
			}
		}
	}

	output.SetCardinality(row_count);
}

void H5ReaderMultithreaded::ReadMatrixColumns(const std::string &path, idx_t row_start, idx_t row_count,
                                              const std::vector<idx_t> &matrix_col_indices, DataChunk &output,
                                              bool is_layer) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	// If no columns requested, just return empty result
	if (matrix_col_indices.empty()) {
		output.SetCardinality(row_count);
		return;
	}

	// Initialize all output columns with zeros
	for (idx_t col = 0; col < output.data.size(); col++) {
		InitializeZeros(output.data[col], row_count);
	}

	// Check if matrix is sparse or dense
	bool is_sparse = false;
	bool is_dense = false;

	// Check object type
	H5O_info_t obj_info;
	H5_CHECK(H5Oget_info_by_name(*file_handle, path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT));

	if (is_layer) {
		is_dense = (obj_info.type == H5O_TYPE_DATASET);
		is_sparse = (obj_info.type == H5O_TYPE_GROUP);
	} else {
		is_dense = IsDatasetPresent("/", "X");
		is_sparse = !is_dense && IsGroupPresent("/X");
	}

	if (is_dense) {
		// Read dense matrix - use hyperslab selection for each requested column
		H5DatasetHandle dataset(*file_handle, path);
		H5DataspaceHandle dataspace(dataset.get());

		// Get dimensions
		hsize_t dims[2];
		H5Sget_simple_extent_dims(dataspace.get(), dims, nullptr);

		// Read each requested column individually using hyperslab selection
		std::vector<double> col_buffer(row_count);

		for (idx_t out_col = 0; out_col < matrix_col_indices.size() && out_col < output.data.size(); out_col++) {
			idx_t matrix_col = matrix_col_indices[out_col];

			// Skip invalid column indices
			if (matrix_col >= dims[1]) {
				continue;
			}

			// Select single column hyperslab: rows [row_start, row_start+row_count), column [matrix_col]
			hsize_t offset[2] = {row_start, matrix_col};
			hsize_t count_dims[2] = {row_count, 1};

			// Reset dataspace selection
			H5Sselect_none(dataspace.get());
			H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, offset, nullptr, count_dims, nullptr);

			// Create memory dataspace for single column (use 2D to match file dataspace rank)
			hsize_t mem_dims[2] = {row_count, 1};
			H5DataspaceHandle memspace(2, mem_dims);

			// Read the column data
			H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, col_buffer.data());

			// Fill the output column
			auto &vec = output.data[out_col];
			for (idx_t row = 0; row < row_count; row++) {
				SetTypedValue(vec, row, col_buffer[row]);
			}
		}
	} else if (is_sparse) {
		// For sparse matrices, use the original batch reading approach and extract needed columns
		// This is simpler and more reliable than column-selective sparse reading
		// Find the range of columns we need
		idx_t min_col = matrix_col_indices.empty() ? 0 : matrix_col_indices[0];
		idx_t max_col = matrix_col_indices.empty() ? 0 : matrix_col_indices[0];
		for (idx_t col : matrix_col_indices) {
			min_col = MinValue(min_col, col);
			max_col = MaxValue(max_col, col);
		}
		idx_t col_count = max_col - min_col + 1;

		// Create a temporary chunk to read all needed columns (from min to max)
		DataChunk temp_chunk;
		vector<LogicalType> temp_types;
		temp_types.push_back(LogicalType::BIGINT); // obs_idx
		for (idx_t i = 0; i < col_count; i++) {
			temp_types.push_back(LogicalType::DOUBLE);
		}
		temp_chunk.Initialize(Allocator::DefaultAllocator(), temp_types);

		// Use the original batch reader
		ReadMatrixBatch(path, row_start, row_count, min_col, col_count, temp_chunk, is_layer);

		// Extract only the columns we need
		for (idx_t out_col = 0; out_col < matrix_col_indices.size() && out_col < output.data.size(); out_col++) {
			idx_t matrix_col = matrix_col_indices[out_col];
			idx_t temp_col = matrix_col - min_col + 1; // +1 to skip obs_idx in temp_chunk

			auto &src = temp_chunk.data[temp_col];
			auto &dst = output.data[out_col];

			for (idx_t row = 0; row < row_count; row++) {
				dst.SetValue(row, src.GetValue(row));
			}
		}
	}

	output.SetCardinality(row_count);
}

// Helper function to read array and return as vector of strings
static std::vector<std::string> ReadArrayAsStrings(hid_t file_handle, const std::string &path, hid_t dtype,
                                                   hsize_t total_size, H5T_class_t type_class) {
	std::vector<std::string> result;

	H5DatasetHandle dataset(file_handle, path);

	if (type_class == H5T_STRING) {
		// String array
		if (H5Tis_variable_str(dtype)) {
			std::vector<char *> str_buffer(total_size);
			H5Dread(dataset.get(), dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, str_buffer.data());
			for (hsize_t i = 0; i < total_size; i++) {
				if (str_buffer[i]) {
					result.emplace_back(str_buffer[i]);
				} else {
					result.emplace_back("");
				}
			}
			// Free memory
			hid_t space = H5Dget_space(dataset.get());
			H5Dvlen_reclaim(dtype, space, H5P_DEFAULT, str_buffer.data());
			H5Sclose(space);
		} else {
			size_t str_size = H5Tget_size(dtype);
			std::vector<char> buffer(total_size * str_size);
			H5Dread(dataset.get(), dtype, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
			for (hsize_t i = 0; i < total_size; i++) {
				char *str_ptr = buffer.data() + i * str_size;
				size_t len = strnlen(str_ptr, str_size);
				result.emplace_back(str_ptr, len);
			}
		}
	} else if (type_class == H5T_INTEGER) {
		std::vector<int64_t> buffer(total_size);
		H5Dread(dataset.get(), H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
		for (hsize_t i = 0; i < total_size; i++) {
			result.emplace_back(std::to_string(buffer[i]));
		}
	} else if (type_class == H5T_FLOAT) {
		std::vector<double> buffer(total_size);
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
		for (hsize_t i = 0; i < total_size; i++) {
			char buf[32];
			snprintf(buf, sizeof(buf), "%.6g", buffer[i]);
			result.emplace_back(buf);
		}
	} else if (type_class == H5T_ENUM) {
		// Boolean array
		std::vector<int8_t> buffer(total_size);
		H5Dread(dataset.get(), H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
		for (hsize_t i = 0; i < total_size; i++) {
			result.emplace_back((buffer[i] != 0) ? "true" : "false");
		}
	}

	return result;
}

// Helper function to recursively collect uns items
static void CollectUnsItems(hid_t file_handle, const std::string &base_path, const std::string &key_prefix,
                            std::vector<H5ReaderMultithreaded::UnsInfo> &uns_keys,
                            LogicalType (*H5TypeToDuckDBType)(hid_t)) {
	H5GroupHandle group(file_handle, base_path);

	hsize_t num_objs;
	H5_CHECK(H5Gget_num_objs(group.get(), &num_objs));

	for (hsize_t i = 0; i < num_objs; i++) {
		ssize_t name_size = H5Gget_objname_by_idx(group.get(), i, nullptr, 0);
		if (name_size < 0)
			continue;

		std::vector<char> name_buffer(name_size + 1);
		H5Gget_objname_by_idx(group.get(), i, name_buffer.data(), name_size + 1);
		std::string member_name(name_buffer.data());

		std::string full_key = key_prefix.empty() ? member_name : key_prefix + "/" + member_name;
		std::string obj_path = base_path + "/" + member_name;

		H5O_info_t obj_info;
		if (H5Oget_info_by_name(file_handle, obj_path.c_str(), &obj_info, H5O_INFO_BASIC, H5P_DEFAULT) < 0)
			continue;

		H5ReaderMultithreaded::UnsInfo info;
		info.key = full_key;

		if (obj_info.type == H5O_TYPE_DATASET) {
			H5DatasetHandle dataset(file_handle, obj_path);
			H5DataspaceHandle dataspace(dataset.get());
			H5TypeHandle datatype(dataset.get(), H5TypeHandle::TypeClass::DATASET);

			int rank = H5Sget_simple_extent_ndims(dataspace.get());
			H5T_class_t type_class = H5Tget_class(datatype.get());

			if (rank == 0) {
				// Scalar value
				info.type = "scalar";
				info.dtype = H5TypeToDuckDBType(datatype.get());
				info.shape.clear();

				// Read scalar value
				hid_t dtype_id = H5Dget_type(dataset.get());
				if (type_class == H5T_STRING) {
					if (H5Tis_variable_str(dtype_id)) {
						char *str_value = nullptr;
						H5Dread(dataset.get(), dtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, &str_value);
						if (str_value) {
							info.value_str = std::string(str_value);
							H5free_memory(str_value);
						}
					} else {
						size_t str_size = H5Tget_size(dtype_id);
						std::vector<char> buffer(str_size + 1, 0);
						H5Dread(dataset.get(), dtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
						info.value_str = std::string(buffer.data());
					}
				} else if (type_class == H5T_INTEGER) {
					int64_t value;
					H5Dread(dataset.get(), H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
					info.value_str = std::to_string(value);
				} else if (type_class == H5T_FLOAT) {
					double value;
					H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
					char buf[32];
					snprintf(buf, sizeof(buf), "%.6g", value);
					info.value_str = buf;
				} else if (type_class == H5T_ENUM) {
					int8_t enum_val;
					H5Dread(dataset.get(), H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, &enum_val);
					info.value_str = (enum_val != 0) ? "true" : "false";
				}
				H5Tclose(dtype_id);
			} else {
				// Array value
				info.type = "array";
				info.dtype = H5TypeToDuckDBType(datatype.get());

				std::vector<hsize_t> dims(rank);
				H5Sget_simple_extent_dims(dataspace.get(), dims.data(), nullptr);
				info.shape = dims;

				// Calculate total size and read array values
				hsize_t total_size = 1;
				for (int j = 0; j < rank; j++) {
					total_size *= dims[j];
				}

				hid_t dtype_id = H5Dget_type(dataset.get());
				info.array_values = ReadArrayAsStrings(file_handle, obj_path, dtype_id, total_size, type_class);
				H5Tclose(dtype_id);
			}
			uns_keys.push_back(info);
		} else if (obj_info.type == H5O_TYPE_GROUP) {
			// Recursively process subgroup
			CollectUnsItems(file_handle, obj_path, full_key, uns_keys, H5TypeToDuckDBType);
		}
	}
}

std::vector<H5ReaderMultithreaded::UnsInfo> H5ReaderMultithreaded::GetUnsKeys() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::vector<UnsInfo> uns_keys;

	// Check if uns group exists
	if (!IsGroupPresent("/uns")) {
		return uns_keys;
	}

	// Recursively collect all items with flattened paths
	CollectUnsItems(*file_handle, "/uns", "", uns_keys, H5ReaderMultithreaded::H5TypeToDuckDBType);

	return uns_keys;
}

Value H5ReaderMultithreaded::ReadUnsScalar(const std::string &key) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::string path = "/uns/" + key;

	// Check if the dataset exists
	if (!IsDatasetPresent("/uns", key)) {
		return Value(); // Return NULL
	}

	H5DatasetHandle dataset(*file_handle, path);
	H5DataspaceHandle dataspace(dataset.get());

	// Verify it's a scalar (0-dimensional)
	int rank = H5Sget_simple_extent_ndims(dataspace.get());
	if (rank != 0) {
		return Value(); // Not a scalar
	}

	// Get the data type
	hid_t dtype_id = H5Dget_type(dataset.get());
	H5T_class_t type_class = H5Tget_class(dtype_id);

	Value result_value;

	if (type_class == H5T_STRING) {
		// String scalar
		if (H5Tis_variable_str(dtype_id)) {
			char *str_value = nullptr;
			H5Dread(dataset.get(), dtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, &str_value);
			if (str_value) {
				result_value = Value(std::string(str_value));
				H5free_memory(str_value);
			}
		} else {
			// Fixed-length string
			size_t str_size = H5Tget_size(dtype_id);
			std::vector<char> buffer(str_size + 1, 0);
			H5Dread(dataset.get(), dtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, buffer.data());
			result_value = Value(std::string(buffer.data()));
		}
	} else if (type_class == H5T_INTEGER) {
		// Integer scalar
		int64_t value;
		H5Dread(dataset.get(), H5T_NATIVE_INT64, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
		result_value = Value::BIGINT(value);
	} else if (type_class == H5T_FLOAT) {
		// Float scalar
		double value;
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
		result_value = Value::DOUBLE(value);
	} else if (type_class == H5T_ENUM) {
		// Boolean or enum scalar
		int8_t value;
		H5Dread(dataset.get(), H5T_NATIVE_INT8, H5S_ALL, H5S_ALL, H5P_DEFAULT, &value);
		result_value = Value::BOOLEAN(value != 0);
	}

	H5Tclose(dtype_id);
	return result_value;
}

void H5ReaderMultithreaded::ReadUnsArray(const std::string &key, Vector &result, idx_t offset, idx_t count) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::string path = "/uns/" + key;

	// Check if the dataset exists
	if (!IsDatasetPresent("/uns", key)) {
		throw IOException("Uns array '%s' not found", key.c_str());
	}

	H5DatasetHandle dataset(*file_handle, path);
	H5DataspaceHandle dataspace(dataset.get());

	// Get dimensions
	int rank = H5Sget_simple_extent_ndims(dataspace.get());
	if (rank == 0) {
		throw IOException("Uns key '%s' is a scalar, not an array", key.c_str());
	}

	std::vector<hsize_t> dims(rank);
	H5Sget_simple_extent_dims(dataspace.get(), dims.data(), nullptr);

	// For now, we only support 1D arrays
	if (rank != 1) {
		throw NotImplementedException("Multi-dimensional uns arrays not yet supported");
	}

	hsize_t total_size = dims[0];

	// Ensure we don't read past the end
	if (offset >= total_size) {
		return;
	}
	if (offset + count > total_size) {
		count = total_size - offset;
	}

	// Set up hyperslab selection
	hsize_t h_offset = offset;
	hsize_t h_count = count;
	H5Sselect_hyperslab(dataspace.get(), H5S_SELECT_SET, &h_offset, nullptr, &h_count, nullptr);

	// Create memory dataspace
	H5DataspaceHandle memspace(1, &h_count);

	// Get data type
	hid_t dtype_id = H5Dget_type(dataset.get());
	H5T_class_t type_class = H5Tget_class(dtype_id);

	if (type_class == H5T_STRING) {
		// String array
		result.SetVectorType(VectorType::FLAT_VECTOR);
		auto string_vec = FlatVector::GetData<string_t>(result);
		auto &validity = FlatVector::Validity(result);
		validity.SetAllValid(count);

		if (H5Tis_variable_str(dtype_id)) {
			// Variable-length strings
			std::vector<char *> str_buffer(count);
			H5Dread(dataset.get(), dtype_id, memspace.get(), dataspace.get(), H5P_DEFAULT, str_buffer.data());

			for (idx_t i = 0; i < count; i++) {
				if (str_buffer[i]) {
					string_vec[i] = StringVector::AddString(result, str_buffer[i]);
					H5free_memory(str_buffer[i]);
				} else {
					validity.SetInvalid(i); // Mark NULL properly
				}
			}
		} else {
			// Fixed-length strings
			size_t str_size = H5Tget_size(dtype_id);
			std::vector<char> buffer(count * str_size);
			H5Dread(dataset.get(), dtype_id, memspace.get(), dataspace.get(), H5P_DEFAULT, buffer.data());

			for (idx_t i = 0; i < count; i++) {
				char *str_ptr = buffer.data() + i * str_size;
				size_t len = strnlen(str_ptr, str_size);
				string_vec[i] = StringVector::AddString(result, str_ptr, len);
			}
		}

#ifdef DEBUG
		// Verify the vector is valid
		Vector::Verify(result, count);
#endif
	} else if (type_class == H5T_INTEGER) {
		// Integer array
		auto data = FlatVector::GetData<int64_t>(result);
		H5Dread(dataset.get(), H5T_NATIVE_INT64, memspace.get(), dataspace.get(), H5P_DEFAULT, data);
	} else if (type_class == H5T_FLOAT) {
		// Float array
		auto data = FlatVector::GetData<double>(result);
		H5Dread(dataset.get(), H5T_NATIVE_DOUBLE, memspace.get(), dataspace.get(), H5P_DEFAULT, data);
	} else if (type_class == H5T_ENUM) {
		// Boolean array
		std::vector<int8_t> bool_buffer(count);
		H5Dread(dataset.get(), H5T_NATIVE_INT8, memspace.get(), dataspace.get(), H5P_DEFAULT, bool_buffer.data());

		auto data = FlatVector::GetData<bool>(result);
		for (idx_t i = 0; i < count; i++) {
			data[i] = bool_buffer[i] != 0;
		}
	}

	H5Tclose(dtype_id);
}

std::vector<std::string> H5ReaderMultithreaded::GetObspKeys() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
			if (H5LinkExists(*file_handle, path) && H5GetObjectType(*file_handle, path) == H5O_TYPE_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const std::exception &e) {
		// Return what we have
	}

	return keys;
}

std::vector<std::string> H5ReaderMultithreaded::GetVarpKeys() {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

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
			if (H5LinkExists(*file_handle, path) && H5GetObjectType(*file_handle, path) == H5O_TYPE_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const std::exception &e) {
		// Return what we have
	}

	return keys;
}

H5ReaderMultithreaded::SparseMatrixInfo H5ReaderMultithreaded::GetObspMatrixInfo(const std::string &key) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	SparseMatrixInfo info;
	std::string matrix_path = "/obsp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}

	try {
		// Check for indptr to determine format
		if (IsDatasetPresent(matrix_path, "indptr")) {
			H5DatasetHandle indptr_ds(*file_handle, matrix_path + "/indptr");
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
				H5DatasetHandle data_ds(*file_handle, matrix_path + "/data");
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
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	SparseMatrixInfo info;
	std::string matrix_path = "/varp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}

	try {
		// Check for indptr to determine format
		if (IsDatasetPresent(matrix_path, "indptr")) {
			H5DatasetHandle indptr_ds(*file_handle, matrix_path + "/indptr");
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
				H5DatasetHandle data_ds(*file_handle, matrix_path + "/data");
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

void H5ReaderMultithreaded::ReadObspMatrix(const std::string &key, Vector &row_result, Vector &col_result,
                                           Vector &value_result, idx_t offset, idx_t count) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::string matrix_path = "/obsp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}

	try {
		SparseMatrixInfo info = GetObspMatrixInfo(key);

		// Read the sparse matrix components
		H5DatasetHandle data_ds(*file_handle, matrix_path + "/data");
		H5DatasetHandle indices_ds(*file_handle, matrix_path + "/indices");
		H5DatasetHandle indptr_ds(*file_handle, matrix_path + "/indptr");

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
						H5Sselect_hyperslab(indices_file_space.get(), H5S_SELECT_SET, indices_offset, nullptr,
						                    indices_count, nullptr);
						H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_file_space.get(),
						        H5P_DEFAULT, &col_idx);

						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5DataspaceHandle data_mem_space(1, data_count);
						H5DataspaceHandle data_file_space(data_ds.get());
						H5Sselect_hyperslab(data_file_space.get(), H5S_SELECT_SET, data_offset, nullptr, data_count,
						                    nullptr);
						H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_file_space.get(),
						        H5P_DEFAULT, &val);

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

void H5ReaderMultithreaded::ReadVarpMatrix(const std::string &key, Vector &row_result, Vector &col_result,
                                           Vector &value_result, idx_t offset, idx_t count) {
	// Acquire global lock for HDF5 operations (no-op if library is threadsafe)
	auto h5_lock = H5GlobalLock::Acquire();

	std::string matrix_path = "/varp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}

	try {
		SparseMatrixInfo info = GetVarpMatrixInfo(key);

		// Read the sparse matrix components
		H5DatasetHandle data_ds(*file_handle, matrix_path + "/data");
		H5DatasetHandle indices_ds(*file_handle, matrix_path + "/indices");
		H5DatasetHandle indptr_ds(*file_handle, matrix_path + "/indptr");

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
						H5Sselect_hyperslab(indices_file_space.get(), H5S_SELECT_SET, indices_offset, nullptr,
						                    indices_count, nullptr);
						H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_file_space.get(),
						        H5P_DEFAULT, &col_idx);

						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5DataspaceHandle data_mem_space(1, data_count);
						H5DataspaceHandle data_file_space(data_ds.get());
						H5Sselect_hyperslab(data_file_space.get(), H5S_SELECT_SET, data_offset, nullptr, data_count,
						                    nullptr);
						H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_file_space.get(),
						        H5P_DEFAULT, &val);

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
H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseMatrixCSR(const std::string &path,
                                                                                   idx_t obs_start, idx_t obs_count,
                                                                                   idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;

	try {
		// Read CSR components from the specified path
		H5DatasetHandle data_ds(*file_handle, path + "/data");
		H5DatasetHandle indices_ds(*file_handle, path + "/indices");
		H5DatasetHandle indptr_ds(*file_handle, path + "/indptr");

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
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT,
			        indptr32.data());
			for (size_t i = 0; i < indptr32.size(); i++) {
				indptr[i] = indptr32[i];
			}
		} else {
			H5Dread(indptr_ds.get(), H5T_NATIVE_INT64, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT,
			        indptr.data());
		}

		// For each observation, read its sparse data
		for (idx_t obs_idx = 0; obs_idx < obs_count; obs_idx++) {
			int64_t row_start_idx = indptr[obs_idx];
			int64_t row_end_idx = indptr[obs_idx + 1];
			int64_t nnz = row_end_idx - row_start_idx;

			if (nnz == 0)
				continue;

			// Read column indices
			std::vector<int32_t> col_indices(nnz);
			hsize_t indices_offset[1] = {static_cast<hsize_t>(row_start_idx)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};

			H5DataspaceHandle indices_space(indices_ds.get());
			H5Sselect_hyperslab(indices_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle indices_mem_space(1, indices_count);
			H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_space.get(), H5P_DEFAULT,
			        col_indices.data());

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
					H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        float_data.data());
					for (size_t i = 0; i < float_data.size(); i++) {
						row_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					H5Dread(data_ds.get(), H5T_NATIVE_DOUBLE, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        row_data.data());
				}
			} else if (type_class == H5T_INTEGER) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<int32_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT32, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						row_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT64, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        int_data.data());
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

H5ReaderMultithreaded::SparseMatrixData H5ReaderMultithreaded::ReadSparseMatrixCSC(const std::string &path,
                                                                                   idx_t obs_start, idx_t obs_count,
                                                                                   idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;

	try {
		// Read CSC components from the specified path
		H5DatasetHandle data_ds(*file_handle, path + "/data");
		H5DatasetHandle indices_ds(*file_handle, path + "/indices");
		H5DatasetHandle indptr_ds(*file_handle, path + "/indptr");

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
				H5Dread(indptr_ds.get(), H5T_NATIVE_INT32, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT,
				        indptr32.data());
				col_indptr[0] = indptr32[0];
				col_indptr[1] = indptr32[1];
			} else {
				H5Dread(indptr_ds.get(), H5T_NATIVE_INT64, indptr_mem_space.get(), indptr_space.get(), H5P_DEFAULT,
				        col_indptr.data());
			}

			int64_t col_start_idx = col_indptr[0];
			int64_t col_end_idx = col_indptr[1];
			int64_t nnz = col_end_idx - col_start_idx;

			if (nnz == 0)
				continue;

			// Read row indices
			std::vector<int32_t> row_indices(nnz);
			hsize_t indices_offset[1] = {static_cast<hsize_t>(col_start_idx)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};

			H5DataspaceHandle indices_space(indices_ds.get());
			H5Sselect_hyperslab(indices_space.get(), H5S_SELECT_SET, indices_offset, nullptr, indices_count, nullptr);
			H5DataspaceHandle indices_mem_space(1, indices_count);
			H5Dread(indices_ds.get(), H5T_NATIVE_INT32, indices_mem_space.get(), indices_space.get(), H5P_DEFAULT,
			        row_indices.data());

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
					H5Dread(data_ds.get(), H5T_NATIVE_FLOAT, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        float_data.data());
					for (size_t i = 0; i < float_data.size(); i++) {
						col_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					H5Dread(data_ds.get(), H5T_NATIVE_DOUBLE, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        col_data.data());
				}
			} else if (type_class == H5T_INTEGER) {
				size_t data_size = H5Tget_size(data_dtype.get());
				if (data_size <= 4) {
					std::vector<int32_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT32, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        int_data.data());
					for (size_t i = 0; i < int_data.size(); i++) {
						col_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					H5Dread(data_ds.get(), H5T_NATIVE_INT64, data_mem_space.get(), data_space.get(), H5P_DEFAULT,
					        int_data.data());
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

void H5ReaderMultithreaded::ReadDenseMatrix(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start,
                                            idx_t var_count, std::vector<double> &values) {
	try {
		// Open the dataset
		H5DatasetHandle dataset(*file_handle, path);
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
