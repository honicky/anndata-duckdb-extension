#include "include/h5_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <cstring> // For strnlen
#include <unordered_set>
#include <mutex>
#include <set>
#include <algorithm>

namespace duckdb {

// Global tracking of active H5Reader instances to detect concurrent access
static std::mutex h5_reader_registry_mutex;
static std::set<std::string> active_h5_files;
static int active_h5_reader_count = 0;

H5Reader::H5Reader(const std::string &file_path) : file_path(file_path) {
	// Check for concurrent H5Reader instances
	{
		std::lock_guard<std::mutex> lock(h5_reader_registry_mutex);

		// Allow multiple readers for the same file, but not different files
		if (!active_h5_files.empty() && active_h5_files.find(file_path) == active_h5_files.end()) {
			// Different file is already open - this would cause thread safety issues
			std::string active_file = *active_h5_files.begin();
			throw InvalidInputException(
			    "Cannot open multiple HDF5 files concurrently. "
			    "Attempted to open '%s' while '%s' is already open. "
			    "HDF5 C++ API does not support thread-safe concurrent file access. "
			    "Consider: (1) Processing files sequentially, (2) Using separate DuckDB connections, "
			    "or (3) Caching results in temporary tables.",
			    file_path.c_str(), active_file.c_str());
		}

		active_h5_files.insert(file_path);
		active_h5_reader_count++;
	}

	try {
		// Turn off HDF5 error printing (only needs to be done once per process)
		static bool error_printing_disabled = false;
		if (!error_printing_disabled) {
			H5::Exception::dontPrint();
			error_printing_disabled = true;
		}

		// Open the HDF5 file
		file = make_uniq<H5::H5File>(file_path, H5F_ACC_RDONLY);
	} catch (const H5::FileIException &e) {
		// Clean up tracking on failure
		{
			std::lock_guard<std::mutex> lock(h5_reader_registry_mutex);
			active_h5_files.erase(file_path);
			active_h5_reader_count--;
		}
		throw IOException("Failed to open HDF5 file: " + file_path);
	} catch (const H5::Exception &e) {
		// Clean up tracking on failure
		{
			std::lock_guard<std::mutex> lock(h5_reader_registry_mutex);
			active_h5_files.erase(file_path);
			active_h5_reader_count--;
		}
		throw IOException("HDF5 error opening file: " + file_path);
	}
}

H5Reader::~H5Reader() {
	// Clean up tracking
	{
		std::lock_guard<std::mutex> lock(h5_reader_registry_mutex);
		active_h5_reader_count--;
		// Only remove from set if this was the last reader for this file
		if (active_h5_reader_count == 0 || std::count(active_h5_files.begin(), active_h5_files.end(), file_path) == 1) {
			active_h5_files.erase(file_path);
		}
	}

	if (file) {
		try {
			file->close();
		} catch (...) {
			// Ignore close errors in destructor
		}
	}
}

bool H5Reader::IsValidAnnData() {
	// At least obs or var must exist for a valid AnnData file
	return HasObs() || HasVar();
}

bool H5Reader::HasObs() {
	return IsGroupPresent("/obs");
}

bool H5Reader::HasVar() {
	return IsGroupPresent("/var");
}

bool H5Reader::HasX() {
	return IsGroupPresent("/X") || IsDatasetPresent("/", "X");
}

size_t H5Reader::GetObsCount() {
	try {
		// Try to get shape from obs/index first
		if (IsDatasetPresent("/obs", "index")) {
			H5::DataSet dataset = file->openDataSet("/obs/index");
			H5::DataSpace dataspace = dataset.getSpace();
			hsize_t dims[1];
			dataspace.getSimpleExtentDims(dims);
			return dims[0];
		}

		// Fall back to any categorical column
		auto members = GetGroupMembers("/obs");
		for (const auto &member : members) {
			std::string codes_path = "/obs/" + member + "/codes";
			if (IsDatasetPresent("/obs/" + member, "codes")) {
				H5::DataSet dataset = file->openDataSet(codes_path);
				H5::DataSpace dataspace = dataset.getSpace();
				hsize_t dims[1];
				dataspace.getSimpleExtentDims(dims);
				return dims[0];
			}
		}

		return 0;
	} catch (const H5::Exception &e) {
		return 0;
	}
}

size_t H5Reader::GetVarCount() {
	try {
		// Try to get shape from var/_index first
		if (IsDatasetPresent("/var", "_index")) {
			H5::DataSet dataset = file->openDataSet("/var/_index");
			H5::DataSpace dataspace = dataset.getSpace();
			hsize_t dims[1];
			dataspace.getSimpleExtentDims(dims);
			return dims[0];
		}

		// Fall back to any direct dataset
		auto members = GetGroupMembers("/var");
		for (const auto &member : members) {
			if (!StringUtil::StartsWith(member, "_") && IsDatasetPresent("/var", member)) {
				H5::DataSet dataset = file->openDataSet("/var/" + member);
				H5::DataSpace dataspace = dataset.getSpace();
				hsize_t dims[1];
				dataspace.getSimpleExtentDims(dims);
				return dims[0];
			}
		}

		return 0;
	} catch (const H5::Exception &e) {
		return 0;
	}
}

std::vector<H5Reader::ColumnInfo> H5Reader::GetObsColumns() {
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

			// Note: We now include _index and index columns as they contain important identifiers
			// obs_idx provides the numeric index for joins, while _index/index contain the actual cell IDs

			ColumnInfo col;
			col.original_name = member; // Store the original HDF5 dataset name
			col.name = member;

			// Handle duplicate column names (case-insensitive) by mangling
			std::string lower_name = StringUtil::Lower(col.name);
			while (seen_names.count(lower_name) > 0) {
				// Add underscore until unique
				col.name += "_";
				lower_name = StringUtil::Lower(col.name);
			}
			seen_names.insert(lower_name);

			// Check if it's categorical (has codes and categories)
			if (IsGroupPresent("/obs/" + member)) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;

				// Load categories
				try {
					H5::DataSet cat_dataset = file->openDataSet("/obs/" + member + "/categories");
					H5::DataSpace cat_space = cat_dataset.getSpace();
					hsize_t cat_dims[1];
					cat_space.getSimpleExtentDims(cat_dims);

					// Read string categories
					H5::DataType dtype = cat_dataset.getDataType();
					if (dtype.getClass() == H5T_STRING) {
						H5::StrType str_type = cat_dataset.getStrType();
						size_t str_size = str_type.getSize();

						col.categories.resize(cat_dims[0]);
						for (size_t i = 0; i < cat_dims[0]; i++) {
							char *buffer = new char[str_size + 1];
							hsize_t offset[1] = {i};
							hsize_t count[1] = {1};
							H5::DataSpace mem_space(1, count);
							cat_space.selectHyperslab(H5S_SELECT_SET, count, offset);
							cat_dataset.read(buffer, str_type, mem_space, cat_space);
							buffer[str_size] = '\0';
							col.categories[i] = std::string(buffer);
							delete[] buffer;
						}
					}
				} catch (...) {
					// If we can't read categories, still add the column
				}
			} else if (IsDatasetPresent("/obs", member)) {
				// Direct dataset
				H5::DataSet dataset = file->openDataSet("/obs/" + member);
				col.type = H5TypeToDuckDBType(dataset.getDataType());
			} else {
				continue; // Skip unknown types
			}

			columns.push_back(col);
		}
	} catch (const H5::Exception &e) {
		// Return what we have so far
	}

	// If no columns found, return dummy schema
	if (columns.empty()) {
		ColumnInfo col1;
		col1.name = "cell_id";
		col1.original_name = "cell_id";
		col1.type = LogicalType::VARCHAR;
		col1.is_categorical = false;
		columns.push_back(col1);

		ColumnInfo col2;
		col2.name = "cell_type";
		col2.original_name = "cell_type";
		col2.type = LogicalType::VARCHAR;
		col2.is_categorical = false;
		columns.push_back(col2);

		ColumnInfo col3;
		col3.name = "n_genes";
		col3.original_name = "n_genes";
		col3.type = LogicalType::INTEGER;
		col3.is_categorical = false;
		columns.push_back(col3);

		ColumnInfo col4;
		col4.name = "n_counts";
		col4.original_name = "n_counts";
		col4.type = LogicalType::DOUBLE;
		col4.is_categorical = false;
		columns.push_back(col4);
	}

	return columns;
}

std::vector<H5Reader::ColumnInfo> H5Reader::GetVarColumns() {
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

			// Note: We now include _index column as it contains important gene identifiers
			// var_idx provides the numeric index for joins, while _index contains the actual gene IDs

			ColumnInfo col;
			col.original_name = member; // Store the original HDF5 dataset name
			col.name = member;

			// Handle duplicate column names (case-insensitive) by mangling
			std::string lower_name = StringUtil::Lower(col.name);
			while (seen_names.count(lower_name) > 0) {
				// Add underscore until unique
				col.name += "_";
				lower_name = StringUtil::Lower(col.name);
			}
			seen_names.insert(lower_name);

			// Check if it's categorical
			if (IsGroupPresent("/var/" + member)) {
				col.is_categorical = true;
				col.type = LogicalType::VARCHAR;

				// Load categories similar to obs
				try {
					H5::DataSet cat_dataset = file->openDataSet("/var/" + member + "/categories");
					// ... (similar category loading code)
				} catch (...) {
					// Continue without categories
				}
			} else if (IsDatasetPresent("/var", member)) {
				// Direct dataset
				H5::DataSet dataset = file->openDataSet("/var/" + member);
				col.type = H5TypeToDuckDBType(dataset.getDataType());
			} else {
				continue;
			}

			columns.push_back(col);
		}
	} catch (const H5::Exception &e) {
		// Return what we have
	}

	// If no columns found, return dummy schema
	if (columns.empty()) {
		ColumnInfo col1;
		col1.name = "gene_id";
		col1.original_name = "gene_id";
		col1.type = LogicalType::VARCHAR;
		col1.is_categorical = false;
		columns.push_back(col1);

		ColumnInfo col2;
		col2.name = "gene_name";
		col2.original_name = "gene_name";
		col2.type = LogicalType::VARCHAR;
		col2.is_categorical = false;
		columns.push_back(col2);

		ColumnInfo col3;
		col3.name = "highly_variable";
		col3.original_name = "highly_variable";
		col3.type = LogicalType::BOOLEAN;
		col3.is_categorical = false;
		columns.push_back(col3);

		ColumnInfo col4;
		col4.name = "mean_counts";
		col4.original_name = "mean_counts";
		col4.type = LogicalType::DOUBLE;
		col4.is_categorical = false;
		columns.push_back(col4);
	}

	return columns;
}

void H5Reader::ReadObsColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
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
		if (IsGroupPresent(group_path)) {
			// Handle categorical columns
			try {
				// Read codes
				H5::DataSet codes_dataset = file->openDataSet(group_path + "/codes");
				H5::DataSpace codes_space = codes_dataset.getSpace();

				// Read categories into memory (usually small)
				H5::DataSet cat_dataset = file->openDataSet(group_path + "/categories");
				H5::DataSpace cat_space = cat_dataset.getSpace();
				hsize_t cat_dims[1];
				cat_space.getSimpleExtentDims(cat_dims);

				// Read all categories first
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);

				H5::DataType cat_dtype = cat_dataset.getDataType();
				if (cat_dtype.getClass() == H5T_STRING) {
					H5::StrType str_type = cat_dataset.getStrType();

					if (str_type.isVariableStr()) {
						// Variable-length strings
						std::vector<char *> str_buffer(cat_dims[0]);
						cat_dataset.read(str_buffer.data(), str_type);

						for (size_t i = 0; i < cat_dims[0]; i++) {
							if (str_buffer[i]) {
								categories.emplace_back(str_buffer[i]);
							} else {
								categories.emplace_back("");
							}
						}

						// Clean up variable-length strings
						cat_dataset.vlenReclaim(str_buffer.data(), str_type, cat_space);
					} else {
						// Fixed-length strings
						size_t str_size = str_type.getSize();
						std::vector<char> buffer(cat_dims[0] * str_size);
						cat_dataset.read(buffer.data(), str_type);

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

				codes_space.selectHyperslab(H5S_SELECT_SET, h_count, h_offset);
				H5::DataSpace mem_space(1, h_count);

				codes_dataset.read(codes.data(), H5::PredType::NATIVE_INT8, mem_space, codes_space);

				// Map codes to categories and set in result vector
				for (idx_t i = 0; i < count; i++) {
					int8_t code = codes[i];
					if (code >= 0 && code < static_cast<int8_t>(categories.size())) {
						result.SetValue(i, Value(categories[code]));
					} else {
						result.SetValue(i, Value()); // NULL for invalid codes
					}
				}

			} catch (const H5::Exception &e) {
				// On error, return NULLs
				for (idx_t i = 0; i < count; i++) {
					result.SetValue(i, Value());
				}
			}
		} else if (IsDatasetPresent("/obs", column_name)) {
			// Direct dataset
			H5::DataSet dataset = file->openDataSet("/obs/" + column_name);
			H5::DataSpace dataspace = dataset.getSpace();
			H5::DataType dtype = dataset.getDataType();

			// Select hyperslab
			hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
			hsize_t h_count[1] = {static_cast<hsize_t>(count)};
			dataspace.selectHyperslab(H5S_SELECT_SET, h_count, h_offset);

			// Memory space
			H5::DataSpace mem_space(1, h_count);

			// Read based on type
			if (dtype.getClass() == H5T_INTEGER) {
				if (dtype.getSize() <= 4) {
					std::vector<int32_t> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_INT32, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(data[i]));
					}
				} else {
					std::vector<int64_t> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_INT64, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(data[i]));
					}
				}
			} else if (dtype.getClass() == H5T_FLOAT) {
				if (dtype.getSize() <= 4) {
					std::vector<float> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_FLOAT, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(data[i]));
					}
				} else {
					std::vector<double> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_DOUBLE, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(data[i]));
					}
				}
			} else if (dtype.getClass() == H5T_STRING) {
				// Handle string data more safely
				try {
					H5::StrType str_type = dataset.getStrType();
					bool is_variable = str_type.isVariableStr();

					if (is_variable) {
						// Variable-length strings - read one at a time to avoid memory issues
						for (idx_t i = 0; i < count; i++) {
							try {
								hsize_t str_offset[1] = {static_cast<hsize_t>(offset + i)};
								hsize_t str_count[1] = {1};
								H5::DataSpace str_mem_space(1, str_count);
								H5::DataSpace str_file_space = dataset.getSpace();
								str_file_space.selectHyperslab(H5S_SELECT_SET, str_count, str_offset);

								char *buffer = nullptr;
								dataset.read(&buffer, str_type, str_mem_space, str_file_space);

								if (buffer != nullptr) {
									std::string str(buffer);
									// Trim trailing whitespace
									str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);
									result.SetValue(i, Value(str));
									// Free the buffer allocated by HDF5
									free(buffer);
								} else {
									result.SetValue(i, Value());
								}
							} catch (...) {
								result.SetValue(i, Value());
							}
						}
					} else {
						// Fixed-length strings
						size_t str_size = str_type.getSize();
						std::vector<char> buffer(count * str_size + 1, '\0');
						dataset.read(buffer.data(), str_type, mem_space, dataspace);

						for (idx_t i = 0; i < count; i++) {
							// Extract string from buffer
							char *str_start = buffer.data() + i * str_size;
							// Create string, ensuring null termination
							std::string str(str_start, strnlen(str_start, str_size));
							// Trim trailing whitespace
							str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);

							if (!str.empty()) {
								result.SetValue(i, Value(str));
							} else {
								result.SetValue(i, Value());
							}
						}
					}
				} catch (const H5::Exception &e) {
					// If string reading fails, return NULLs
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value());
					}
				}
			} else {
				// Unsupported type - use NULL
				for (idx_t i = 0; i < count; i++) {
					result.SetValue(i, Value());
				}
			}
		} else {
			// Column not found - return NULLs
			for (idx_t i = 0; i < count; i++) {
				result.SetValue(i, Value());
			}
		}
	} catch (const H5::Exception &e) {
		// On error, return NULLs
		for (idx_t i = 0; i < count; i++) {
			result.SetValue(i, Value());
		}
	}
}

void H5Reader::ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count) {
	// Handle var_idx column
	if (column_name == "var_idx") {
		for (idx_t i = 0; i < count; i++) {
			result.SetValue(i, Value::BIGINT(offset + i));
		}
		return;
	}

	try {
		// Check if it's a categorical column
		std::string group_path = "/var/" + column_name;
		if (IsGroupPresent(group_path)) {
			// Handle categorical columns (same logic as obs)
			try {
				// Read codes
				H5::DataSet codes_dataset = file->openDataSet(group_path + "/codes");
				H5::DataSpace codes_space = codes_dataset.getSpace();

				// Read categories into memory
				H5::DataSet cat_dataset = file->openDataSet(group_path + "/categories");
				H5::DataSpace cat_space = cat_dataset.getSpace();
				hsize_t cat_dims[1];
				cat_space.getSimpleExtentDims(cat_dims);

				// Read all categories first
				std::vector<std::string> categories;
				categories.reserve(cat_dims[0]);

				H5::DataType cat_dtype = cat_dataset.getDataType();
				if (cat_dtype.getClass() == H5T_STRING) {
					H5::StrType str_type = cat_dataset.getStrType();

					if (str_type.isVariableStr()) {
						// Variable-length strings
						std::vector<char *> str_buffer(cat_dims[0]);
						cat_dataset.read(str_buffer.data(), str_type);

						for (size_t i = 0; i < cat_dims[0]; i++) {
							if (str_buffer[i]) {
								categories.emplace_back(str_buffer[i]);
							} else {
								categories.emplace_back("");
							}
						}

						// Clean up variable-length strings
						cat_dataset.vlenReclaim(str_buffer.data(), str_type, cat_space);
					} else {
						// Fixed-length strings
						size_t str_size = str_type.getSize();
						std::vector<char> buffer(cat_dims[0] * str_size);
						cat_dataset.read(buffer.data(), str_type);

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

				codes_space.selectHyperslab(H5S_SELECT_SET, h_count, h_offset);
				H5::DataSpace mem_space(1, h_count);

				codes_dataset.read(codes.data(), H5::PredType::NATIVE_INT8, mem_space, codes_space);

				// Map codes to categories and set in result vector
				for (idx_t i = 0; i < count; i++) {
					int8_t code = codes[i];
					if (code >= 0 && code < static_cast<int8_t>(categories.size())) {
						result.SetValue(i, Value(categories[code]));
					} else {
						result.SetValue(i, Value()); // NULL for invalid codes
					}
				}

			} catch (const H5::Exception &e) {
				// On error, return NULLs
				for (idx_t i = 0; i < count; i++) {
					result.SetValue(i, Value());
				}
			}
		} else if (IsDatasetPresent("/var", column_name)) {
			// Direct dataset
			H5::DataSet dataset = file->openDataSet("/var/" + column_name);
			H5::DataSpace dataspace = dataset.getSpace();
			H5::DataType dtype = dataset.getDataType();

			// Select hyperslab
			hsize_t h_offset[1] = {static_cast<hsize_t>(offset)};
			hsize_t h_count[1] = {static_cast<hsize_t>(count)};
			dataspace.selectHyperslab(H5S_SELECT_SET, h_count, h_offset);

			// Memory space
			H5::DataSpace mem_space(1, h_count);

			// Read based on type
			if (dtype.getClass() == H5T_INTEGER) {
				if (dtype.getSize() <= 4) {
					std::vector<int32_t> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_INT32, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::INTEGER(data[i]));
					}
				} else {
					std::vector<int64_t> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_INT64, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::BIGINT(data[i]));
					}
				}
			} else if (dtype.getClass() == H5T_FLOAT) {
				if (dtype.getSize() <= 4) {
					std::vector<float> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_FLOAT, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::FLOAT(data[i]));
					}
				} else {
					std::vector<double> data(count);
					dataset.read(data.data(), H5::PredType::NATIVE_DOUBLE, mem_space, dataspace);
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value::DOUBLE(data[i]));
					}
				}
			} else if (dtype.getClass() == H5T_STRING) {
				// Handle string data more safely
				try {
					H5::StrType str_type = dataset.getStrType();
					bool is_variable = str_type.isVariableStr();

					if (is_variable) {
						// Variable-length strings - read one at a time to avoid memory issues
						for (idx_t i = 0; i < count; i++) {
							try {
								hsize_t str_offset[1] = {static_cast<hsize_t>(offset + i)};
								hsize_t str_count[1] = {1};
								H5::DataSpace str_mem_space(1, str_count);
								H5::DataSpace str_file_space = dataset.getSpace();
								str_file_space.selectHyperslab(H5S_SELECT_SET, str_count, str_offset);

								char *buffer = nullptr;
								dataset.read(&buffer, str_type, str_mem_space, str_file_space);

								if (buffer != nullptr) {
									std::string str(buffer);
									// Trim trailing whitespace
									str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);
									result.SetValue(i, Value(str));
									// Free the buffer allocated by HDF5
									free(buffer);
								} else {
									result.SetValue(i, Value());
								}
							} catch (...) {
								result.SetValue(i, Value());
							}
						}
					} else {
						// Fixed-length strings
						size_t str_size = str_type.getSize();
						std::vector<char> buffer(count * str_size + 1, '\0');
						dataset.read(buffer.data(), str_type, mem_space, dataspace);

						for (idx_t i = 0; i < count; i++) {
							// Extract string from buffer
							char *str_start = buffer.data() + i * str_size;
							// Create string, ensuring null termination
							std::string str(str_start, strnlen(str_start, str_size));
							// Trim trailing whitespace
							str.erase(str.find_last_not_of(" \t\n\r\f\v") + 1);

							if (!str.empty()) {
								result.SetValue(i, Value(str));
							} else {
								result.SetValue(i, Value());
							}
						}
					}
				} catch (const H5::Exception &e) {
					// If string reading fails, return NULLs
					for (idx_t i = 0; i < count; i++) {
						result.SetValue(i, Value());
					}
				}
			} else {
				// Unsupported type - use NULL
				for (idx_t i = 0; i < count; i++) {
					result.SetValue(i, Value());
				}
			}
		} else {
			// Column not found - return NULLs
			for (idx_t i = 0; i < count; i++) {
				result.SetValue(i, Value());
			}
		}
	} catch (const H5::Exception &e) {
		// On error, return NULLs
		for (idx_t i = 0; i < count; i++) {
			result.SetValue(i, Value());
		}
	}
}

LogicalType H5Reader::H5TypeToDuckDBType(const H5::DataType &h5_type) {
	H5T_class_t type_class = h5_type.getClass();

	switch (type_class) {
	case H5T_INTEGER: {
		size_t size = h5_type.getSize();
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
		size_t size = h5_type.getSize();
		if (size <= 4) {
			return LogicalType::FLOAT;
		}
		return LogicalType::DOUBLE;
	}
	case H5T_STRING:
		return LogicalType::VARCHAR;
	case H5T_ENUM:
		return LogicalType::VARCHAR;
	default:
		return LogicalType::VARCHAR;
	}
}

bool H5Reader::IsGroupPresent(const std::string &group_name) {
	try {
		H5::Group group = file->openGroup(group_name);
		// Group will be closed when it goes out of scope
		return true;
	} catch (const H5::Exception &e) {
		return false;
	}
}

bool H5Reader::IsDatasetPresent(const std::string &group_name, const std::string &dataset_name) {
	try {
		H5::Group group = file->openGroup(group_name);
		group.openDataSet(dataset_name);
		return true;
	} catch (const H5::Exception &e) {
		return false;
	}
}

std::vector<std::string> H5Reader::GetGroupMembers(const std::string &group_name) {
	std::vector<std::string> members;

	try {
		H5::Group group = file->openGroup(group_name);

		for (hsize_t i = 0; i < group.getNumObjs(); i++) {
			members.push_back(group.getObjnameByIdx(i));
		}
	} catch (const H5::Exception &e) {
		// Return empty vector on error
	}

	return members;
}

H5Reader::XMatrixInfo H5Reader::GetXMatrixInfo() {
	XMatrixInfo info;
	info.n_obs = GetObsCount();
	info.n_var = GetVarCount();

	try {
		// Check if X is a dataset (dense) or group (sparse)
		if (IsDatasetPresent("/", "X")) {
			// Dense matrix
			H5::DataSet dataset = file->openDataSet("/X");
			H5::DataSpace dataspace = dataset.getSpace();

			// Get dimensions
			hsize_t dims[2];
			int ndims = dataspace.getSimpleExtentDims(dims);
			if (ndims == 2) {
				info.n_obs = dims[0];
				info.n_var = dims[1];
			}

			// Get data type
			H5::DataType dtype = dataset.getDataType();
			if (dtype.getClass() == H5T_FLOAT) {
				info.dtype = (dtype.getSize() <= 4) ? LogicalType::FLOAT : LogicalType::DOUBLE;
			} else if (dtype.getClass() == H5T_INTEGER) {
				info.dtype = LogicalType::INTEGER;
			}

			info.is_sparse = false;
			info.sparse_format = "";
		} else if (IsGroupPresent("/X")) {
			// Sparse matrix
			info.is_sparse = true;

			// Determine sparse format by checking indptr dimensions
			if (IsDatasetPresent("/X", "indptr")) {
				H5::DataSet indptr = file->openDataSet("/X/indptr");
				H5::DataSpace indptr_space = indptr.getSpace();
				hsize_t indptr_dims[1];
				indptr_space.getSimpleExtentDims(indptr_dims);

				// CSR has n_obs + 1 elements, CSC has n_var + 1
				if (indptr_dims[0] == info.n_obs + 1) {
					info.sparse_format = "csr";
				} else if (indptr_dims[0] == info.n_var + 1) {
					info.sparse_format = "csc";
				}
			}

			// Get data type
			if (IsDatasetPresent("/X", "data")) {
				H5::DataSet data = file->openDataSet("/X/data");
				H5::DataType dtype = data.getDataType();
				if (dtype.getClass() == H5T_FLOAT) {
					info.dtype = (dtype.getSize() <= 4) ? LogicalType::FLOAT : LogicalType::DOUBLE;
				} else if (dtype.getClass() == H5T_INTEGER) {
					info.dtype = LogicalType::INTEGER;
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return default info
	}

	return info;
}

std::vector<std::string> H5Reader::GetVarNames(const std::string &column_name) {
	std::vector<std::string> names;
	size_t var_count = GetVarCount();
	names.reserve(var_count);

	try {
		std::string dataset_path = "/var/" + column_name;

		if (IsDatasetPresent("/var", column_name)) {
			H5::DataSet dataset = file->openDataSet(dataset_path);
			H5::DataType dtype = dataset.getDataType();

			if (dtype.getClass() == H5T_STRING) {
				H5::StrType str_type = dataset.getStrType();
				H5::DataSpace dataspace = dataset.getSpace();

				if (str_type.isVariableStr()) {
					// Variable-length strings
					std::vector<char *> str_buffer(var_count);
					dataset.read(str_buffer.data(), str_type);

					for (size_t i = 0; i < var_count; i++) {
						if (str_buffer[i]) {
							names.emplace_back(str_buffer[i]);
						} else {
							names.emplace_back("gene_" + std::to_string(i));
						}
					}

					// Clean up
					dataset.vlenReclaim(str_buffer.data(), str_type, dataspace);
				} else {
					// Fixed-length strings
					size_t str_size = str_type.getSize();
					std::vector<char> buffer(var_count * str_size);
					dataset.read(buffer.data(), str_type);

					for (size_t i = 0; i < var_count; i++) {
						char *str_ptr = buffer.data() + i * str_size;
						size_t len = strnlen(str_ptr, str_size);
						std::string name(str_ptr, len);
						// Trim whitespace
						name.erase(name.find_last_not_of(" \t\n\r\f\v") + 1);
						if (!name.empty()) {
							names.push_back(name);
						} else {
							names.push_back("gene_" + std::to_string(i));
						}
					}
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Fall back to generic names
	}

	// If we couldn't read names, generate generic ones
	if (names.empty()) {
		for (size_t i = 0; i < var_count; i++) {
			names.push_back("gene_" + std::to_string(i));
		}
	}

	return names;
}

void H5Reader::ReadDenseMatrix(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start,
                               idx_t var_count, std::vector<double> &values) {
	values.clear();
	values.resize(obs_count * var_count, 0.0);

	try {
		H5::DataSet dataset = file->openDataSet(path);
		H5::DataSpace dataspace = dataset.getSpace();

		// Select hyperslab in file
		hsize_t h_offset[2] = {static_cast<hsize_t>(obs_start), static_cast<hsize_t>(var_start)};
		hsize_t h_count[2] = {static_cast<hsize_t>(obs_count), static_cast<hsize_t>(var_count)};
		dataspace.selectHyperslab(H5S_SELECT_SET, h_count, h_offset);

		// Create memory dataspace
		H5::DataSpace mem_space(2, h_count);

		// Read the data
		H5::DataType dtype = dataset.getDataType();
		if (dtype.getClass() == H5T_FLOAT) {
			if (dtype.getSize() <= 4) {
				std::vector<float> float_values(obs_count * var_count);
				dataset.read(float_values.data(), H5::PredType::NATIVE_FLOAT, mem_space, dataspace);
				for (size_t i = 0; i < float_values.size(); i++) {
					values[i] = static_cast<double>(float_values[i]);
				}
			} else {
				dataset.read(values.data(), H5::PredType::NATIVE_DOUBLE, mem_space, dataspace);
			}
		} else if (dtype.getClass() == H5T_INTEGER) {
			if (dtype.getSize() <= 4) {
				std::vector<int32_t> int_values(obs_count * var_count);
				dataset.read(int_values.data(), H5::PredType::NATIVE_INT32, mem_space, dataspace);
				for (size_t i = 0; i < int_values.size(); i++) {
					values[i] = static_cast<double>(int_values[i]);
				}
			} else {
				std::vector<int64_t> int_values(obs_count * var_count);
				dataset.read(int_values.data(), H5::PredType::NATIVE_INT64, mem_space, dataspace);
				for (size_t i = 0; i < int_values.size(); i++) {
					values[i] = static_cast<double>(int_values[i]);
				}
			}
		}
	} catch (const H5::Exception &e) {
		// On error, values remain as zeros
		// Caller can handle this case
	}
}

void H5Reader::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count,
                           std::vector<double> &values) {
	values.clear();
	values.resize(obs_count * var_count, 0.0);

	try {
		if (IsDatasetPresent("/", "X")) {
			// Dense matrix - use generic reader
			ReadDenseMatrix("/X", obs_start, obs_count, var_start, var_count, values);
		} else if (IsGroupPresent("/X")) {
			// Sparse matrix - convert to dense for backward compatibility
			auto sparse_data = ReadSparseXMatrix(obs_start, obs_count, var_start, var_count);

			// Fill in the dense matrix from sparse data
			for (size_t i = 0; i < sparse_data.values.size(); i++) {
				idx_t obs_idx = sparse_data.row_indices[i];
				idx_t var_idx = sparse_data.col_indices[i];
				values[obs_idx * var_count + var_idx] = sparse_data.values[i];
			}
		}
	} catch (const H5::Exception &e) {
		// On error, values remain as zeros
	}
}

H5Reader::SparseMatrixData H5Reader::ReadSparseXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start,
                                                       idx_t var_count) {
	// Use the generic sparse matrix reader for the X matrix path
	return ReadSparseMatrixAtPath("/X", obs_start, obs_count, var_start, var_count);
}

H5Reader::SparseMatrixData H5Reader::ReadSparseMatrixAtPath(const std::string &path, idx_t obs_start, idx_t obs_count,
                                                            idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;

	try {
		// Check for encoding attribute to determine format
		bool is_csr = false;
		bool is_csc = false;

		// Try to read the encoding attribute
		try {
			H5::Group matrix_group = file->openGroup(path);
			if (matrix_group.attrExists("encoding-type")) {
				H5::Attribute encoding_attr = matrix_group.openAttribute("encoding-type");
				H5::StrType str_type = encoding_attr.getStrType();
				std::string encoding;
				encoding_attr.read(str_type, encoding);

				if (encoding == "csr" || encoding == "CSR") {
					is_csr = true;
				} else if (encoding == "csc" || encoding == "CSC") {
					is_csc = true;
				}
			}
		} catch (...) {
			// If no encoding attribute, try to detect by structure
		}

		// If no encoding attribute found, detect by structure
		if (!is_csr && !is_csc) {
			// Check for indptr to determine if it's sparse
			if (IsDatasetPresent(path, "indptr")) {
				// Check the dimensions to determine CSR vs CSC
				H5::DataSet indptr_ds = file->openDataSet(path + "/indptr");
				H5::DataSpace indptr_space = indptr_ds.getSpace();
				hsize_t indptr_dims[1];
				indptr_space.getSimpleExtentDims(indptr_dims);

				// indptr length = n_obs + 1 for CSR, n_var + 1 for CSC
				size_t indptr_len = indptr_dims[0] - 1;

				if (indptr_len == GetObsCount()) {
					is_csr = true;
				} else if (indptr_len == GetVarCount()) {
					is_csc = true;
				}
			}
		}

		if (is_csr) {
			// CSR format reading
			return ReadSparseMatrixCSR(path, obs_start, obs_count, var_start, var_count);
		} else if (is_csc) {
			// CSC format reading
			return ReadSparseMatrixCSC(path, obs_start, obs_count, var_start, var_count);
		}
	} catch (const H5::Exception &e) {
		// Return empty sparse data on error
	}

	return sparse_data;
}

H5Reader::SparseMatrixData H5Reader::ReadSparseXMatrixCSR(idx_t obs_start, idx_t obs_count, idx_t var_start,
                                                          idx_t var_count) {
	// Use the generic CSR reader for X matrix
	return ReadSparseMatrixCSR("/X", obs_start, obs_count, var_start, var_count);
}

H5Reader::SparseMatrixData H5Reader::ReadSparseMatrixCSR(const std::string &path, idx_t obs_start, idx_t obs_count,
                                                         idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;

	try {
		// Read CSR components from the specified path
		H5::DataSet data_ds = file->openDataSet(path + "/data");
		H5::DataSet indices_ds = file->openDataSet(path + "/indices");
		H5::DataSet indptr_ds = file->openDataSet(path + "/indptr");

		// Get total number of observations
		H5::DataSpace indptr_space = indptr_ds.getSpace();
		hsize_t indptr_dims[1];
		indptr_space.getSimpleExtentDims(indptr_dims);
		size_t total_obs = indptr_dims[0] - 1; // indptr has n_obs + 1 elements

		// Read indptr for the requested observation range (need obs_start to obs_start + obs_count + 1)
		std::vector<int64_t> indptr(obs_count + 1);
		hsize_t indptr_offset[1] = {static_cast<hsize_t>(obs_start)};
		hsize_t indptr_count[1] = {static_cast<hsize_t>(obs_count + 1)};
		H5::DataSpace indptr_sel_space = indptr_ds.getSpace();
		indptr_sel_space.selectHyperslab(H5S_SELECT_SET, indptr_count, indptr_offset);
		H5::DataSpace indptr_mem_space(1, indptr_count);

		// Read indptr based on its actual type
		H5::DataType indptr_dtype = indptr_ds.getDataType();
		if (indptr_dtype.getSize() <= 4) {
			std::vector<int32_t> indptr32(obs_count + 1);
			indptr_ds.read(indptr32.data(), H5::PredType::NATIVE_INT32, indptr_mem_space, indptr_sel_space);
			for (size_t i = 0; i < indptr32.size(); i++) {
				indptr[i] = indptr32[i];
			}
		} else {
			indptr_ds.read(indptr.data(), H5::PredType::NATIVE_INT64, indptr_mem_space, indptr_sel_space);
		}

		// For each observation, read its sparse data
		for (idx_t obs_idx = 0; obs_idx < obs_count; obs_idx++) {
			int64_t row_start = indptr[obs_idx];
			int64_t row_end = indptr[obs_idx + 1];
			int64_t nnz = row_end - row_start;

			if (nnz == 0)
				continue;

			// Read indices and data for this row
			std::vector<int32_t> col_indices(nnz);
			std::vector<double> row_data(nnz);

			// Read column indices
			hsize_t indices_offset[1] = {static_cast<hsize_t>(row_start)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
			H5::DataSpace indices_sel_space = indices_ds.getSpace();
			indices_sel_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
			H5::DataSpace indices_mem_space(1, indices_count);
			indices_ds.read(col_indices.data(), H5::PredType::NATIVE_INT32, indices_mem_space, indices_sel_space);

			// Read data values
			H5::DataSpace data_sel_space = data_ds.getSpace();
			data_sel_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
			H5::DataSpace data_mem_space(1, indices_count);

			H5::DataType data_dtype = data_ds.getDataType();
			if (data_dtype.getClass() == H5T_FLOAT) {
				if (data_dtype.getSize() <= 4) {
					std::vector<float> float_data(nnz);
					data_ds.read(float_data.data(), H5::PredType::NATIVE_FLOAT, data_mem_space, data_sel_space);
					for (size_t i = 0; i < float_data.size(); i++) {
						row_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					data_ds.read(row_data.data(), H5::PredType::NATIVE_DOUBLE, data_mem_space, data_sel_space);
				}
			} else if (data_dtype.getClass() == H5T_INTEGER) {
				if (data_dtype.getSize() <= 4) {
					std::vector<int32_t> int_data(nnz);
					data_ds.read(int_data.data(), H5::PredType::NATIVE_INT32, data_mem_space, data_sel_space);
					for (size_t i = 0; i < int_data.size(); i++) {
						row_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					data_ds.read(int_data.data(), H5::PredType::NATIVE_INT64, data_mem_space, data_sel_space);
					for (size_t i = 0; i < int_data.size(); i++) {
						row_data[i] = static_cast<double>(int_data[i]);
					}
				}
			}

			// Add to sparse data structure only values in the requested var range
			for (size_t i = 0; i < col_indices.size(); i++) {
				int32_t col = col_indices[i];
				// Check if this column is in the requested var range
				if (col >= var_start && col < var_start + var_count) {
					sparse_data.row_indices.push_back(obs_idx);
					sparse_data.col_indices.push_back(col - var_start); // Adjust to local column index
					sparse_data.values.push_back(row_data[i]);
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return empty sparse data on error
	}

	return sparse_data;
}

H5Reader::SparseMatrixData H5Reader::ReadSparseXMatrixCSC(idx_t obs_start, idx_t obs_count, idx_t var_start,
                                                          idx_t var_count) {
	// Use the generic CSC reader for X matrix
	return ReadSparseMatrixCSC("/X", obs_start, obs_count, var_start, var_count);
}

H5Reader::SparseMatrixData H5Reader::ReadSparseMatrixCSC(const std::string &path, idx_t obs_start, idx_t obs_count,
                                                         idx_t var_start, idx_t var_count) {
	SparseMatrixData sparse_data;

	try {
		// Read CSC components from the specified path
		H5::DataSet data_ds = file->openDataSet(path + "/data");
		H5::DataSet indices_ds = file->openDataSet(path + "/indices");
		H5::DataSet indptr_ds = file->openDataSet(path + "/indptr");

		// Get total number of variables (columns in CSC)
		H5::DataSpace indptr_space = indptr_ds.getSpace();
		hsize_t indptr_dims[1];
		indptr_space.getSimpleExtentDims(indptr_dims);
		size_t total_var = indptr_dims[0] - 1; // indptr has n_var + 1 elements for CSC

		// For CSC, we need to read column-wise and collect values for requested rows
		// We'll iterate through the requested columns and extract values for requested rows
		for (idx_t var_idx = var_start; var_idx < var_start + var_count && var_idx < total_var; var_idx++) {
			// Read indptr for this column and the next to get the range
			std::vector<int64_t> col_indptr(2);
			hsize_t indptr_offset[1] = {static_cast<hsize_t>(var_idx)};
			hsize_t indptr_count[1] = {2};
			H5::DataSpace indptr_sel_space = indptr_ds.getSpace();
			indptr_sel_space.selectHyperslab(H5S_SELECT_SET, indptr_count, indptr_offset);
			H5::DataSpace indptr_mem_space(1, indptr_count);

			// Read indptr based on its actual type
			H5::DataType indptr_dtype = indptr_ds.getDataType();
			if (indptr_dtype.getSize() <= 4) {
				std::vector<int32_t> indptr32(2);
				indptr_ds.read(indptr32.data(), H5::PredType::NATIVE_INT32, indptr_mem_space, indptr_sel_space);
				col_indptr[0] = indptr32[0];
				col_indptr[1] = indptr32[1];
			} else {
				indptr_ds.read(col_indptr.data(), H5::PredType::NATIVE_INT64, indptr_mem_space, indptr_sel_space);
			}

			int64_t col_start = col_indptr[0];
			int64_t col_end = col_indptr[1];
			int64_t nnz = col_end - col_start;

			if (nnz == 0)
				continue;

			// Read row indices and data for this column
			std::vector<int32_t> row_indices(nnz);
			std::vector<double> col_data(nnz);

			// Read row indices
			hsize_t indices_offset[1] = {static_cast<hsize_t>(col_start)};
			hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
			H5::DataSpace indices_sel_space = indices_ds.getSpace();
			indices_sel_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
			H5::DataSpace indices_mem_space(1, indices_count);
			indices_ds.read(row_indices.data(), H5::PredType::NATIVE_INT32, indices_mem_space, indices_sel_space);

			// Read data values
			H5::DataSpace data_sel_space = data_ds.getSpace();
			data_sel_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
			H5::DataSpace data_mem_space(1, indices_count);

			H5::DataType data_dtype = data_ds.getDataType();
			if (data_dtype.getClass() == H5T_FLOAT) {
				if (data_dtype.getSize() <= 4) {
					std::vector<float> float_data(nnz);
					data_ds.read(float_data.data(), H5::PredType::NATIVE_FLOAT, data_mem_space, data_sel_space);
					for (size_t i = 0; i < float_data.size(); i++) {
						col_data[i] = static_cast<double>(float_data[i]);
					}
				} else {
					data_ds.read(col_data.data(), H5::PredType::NATIVE_DOUBLE, data_mem_space, data_sel_space);
				}
			} else if (data_dtype.getClass() == H5T_INTEGER) {
				if (data_dtype.getSize() <= 4) {
					std::vector<int32_t> int_data(nnz);
					data_ds.read(int_data.data(), H5::PredType::NATIVE_INT32, data_mem_space, data_sel_space);
					for (size_t i = 0; i < int_data.size(); i++) {
						col_data[i] = static_cast<double>(int_data[i]);
					}
				} else {
					std::vector<int64_t> int_data(nnz);
					data_ds.read(int_data.data(), H5::PredType::NATIVE_INT64, data_mem_space, data_sel_space);
					for (size_t i = 0; i < int_data.size(); i++) {
						col_data[i] = static_cast<double>(int_data[i]);
					}
				}
			}

			// Add to sparse data structure only values in the requested row range
			for (size_t i = 0; i < row_indices.size(); i++) {
				int32_t row = row_indices[i];
				// Check if this row is in the requested obs range
				if (row >= obs_start && row < obs_start + obs_count) {
					sparse_data.row_indices.push_back(row - obs_start);     // Adjust to local row index
					sparse_data.col_indices.push_back(var_idx - var_start); // Adjust to local column index
					sparse_data.values.push_back(col_data[i]);
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return empty sparse data on error
	}

	return sparse_data;
}

std::vector<H5Reader::MatrixInfo> H5Reader::GetObsmMatrices() {
	std::vector<MatrixInfo> matrices;

	try {
		if (IsGroupPresent("/obsm")) {
			H5::Group obsm_group = file->openGroup("/obsm");

			// Get number of datasets in obsm group
			hsize_t num_datasets = obsm_group.getNumObjs();

			for (hsize_t i = 0; i < num_datasets; i++) {
				std::string matrix_name = obsm_group.getObjnameByIdx(i);

				// Skip if not a dataset
				H5G_obj_t obj_type = obsm_group.getObjTypeByIdx(i);
				if (obj_type != H5G_DATASET) {
					continue;
				}

				// Open dataset and get dimensions
				H5::DataSet dataset = obsm_group.openDataSet(matrix_name);
				H5::DataSpace dataspace = dataset.getSpace();

				int ndims = dataspace.getSimpleExtentNdims();
				if (ndims == 2) {
					hsize_t dims[2];
					dataspace.getSimpleExtentDims(dims);

					MatrixInfo info;
					info.name = matrix_name;
					info.rows = dims[0];
					info.cols = dims[1];
					info.dtype = H5TypeToDuckDBType(dataset.getDataType());

					matrices.push_back(info);
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return empty list on error
	}

	return matrices;
}

std::vector<H5Reader::MatrixInfo> H5Reader::GetVarmMatrices() {
	std::vector<MatrixInfo> matrices;

	try {
		if (IsGroupPresent("/varm")) {
			H5::Group varm_group = file->openGroup("/varm");

			// Get number of datasets in varm group
			hsize_t num_datasets = varm_group.getNumObjs();

			for (hsize_t i = 0; i < num_datasets; i++) {
				std::string matrix_name = varm_group.getObjnameByIdx(i);

				// Skip if not a dataset
				H5G_obj_t obj_type = varm_group.getObjTypeByIdx(i);
				if (obj_type != H5G_DATASET) {
					continue;
				}

				// Open dataset and get dimensions
				H5::DataSet dataset = varm_group.openDataSet(matrix_name);
				H5::DataSpace dataspace = dataset.getSpace();

				int ndims = dataspace.getSimpleExtentNdims();
				if (ndims == 2) {
					hsize_t dims[2];
					dataspace.getSimpleExtentDims(dims);

					MatrixInfo info;
					info.name = matrix_name;
					info.rows = dims[0];
					info.cols = dims[1];
					info.dtype = H5TypeToDuckDBType(dataset.getDataType());

					matrices.push_back(info);
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return empty list on error
	}

	return matrices;
}

void H5Reader::ReadObsmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx,
                              Vector &result) {
	try {
		std::string dataset_path = "/obsm/" + matrix_name;
		H5::DataSet dataset = file->openDataSet(dataset_path);
		H5::DataSpace dataspace = dataset.getSpace();

		// Get dimensions
		hsize_t dims[2];
		dataspace.getSimpleExtentDims(dims);

		// Ensure col_idx is valid
		if (col_idx >= dims[1]) {
			throw InvalidInputException("Column index out of bounds for obsm matrix %s", matrix_name.c_str());
		}

		// Select hyperslab for the requested column
		hsize_t offset[2] = {static_cast<hsize_t>(row_start), static_cast<hsize_t>(col_idx)};
		hsize_t count[2] = {static_cast<hsize_t>(row_count), 1};
		dataspace.selectHyperslab(H5S_SELECT_SET, count, offset);

		// Create memory dataspace
		hsize_t mem_dims[1] = {static_cast<hsize_t>(row_count)};
		H5::DataSpace memspace(1, mem_dims);

		// Read data based on type
		H5::DataType dtype = dataset.getDataType();
		if (dtype.getClass() == H5T_FLOAT) {
			if (dtype.getSize() == 4) {
				std::vector<float> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::FLOAT(values[i]));
				}
			} else {
				std::vector<double> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_DOUBLE, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::DOUBLE(values[i]));
				}
			}
		} else if (dtype.getClass() == H5T_INTEGER) {
			if (dtype.getSize() <= 4) {
				std::vector<int32_t> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_INT32, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::INTEGER(values[i]));
				}
			} else {
				std::vector<int64_t> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_INT64, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::BIGINT(values[i]));
				}
			}
		}
	} catch (const H5::Exception &e) {
		throw InvalidInputException("Failed to read obsm matrix %s: %s", matrix_name.c_str(), e.getCDetailMsg());
	}
}

void H5Reader::ReadVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx,
                              Vector &result) {
	try {
		std::string dataset_path = "/varm/" + matrix_name;
		H5::DataSet dataset = file->openDataSet(dataset_path);
		H5::DataSpace dataspace = dataset.getSpace();

		// Get dimensions
		hsize_t dims[2];
		dataspace.getSimpleExtentDims(dims);

		// Ensure col_idx is valid
		if (col_idx >= dims[1]) {
			throw InvalidInputException("Column index out of bounds for varm matrix %s", matrix_name.c_str());
		}

		// Select hyperslab for the requested column
		hsize_t offset[2] = {static_cast<hsize_t>(row_start), static_cast<hsize_t>(col_idx)};
		hsize_t count[2] = {static_cast<hsize_t>(row_count), 1};
		dataspace.selectHyperslab(H5S_SELECT_SET, count, offset);

		// Create memory dataspace
		hsize_t mem_dims[1] = {static_cast<hsize_t>(row_count)};
		H5::DataSpace memspace(1, mem_dims);

		// Read data based on type
		H5::DataType dtype = dataset.getDataType();
		if (dtype.getClass() == H5T_FLOAT) {
			if (dtype.getSize() == 4) {
				std::vector<float> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::FLOAT(values[i]));
				}
			} else {
				std::vector<double> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_DOUBLE, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::DOUBLE(values[i]));
				}
			}
		} else if (dtype.getClass() == H5T_INTEGER) {
			if (dtype.getSize() <= 4) {
				std::vector<int32_t> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_INT32, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::INTEGER(values[i]));
				}
			} else {
				std::vector<int64_t> values(row_count);
				dataset.read(values.data(), H5::PredType::NATIVE_INT64, memspace, dataspace);
				for (idx_t i = 0; i < row_count; i++) {
					result.SetValue(i, Value::BIGINT(values[i]));
				}
			}
		}
	} catch (const H5::Exception &e) {
		throw InvalidInputException("Failed to read varm matrix %s: %s", matrix_name.c_str(), e.getCDetailMsg());
	}
}

std::vector<H5Reader::LayerInfo> H5Reader::GetLayers() {
	std::vector<LayerInfo> layers;

	if (!IsGroupPresent("/layers")) {
		return layers; // No layers in this file
	}

	try {
		H5::Group layers_group = file->openGroup("/layers");

		// Iterate through all items in the layers group
		hsize_t num_objs = layers_group.getNumObjs();
		for (hsize_t i = 0; i < num_objs; i++) {
			std::string layer_name = layers_group.getObjnameByIdx(i);
			LayerInfo info;
			info.name = layer_name;

			// Check if it's a dataset (dense) or group (sparse)
			H5G_stat_t stat_buf;
			layers_group.getObjinfo(layer_name, stat_buf);

			if (stat_buf.type == H5G_DATASET) {
				// Dense layer
				H5::DataSet dataset = layers_group.openDataSet(layer_name);
				H5::DataSpace dataspace = dataset.getSpace();

				hsize_t dims[2];
				dataspace.getSimpleExtentDims(dims);
				info.rows = dims[0];
				info.cols = dims[1];
				info.dtype = H5TypeToDuckDBType(dataset.getDataType());
				info.is_sparse = false;
				info.sparse_format = "";
			} else if (stat_buf.type == H5G_GROUP) {
				// Sparse layer
				H5::Group sparse_group = layers_group.openGroup(layer_name);
				info.is_sparse = true;

				// Determine sparse format (CSR or CSC)
				if (IsDatasetPresent("/layers/" + layer_name, "indptr")) {
					H5::DataSet indptr = sparse_group.openDataSet("indptr");
					H5::DataSpace indptr_space = indptr.getSpace();
					hsize_t indptr_dims[1];
					indptr_space.getSimpleExtentDims(indptr_dims);

					// For CSR: indptr has n_obs + 1 elements
					// For CSC: indptr has n_var + 1 elements
					// We need to check against the main X matrix dimensions
					idx_t n_obs = GetObsCount();
					idx_t n_var = GetVarCount();

					if (indptr_dims[0] == static_cast<hsize_t>(n_obs + 1)) {
						info.sparse_format = "csr";
						info.rows = n_obs;
						info.cols = n_var;
					} else if (indptr_dims[0] == static_cast<hsize_t>(n_var + 1)) {
						info.sparse_format = "csc";
						info.rows = n_obs;
						info.cols = n_var;
					}
				}

				// Get data type
				if (IsDatasetPresent("/layers/" + layer_name, "data")) {
					H5::DataSet data_dataset = sparse_group.openDataSet("data");
					info.dtype = H5TypeToDuckDBType(data_dataset.getDataType());
				}
			}

			layers.push_back(info);
		}
	} catch (const H5::Exception &e) {
		throw IOException("Failed to read layers: %s", e.getCDetailMsg());
	}

	return layers;
}

void H5Reader::ReadLayerMatrix(const std::string &layer_name, idx_t row_idx, idx_t start_col, idx_t count,
                               DataChunk &output, const std::vector<std::string> &var_names) {
	try {
		std::string layer_path = "/layers/" + layer_name;

		// Check if it's a sparse or dense layer
		H5::Group layers_group = file->openGroup("/layers");
		H5G_stat_t stat_buf;
		layers_group.getObjinfo(layer_name, stat_buf);

		if (stat_buf.type == H5G_DATASET) {
			// Dense layer - read directly like the main X matrix
			H5::DataSet dataset = layers_group.openDataSet(layer_name);
			H5::DataSpace dataspace = dataset.getSpace();

			hsize_t dims[2];
			dataspace.getSimpleExtentDims(dims);

			// Set up hyperslab for this row and column range
			hsize_t offset[2] = {row_idx, start_col};
			hsize_t count_dims[2] = {1, count};
			dataspace.selectHyperslab(H5S_SELECT_SET, count_dims, offset);

			// Create memory dataspace
			H5::DataSpace memspace(2, count_dims);

			// Read the data based on type
			H5::DataType dtype = dataset.getDataType();

			// First column is always obs_idx
			auto &obs_idx_vec = output.data[0];
			obs_idx_vec.SetValue(0, Value::BIGINT(row_idx));

			// Read gene expression values into remaining columns
			if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 4) {
				std::vector<float> buffer(count);
				dataset.read(buffer.data(), H5::PredType::NATIVE_FLOAT, memspace, dataspace);
				for (idx_t i = 0; i < count && i < var_names.size(); i++) {
					output.data[i + 1].SetValue(0, Value::FLOAT(buffer[i]));
				}
			} else if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 8) {
				std::vector<double> buffer(count);
				dataset.read(buffer.data(), H5::PredType::NATIVE_DOUBLE, memspace, dataspace);
				for (idx_t i = 0; i < count && i < var_names.size(); i++) {
					output.data[i + 1].SetValue(0, Value::DOUBLE(buffer[i]));
				}
			} else if (dtype.getClass() == H5T_INTEGER) {
				if (dtype.getSize() == 4) {
					std::vector<int32_t> buffer(count);
					dataset.read(buffer.data(), H5::PredType::NATIVE_INT32, memspace, dataspace);
					for (idx_t i = 0; i < count && i < var_names.size(); i++) {
						output.data[i + 1].SetValue(0, Value::INTEGER(buffer[i]));
					}
				} else if (dtype.getSize() == 8) {
					std::vector<int64_t> buffer(count);
					dataset.read(buffer.data(), H5::PredType::NATIVE_INT64, memspace, dataspace);
					for (idx_t i = 0; i < count && i < var_names.size(); i++) {
						output.data[i + 1].SetValue(0, Value::BIGINT(buffer[i]));
					}
				}
			}
		} else if (stat_buf.type == H5G_GROUP) {
			// Sparse layer - read using sparse format
			H5::Group sparse_group = layers_group.openGroup(layer_name);

			// Determine if CSR or CSC
			H5::DataSet indptr = sparse_group.openDataSet("indptr");
			H5::DataSpace indptr_space = indptr.getSpace();
			hsize_t indptr_dims[1];
			indptr_space.getSimpleExtentDims(indptr_dims);

			idx_t n_obs = GetObsCount();
			idx_t n_var = GetVarCount();
			bool is_csr = (indptr_dims[0] == static_cast<hsize_t>(n_obs + 1));

			// First column is always obs_idx
			auto &obs_idx_vec = output.data[0];
			obs_idx_vec.SetValue(0, Value::BIGINT(row_idx));

			// Initialize all gene columns to 0
			for (idx_t i = 1; i <= var_names.size(); i++) {
				auto &vec = output.data[i];
				// Set appropriate zero based on column type
				if (output.data[i].GetType() == LogicalType::FLOAT) {
					vec.SetValue(0, Value::FLOAT(0.0f));
				} else if (output.data[i].GetType() == LogicalType::DOUBLE) {
					vec.SetValue(0, Value::DOUBLE(0.0));
				} else if (output.data[i].GetType() == LogicalType::INTEGER) {
					vec.SetValue(0, Value::INTEGER(0));
				} else if (output.data[i].GetType() == LogicalType::BIGINT) {
					vec.SetValue(0, Value::BIGINT(0));
				}
			}

			if (is_csr) {
				// CSR format - row-major, efficient for reading rows
				H5::DataSet indices = sparse_group.openDataSet("indices");
				H5::DataSet data_dataset = sparse_group.openDataSet("data");

				// Read indptr for this row
				std::vector<int32_t> indptr_data(2);
				hsize_t indptr_offset[1] = {row_idx};
				hsize_t indptr_count[1] = {2};
				H5::DataSpace indptr_memspace(1, indptr_count);
				indptr_space.selectHyperslab(H5S_SELECT_SET, indptr_count, indptr_offset);
				indptr.read(indptr_data.data(), H5::PredType::NATIVE_INT32, indptr_memspace, indptr_space);

				int32_t row_start = indptr_data[0];
				int32_t row_end = indptr_data[1];
				int32_t nnz = row_end - row_start;

				if (nnz > 0) {
					// Read indices and data for this row
					std::vector<int32_t> col_indices(nnz);
					H5::DataSpace indices_space = indices.getSpace();
					hsize_t indices_offset[1] = {static_cast<hsize_t>(row_start)};
					hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
					H5::DataSpace indices_memspace(1, indices_count);
					indices_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
					indices.read(col_indices.data(), H5::PredType::NATIVE_INT32, indices_memspace, indices_space);

					// Read data values based on type
					H5::DataType dtype = data_dataset.getDataType();
					H5::DataSpace data_space = data_dataset.getSpace();
					H5::DataSpace data_memspace(1, indices_count);
					data_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);

					if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 4) {
						std::vector<float> values(nnz);
						data_dataset.read(values.data(), H5::PredType::NATIVE_FLOAT, data_memspace, data_space);
						for (int32_t i = 0; i < nnz; i++) {
							int32_t col = col_indices[i];
							if (col >= start_col && col < start_col + static_cast<int32_t>(count) &&
							    col - start_col < static_cast<int32_t>(var_names.size())) {
								output.data[col - start_col + 1].SetValue(0, Value::FLOAT(values[i]));
							}
						}
					} else if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 8) {
						std::vector<double> values(nnz);
						data_dataset.read(values.data(), H5::PredType::NATIVE_DOUBLE, data_memspace, data_space);
						for (int32_t i = 0; i < nnz; i++) {
							int32_t col = col_indices[i];
							if (col >= start_col && col < start_col + static_cast<int32_t>(count) &&
							    col - start_col < static_cast<int32_t>(var_names.size())) {
								output.data[col - start_col + 1].SetValue(0, Value::DOUBLE(values[i]));
							}
						}
					} else if (dtype.getClass() == H5T_INTEGER) {
						if (dtype.getSize() == 4) {
							std::vector<int32_t> values(nnz);
							data_dataset.read(values.data(), H5::PredType::NATIVE_INT32, data_memspace, data_space);
							for (int32_t i = 0; i < nnz; i++) {
								int32_t col = col_indices[i];
								if (col >= start_col && col < start_col + static_cast<int32_t>(count) &&
								    col - start_col < static_cast<int32_t>(var_names.size())) {
									output.data[col - start_col + 1].SetValue(0, Value::INTEGER(values[i]));
								}
							}
						} else if (dtype.getSize() == 8) {
							std::vector<int64_t> values(nnz);
							data_dataset.read(values.data(), H5::PredType::NATIVE_INT64, data_memspace, data_space);
							for (int32_t i = 0; i < nnz; i++) {
								int32_t col = col_indices[i];
								if (col >= start_col && col < start_col + static_cast<int32_t>(count) &&
								    col - start_col < static_cast<int32_t>(var_names.size())) {
									output.data[col - start_col + 1].SetValue(0, Value::BIGINT(values[i]));
								}
							}
						}
					}
				}
			} else {
				// CSC format - column-major, less efficient for reading rows
				// We need to iterate through all columns and check if they have values for this row
				H5::DataSet indices = sparse_group.openDataSet("indices");
				H5::DataSet data_dataset = sparse_group.openDataSet("data");

				// Read all indptr to find columns with data
				std::vector<int32_t> indptr_data(n_var + 1);
				indptr.read(indptr_data.data(), H5::PredType::NATIVE_INT32);

				// For each column in our range
				for (idx_t col = start_col; col < start_col + count && col < n_var; col++) {
					int32_t col_start = indptr_data[col];
					int32_t col_end = indptr_data[col + 1];

					if (col_start < col_end) {
						// This column has data, check if our row is in it
						int32_t nnz = col_end - col_start;
						std::vector<int32_t> row_indices(nnz);

						H5::DataSpace indices_space = indices.getSpace();
						hsize_t indices_offset[1] = {static_cast<hsize_t>(col_start)};
						hsize_t indices_count[1] = {static_cast<hsize_t>(nnz)};
						H5::DataSpace indices_memspace(1, indices_count);
						indices_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
						indices.read(row_indices.data(), H5::PredType::NATIVE_INT32, indices_memspace, indices_space);

						// Check if our row is in this column
						for (int32_t i = 0; i < nnz; i++) {
							if (row_indices[i] == static_cast<int32_t>(row_idx)) {
								// Found our row, read the value
								H5::DataType dtype = data_dataset.getDataType();
								H5::DataSpace data_space = data_dataset.getSpace();
								hsize_t data_offset[1] = {static_cast<hsize_t>(col_start + i)};
								hsize_t data_count[1] = {1};
								H5::DataSpace data_memspace(1, data_count);
								data_space.selectHyperslab(H5S_SELECT_SET, data_count, data_offset);

								idx_t output_col = col - start_col + 1;
								if (output_col <= var_names.size()) {
									if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 4) {
										float value;
										data_dataset.read(&value, H5::PredType::NATIVE_FLOAT, data_memspace,
										                  data_space);
										output.data[output_col].SetValue(0, Value::FLOAT(value));
									} else if (dtype.getClass() == H5T_FLOAT && dtype.getSize() == 8) {
										double value;
										data_dataset.read(&value, H5::PredType::NATIVE_DOUBLE, data_memspace,
										                  data_space);
										output.data[output_col].SetValue(0, Value::DOUBLE(value));
									} else if (dtype.getClass() == H5T_INTEGER && dtype.getSize() == 4) {
										int32_t value;
										data_dataset.read(&value, H5::PredType::NATIVE_INT32, data_memspace,
										                  data_space);
										output.data[output_col].SetValue(0, Value::INTEGER(value));
									} else if (dtype.getClass() == H5T_INTEGER && dtype.getSize() == 8) {
										int64_t value;
										data_dataset.read(&value, H5::PredType::NATIVE_INT64, data_memspace,
										                  data_space);
										output.data[output_col].SetValue(0, Value::BIGINT(value));
									}
								}
								break; // Found our row in this column, no need to continue
							}
						}
					}
				}
			}
		}

		output.SetCardinality(1);
	} catch (const H5::Exception &e) {
		throw IOException("Failed to read layer %s: %s", layer_name.c_str(), e.getCDetailMsg());
	}
}

std::string H5Reader::ReadVarColumnString(const std::string &column_name, idx_t var_idx) {
	if (!file || !file->getId()) {
		return "";
	}

	try {
		// Try to read from var DataFrame
		if (IsGroupPresent("/var")) {
			H5::Group var_group = file->openGroup("/var");
			if (IsDatasetPresent("/var", column_name)) {
				H5::DataSet dataset = var_group.openDataSet(column_name);
				H5::DataSpace dataspace = dataset.getSpace();

				// Get dimensions
				hsize_t dims[1];
				dataspace.getSimpleExtentDims(dims);

				if (var_idx >= dims[0]) {
					return "";
				}

				H5::DataType dtype = dataset.getDataType();
				if (dtype.getClass() == H5T_STRING) {
					H5::StrType str_type = dataset.getStrType();

					// Select single element
					hsize_t offset[1] = {var_idx};
					hsize_t count[1] = {1};
					dataspace.selectHyperslab(H5S_SELECT_SET, count, offset);
					H5::DataSpace mem_space(1, count);

					if (str_type.isVariableStr()) {
						char *str_buffer;
						dataset.read(&str_buffer, str_type, mem_space, dataspace);
						std::string result = str_buffer ? str_buffer : "";
						dataset.vlenReclaim(&str_buffer, str_type, mem_space);
						return result;
					} else {
						size_t str_size = str_type.getSize();
						std::vector<char> buffer(str_size);
						dataset.read(buffer.data(), str_type, mem_space, dataspace);
						size_t len = strnlen(buffer.data(), str_size);
						std::string result(buffer.data(), len);
						// Trim whitespace
						result.erase(result.find_last_not_of(" \t\n\r\f\v") + 1);
						return result;
					}
				}
			}
		}
	} catch (const H5::Exception &e) {
		// Return empty string on error
	}

	return "";
}

void H5Reader::ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output) {
	ReadMatrixBatch("/X", row_start, row_count, col_start, col_count, output, false);
}

void H5Reader::ReadLayerMatrixBatch(const std::string &layer_name, idx_t row_start, idx_t row_count, idx_t col_start,
                                    idx_t col_count, DataChunk &output) {
	std::string layer_path = "/layers/" + layer_name;
	ReadMatrixBatch(layer_path, row_start, row_count, col_start, col_count, output, true);
}

// Helper function to set value in vector based on its type
void H5Reader::SetTypedValue(Vector &vec, idx_t row, double value) {
	switch (vec.GetType().id()) {
	case LogicalTypeId::FLOAT:
		vec.SetValue(row, Value::FLOAT(static_cast<float>(value)));
		break;
	case LogicalTypeId::DOUBLE:
		vec.SetValue(row, Value::DOUBLE(value));
		break;
	case LogicalTypeId::INTEGER:
		vec.SetValue(row, Value::INTEGER(static_cast<int32_t>(value)));
		break;
	case LogicalTypeId::BIGINT:
		vec.SetValue(row, Value::BIGINT(static_cast<int64_t>(value)));
		break;
	default:
		vec.SetValue(row, Value::DOUBLE(value));
		break;
	}
}

// Helper function to initialize vector with zeros based on its type
void H5Reader::InitializeZeros(Vector &vec, idx_t count) {
	switch (vec.GetType().id()) {
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
	default:
		for (idx_t i = 0; i < count; i++) {
			vec.SetValue(i, Value::DOUBLE(0.0));
		}
		break;
	}
}

// Unified matrix reading function
void H5Reader::ReadMatrixBatch(const std::string &path, idx_t row_start, idx_t row_count, idx_t col_start,
                               idx_t col_count, DataChunk &output, bool is_layer) {
	try {
		// First column is always obs_idx
		auto &obs_idx_vec = output.data[0];
		for (idx_t i = 0; i < row_count; i++) {
			obs_idx_vec.SetValue(i, Value::BIGINT(row_start + i));
		}

		// Initialize all data columns with zeros
		for (idx_t col = 1; col <= col_count && col < output.data.size(); col++) {
			InitializeZeros(output.data[col], row_count);
		}

		// Check if matrix is sparse or dense
		bool is_sparse = false;
		bool is_dense = false;

		if (is_layer) {
			// For layers, check the structure
			H5G_stat_t stat_buf;
			file->getObjinfo(path, stat_buf);
			is_dense = (stat_buf.type == H5G_DATASET);
			is_sparse = (stat_buf.type == H5G_GROUP);
		} else {
			// For X matrix
			is_dense = IsDatasetPresent("/", "X");
			is_sparse = !is_dense && IsGroupPresent("/X");
		}

		if (is_dense) {
			// Dense matrix reading
			std::vector<double> values;
			if (is_layer) {
				ReadDenseMatrix(path, row_start, row_count, col_start, col_count, values);
			} else {
				ReadDenseMatrix("/X", row_start, row_count, col_start, col_count, values);
			}

			// Fill columns from dense matrix
			for (idx_t col = 0; col < col_count && col + 1 < output.data.size(); col++) {
				auto &vec = output.data[col + 1]; // +1 to skip obs_idx

				for (idx_t row = 0; row < row_count; row++) {
					idx_t matrix_idx = row * col_count + col;
					if (matrix_idx < values.size()) {
						SetTypedValue(vec, row, values[matrix_idx]);
					}
				}
			}
		} else if (is_sparse) {
			// Sparse matrix reading
			auto sparse_data =
			    ReadSparseMatrixAtPath(is_layer ? path : "/X", row_start, row_count, col_start, col_count);

			// Fill in non-zero values
			for (size_t i = 0; i < sparse_data.values.size(); i++) {
				idx_t row = sparse_data.row_indices[i];
				idx_t col = sparse_data.col_indices[i];

				if (col + 1 < output.data.size() && row < row_count) {
					SetTypedValue(output.data[col + 1], row, sparse_data.values[i]);
				}
			}
		}

		output.SetCardinality(row_count);
	} catch (const H5::Exception &e) {
		throw IOException("Failed to read matrix at %s: %s", path.c_str(), e.getCDetailMsg());
	}
}

std::vector<H5Reader::UnsInfo> H5Reader::GetUnsKeys() {
	std::vector<UnsInfo> uns_keys;

	// Check if uns group exists
	if (!IsGroupPresent("/uns")) {
		return uns_keys;
	}

	try {
		H5::Group uns_group = file->openGroup("/uns");
		hsize_t num_objs = uns_group.getNumObjs();

		for (hsize_t i = 0; i < num_objs; i++) {
			std::string key_name = uns_group.getObjnameByIdx(i);
			H5G_stat_t stat_buf;
			uns_group.getObjinfo(key_name, stat_buf);

			UnsInfo info;
			info.key = key_name;

			if (stat_buf.type == H5G_DATASET) {
				// It's a dataset (scalar or array)
				H5::DataSet dataset = uns_group.openDataSet(key_name);
				H5::DataSpace dataspace = dataset.getSpace();
				H5::DataType datatype = dataset.getDataType();

				int rank = dataspace.getSimpleExtentNdims();

				if (rank == 0) {
					// Scalar value
					info.type = "scalar";
					info.dtype = H5TypeToDuckDBType(datatype);

					// Read scalar value if it's a string
					if (datatype.getClass() == H5T_STRING) {
						if (datatype.isVariableStr()) {
							char *str_val;
							dataset.read(&str_val, datatype);
							info.value_str = std::string(str_val);
							free(str_val);
						} else {
							size_t str_size = datatype.getSize();
							std::vector<char> buffer(str_size + 1, 0);
							dataset.read(buffer.data(), datatype);
							info.value_str = std::string(buffer.data());
						}
					}
				} else {
					// Array
					info.type = "array";
					info.dtype = H5TypeToDuckDBType(datatype);

					std::vector<hsize_t> dims(rank);
					dataspace.getSimpleExtentDims(dims.data());
					info.shape = dims;
				}
			} else if (stat_buf.type == H5G_GROUP) {
				// It's a group (nested dict or DataFrame)
				H5::Group subgroup = uns_group.openGroup(key_name);

				// Check if it looks like a DataFrame (has _index)
				bool has_index = false;
				try {
					subgroup.openDataSet("_index");
					has_index = true;
				} catch (...) {
				}

				if (has_index) {
					info.type = "dataframe";
				} else {
					info.type = "group";
				}
				info.dtype = LogicalType::VARCHAR; // Groups are returned as JSON strings
			}

			uns_keys.push_back(info);
		}
	} catch (const H5::Exception &e) {
		// Ignore errors and return what we have
	}

	return uns_keys;
}

Value H5Reader::ReadUnsScalar(const std::string &key) {
	if (!IsDatasetPresent("/uns", key)) {
		return Value(LogicalType::VARCHAR); // Return NULL
	}

	try {
		H5::DataSet dataset = file->openDataSet("/uns/" + key);
		H5::DataType datatype = dataset.getDataType();

		if (datatype.getClass() == H5T_STRING) {
			// String scalar
			if (datatype.isVariableStr()) {
				char *str_val;
				dataset.read(&str_val, datatype);
				std::string result(str_val);
				free(str_val);
				return Value(result);
			} else {
				size_t str_size = datatype.getSize();
				std::vector<char> buffer(str_size + 1, 0);
				dataset.read(buffer.data(), datatype);
				return Value(std::string(buffer.data()));
			}
		} else if (datatype.getClass() == H5T_INTEGER) {
			// Integer scalar
			if (datatype.getSize() == 8) {
				int64_t val;
				dataset.read(&val, H5::PredType::NATIVE_INT64);
				return Value::BIGINT(val);
			} else {
				int32_t val;
				dataset.read(&val, H5::PredType::NATIVE_INT32);
				return Value::INTEGER(val);
			}
		} else if (datatype.getClass() == H5T_FLOAT) {
			// Float scalar
			double val;
			dataset.read(&val, H5::PredType::NATIVE_DOUBLE);
			return Value::DOUBLE(val);
		} else if (datatype.getSize() == 1) {
			// Boolean scalar (stored as 1-byte)
			uint8_t val;
			dataset.read(&val, H5::PredType::NATIVE_UINT8);
			return Value::BOOLEAN(val != 0);
		}

		return Value(LogicalType::VARCHAR); // Unknown type
	} catch (const H5::Exception &e) {
		return Value(LogicalType::VARCHAR); // Return NULL on error
	}
}

void H5Reader::ReadUnsArray(const std::string &key, Vector &result, idx_t offset, idx_t count) {
	if (!IsDatasetPresent("/uns", key)) {
		return;
	}

	try {
		H5::DataSet dataset = file->openDataSet("/uns/" + key);
		H5::DataSpace dataspace = dataset.getSpace();
		H5::DataType datatype = dataset.getDataType();

		// Get array dimensions
		int rank = dataspace.getSimpleExtentNdims();
		std::vector<hsize_t> dims(rank);
		dataspace.getSimpleExtentDims(dims.data());

		// For now, we only support 1D arrays
		if (rank != 1) {
			throw InvalidInputException("Multi-dimensional arrays in uns are not yet supported");
		}

		hsize_t total_size = dims[0];
		hsize_t actual_count = MinValue<hsize_t>(count, total_size - offset);

		// Read the data based on type
		if (datatype.getClass() == H5T_FLOAT) {
			std::vector<double> buffer(actual_count);

			// Create memory and file dataspaces for hyperslab selection
			hsize_t mem_dims[1] = {actual_count};
			H5::DataSpace mem_space(1, mem_dims);

			hsize_t file_offset[1] = {offset};
			hsize_t file_count[1] = {actual_count};
			dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

			dataset.read(buffer.data(), H5::PredType::NATIVE_DOUBLE, mem_space, dataspace);

			for (idx_t i = 0; i < actual_count; i++) {
				result.SetValue(i, Value::DOUBLE(buffer[i]));
			}
		} else if (datatype.getClass() == H5T_INTEGER) {
			std::vector<int64_t> buffer(actual_count);

			hsize_t mem_dims[1] = {actual_count};
			H5::DataSpace mem_space(1, mem_dims);

			hsize_t file_offset[1] = {offset};
			hsize_t file_count[1] = {actual_count};
			dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

			dataset.read(buffer.data(), H5::PredType::NATIVE_INT64, mem_space, dataspace);

			for (idx_t i = 0; i < actual_count; i++) {
				result.SetValue(i, Value::BIGINT(buffer[i]));
			}
		} else if (datatype.getClass() == H5T_STRING) {
			// String array
			for (idx_t i = 0; i < actual_count; i++) {
				hsize_t file_offset[1] = {offset + i};
				hsize_t file_count[1] = {1};
				dataspace.selectHyperslab(H5S_SELECT_SET, file_count, file_offset);

				hsize_t mem_dims[1] = {1};
				H5::DataSpace mem_space(1, mem_dims);

				if (datatype.isVariableStr()) {
					char *str_val;
					dataset.read(&str_val, datatype, mem_space, dataspace);
					result.SetValue(i, Value(std::string(str_val)));
					free(str_val);
				} else {
					size_t str_size = datatype.getSize();
					std::vector<char> buffer(str_size + 1, 0);
					dataset.read(buffer.data(), datatype, mem_space, dataspace);
					result.SetValue(i, Value(std::string(buffer.data())));
				}
			}
		}
	} catch (const H5::Exception &e) {
		throw IOException("Failed to read uns array '%s': %s", key.c_str(), e.getCDetailMsg());
	}
}

std::vector<std::string> H5Reader::GetObspKeys() {
	std::vector<std::string> keys;

	// Check if obsp group exists
	if (!IsGroupPresent("/obsp")) {
		return keys;
	}

	try {
		H5::Group obsp_group = file->openGroup("/obsp");
		hsize_t num_objs = obsp_group.getNumObjs();

		for (hsize_t i = 0; i < num_objs; i++) {
			std::string key_name = obsp_group.getObjnameByIdx(i);
			H5G_stat_t stat_buf;
			obsp_group.getObjinfo(key_name, stat_buf);

			// Only include groups (sparse matrices are stored as groups)
			if (stat_buf.type == H5G_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const H5::Exception &e) {
		// Return what we have
	}

	return keys;
}

std::vector<std::string> H5Reader::GetVarpKeys() {
	std::vector<std::string> keys;

	// Check if varp group exists
	if (!IsGroupPresent("/varp")) {
		return keys;
	}

	try {
		H5::Group varp_group = file->openGroup("/varp");
		hsize_t num_objs = varp_group.getNumObjs();

		for (hsize_t i = 0; i < num_objs; i++) {
			std::string key_name = varp_group.getObjnameByIdx(i);
			H5G_stat_t stat_buf;
			varp_group.getObjinfo(key_name, stat_buf);

			// Only include groups (sparse matrices are stored as groups)
			if (stat_buf.type == H5G_GROUP) {
				keys.push_back(key_name);
			}
		}
	} catch (const H5::Exception &e) {
		// Return what we have
	}

	return keys;
}

H5Reader::SparseMatrixInfo H5Reader::GetObspMatrixInfo(const std::string &key) {
	SparseMatrixInfo info;
	info.nrows = GetObsCount();
	info.ncols = GetObsCount(); // obsp is obs x obs
	info.nnz = 0;
	info.format = "csr"; // default

	std::string matrix_path = "/obsp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}

	try {
		H5::Group matrix_group = file->openGroup(matrix_path);

		// Check for encoding attribute to determine format
		if (matrix_group.attrExists("encoding-type")) {
			H5::Attribute encoding_attr = matrix_group.openAttribute("encoding-type");
			H5::StrType str_type = encoding_attr.getStrType();
			std::string encoding;
			encoding_attr.read(str_type, encoding);

			if (encoding == "csr_matrix") {
				info.format = "csr";
			} else if (encoding == "csc_matrix") {
				info.format = "csc";
			}
		}

		// Read shape if available
		if (matrix_group.attrExists("shape")) {
			H5::Attribute shape_attr = matrix_group.openAttribute("shape");
			hsize_t shape[2];
			shape_attr.read(H5::PredType::NATIVE_HSIZE, shape);
			info.nrows = shape[0];
			info.ncols = shape[1];
		}

		// Get number of non-zero elements from data array
		if (IsDatasetPresent(matrix_path, "data")) {
			H5::DataSet data_dataset = matrix_group.openDataSet("data");
			H5::DataSpace data_space = data_dataset.getSpace();
			hsize_t dims[1];
			data_space.getSimpleExtentDims(dims);
			info.nnz = dims[0];
		}

	} catch (const H5::Exception &e) {
		throw IOException("Failed to get obsp matrix info for '%s': %s", key.c_str(), e.getCDetailMsg());
	}

	return info;
}

H5Reader::SparseMatrixInfo H5Reader::GetVarpMatrixInfo(const std::string &key) {
	SparseMatrixInfo info;
	info.nrows = GetVarCount();
	info.ncols = GetVarCount(); // varp is var x var
	info.nnz = 0;
	info.format = "csr"; // default

	std::string matrix_path = "/varp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}

	try {
		H5::Group matrix_group = file->openGroup(matrix_path);

		// Check for encoding attribute to determine format
		if (matrix_group.attrExists("encoding-type")) {
			H5::Attribute encoding_attr = matrix_group.openAttribute("encoding-type");
			H5::StrType str_type = encoding_attr.getStrType();
			std::string encoding;
			encoding_attr.read(str_type, encoding);

			if (encoding == "csr_matrix") {
				info.format = "csr";
			} else if (encoding == "csc_matrix") {
				info.format = "csc";
			}
		}

		// Read shape if available
		if (matrix_group.attrExists("shape")) {
			H5::Attribute shape_attr = matrix_group.openAttribute("shape");
			hsize_t shape[2];
			shape_attr.read(H5::PredType::NATIVE_HSIZE, shape);
			info.nrows = shape[0];
			info.ncols = shape[1];
		}

		// Get number of non-zero elements from data array
		if (IsDatasetPresent(matrix_path, "data")) {
			H5::DataSet data_dataset = matrix_group.openDataSet("data");
			H5::DataSpace data_space = data_dataset.getSpace();
			hsize_t dims[1];
			data_space.getSimpleExtentDims(dims);
			info.nnz = dims[0];
		}

	} catch (const H5::Exception &e) {
		throw IOException("Failed to get varp matrix info for '%s': %s", key.c_str(), e.getCDetailMsg());
	}

	return info;
}

void H5Reader::ReadObspMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result,
                              idx_t offset, idx_t count) {
	std::string matrix_path = "/obsp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("obsp matrix '%s' not found", key.c_str());
	}

	try {
		H5::Group matrix_group = file->openGroup(matrix_path);
		SparseMatrixInfo info = GetObspMatrixInfo(key);

		// Read the sparse matrix components
		// CSR format: data (values), indices (column indices), indptr (row pointers)
		// CSC format: data (values), indices (row indices), indptr (column pointers)

		H5::DataSet data_dataset = matrix_group.openDataSet("data");
		H5::DataSet indices_dataset = matrix_group.openDataSet("indices");
		H5::DataSet indptr_dataset = matrix_group.openDataSet("indptr");

		if (info.format == "csr") {
			// Read all indptr to find which rows contain our offset range
			H5::DataSpace indptr_space = indptr_dataset.getSpace();
			hsize_t indptr_dims[1];
			indptr_space.getSimpleExtentDims(indptr_dims);
			std::vector<int32_t> indptr(indptr_dims[0]);
			indptr_dataset.read(indptr.data(), H5::PredType::NATIVE_INT32);

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
						H5::DataSpace indices_mem_space(1, indices_count);
						H5::DataSpace indices_file_space = indices_dataset.getSpace();
						indices_file_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
						indices_dataset.read(&col_idx, H5::PredType::NATIVE_INT32, indices_mem_space,
						                     indices_file_space);

						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5::DataSpace data_mem_space(1, data_count);
						H5::DataSpace data_file_space = data_dataset.getSpace();
						data_file_space.selectHyperslab(H5S_SELECT_SET, data_count, data_offset);
						data_dataset.read(&val, H5::PredType::NATIVE_FLOAT, data_mem_space, data_file_space);

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
			throw NotImplementedException("CSC format for obsp not yet implemented");
		}

	} catch (const H5::Exception &e) {
		throw IOException("Failed to read obsp matrix '%s': %s", key.c_str(), e.getCDetailMsg());
	}
}

void H5Reader::ReadVarpMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result,
                              idx_t offset, idx_t count) {
	std::string matrix_path = "/varp/" + key;

	if (!IsGroupPresent(matrix_path)) {
		throw InvalidInputException("varp matrix '%s' not found", key.c_str());
	}

	// Similar implementation to ReadObspMatrix but for varp
	// Reuse most of the logic
	try {
		H5::Group matrix_group = file->openGroup(matrix_path);
		SparseMatrixInfo info = GetVarpMatrixInfo(key);

		H5::DataSet data_dataset = matrix_group.openDataSet("data");
		H5::DataSet indices_dataset = matrix_group.openDataSet("indices");
		H5::DataSet indptr_dataset = matrix_group.openDataSet("indptr");

		if (info.format == "csr") {
			// Read all indptr to find which rows contain our offset range
			H5::DataSpace indptr_space = indptr_dataset.getSpace();
			hsize_t indptr_dims[1];
			indptr_space.getSimpleExtentDims(indptr_dims);
			std::vector<int32_t> indptr(indptr_dims[0]);
			indptr_dataset.read(indptr.data(), H5::PredType::NATIVE_INT32);

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
						H5::DataSpace indices_mem_space(1, indices_count);
						H5::DataSpace indices_file_space = indices_dataset.getSpace();
						indices_file_space.selectHyperslab(H5S_SELECT_SET, indices_count, indices_offset);
						indices_dataset.read(&col_idx, H5::PredType::NATIVE_INT32, indices_mem_space,
						                     indices_file_space);

						// Read value
						float val;
						hsize_t data_offset[1] = {j};
						hsize_t data_count[1] = {1};
						H5::DataSpace data_mem_space(1, data_count);
						H5::DataSpace data_file_space = data_dataset.getSpace();
						data_file_space.selectHyperslab(H5S_SELECT_SET, data_count, data_offset);
						data_dataset.read(&val, H5::PredType::NATIVE_FLOAT, data_mem_space, data_file_space);

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
			throw NotImplementedException("CSC format for varp not yet implemented");
		}

	} catch (const H5::Exception &e) {
		throw IOException("Failed to read varp matrix '%s': %s", key.c_str(), e.getCDetailMsg());
	}
}

} // namespace duckdb
