#include "include/h5_reader.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <cstring> // For strnlen

namespace duckdb {

H5Reader::H5Reader(const std::string &file_path) : file_path(file_path) {
	try {
		// Turn off HDF5 error printing
		H5::Exception::dontPrint();

		// Open the HDF5 file
		file = make_uniq<H5::H5File>(file_path, H5F_ACC_RDONLY);
	} catch (const H5::FileIException &e) {
		throw IOException("Failed to open HDF5 file: " + file_path);
	} catch (const H5::Exception &e) {
		throw IOException("HDF5 error opening file: " + file_path);
	}
}

H5Reader::~H5Reader() {
	if (file) {
		try {
			file->close();
		} catch (...) {
			// Ignore close errors in destructor
		}
	}
}

bool H5Reader::IsValidAnnData() {
	// Check for required groups
	return IsGroupPresent("/obs") && IsGroupPresent("/var") && (IsGroupPresent("/X") || IsDatasetPresent("/", "X"));
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

	// Add obs_idx as the first column (row index)
	ColumnInfo idx_col;
	idx_col.name = "obs_idx";
	idx_col.type = LogicalType::BIGINT;
	idx_col.is_categorical = false;
	columns.push_back(idx_col);

	try {
		auto members = GetGroupMembers("/obs");

		for (const auto &member : members) {
			if (member == "__categories" || member == "index" || member == "_index") {
				continue; // Skip metadata and index columns (we're using obs_idx instead)
			}

			ColumnInfo col;
			col.name = member;

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
		col1.type = LogicalType::VARCHAR;
		col1.is_categorical = false;
		columns.push_back(col1);

		ColumnInfo col2;
		col2.name = "cell_type";
		col2.type = LogicalType::VARCHAR;
		col2.is_categorical = false;
		columns.push_back(col2);

		ColumnInfo col3;
		col3.name = "n_genes";
		col3.type = LogicalType::INTEGER;
		col3.is_categorical = false;
		columns.push_back(col3);

		ColumnInfo col4;
		col4.name = "n_counts";
		col4.type = LogicalType::DOUBLE;
		col4.is_categorical = false;
		columns.push_back(col4);
	}

	return columns;
}

std::vector<H5Reader::ColumnInfo> H5Reader::GetVarColumns() {
	std::vector<ColumnInfo> columns;

	try {
		auto members = GetGroupMembers("/var");

		for (const auto &member : members) {
			if (member == "__categories" || member == "_index") {
				continue; // Skip metadata
			}

			ColumnInfo col;
			col.name = member;

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
		col1.type = LogicalType::VARCHAR;
		col1.is_categorical = false;
		columns.push_back(col1);

		ColumnInfo col2;
		col2.name = "gene_name";
		col2.type = LogicalType::VARCHAR;
		col2.is_categorical = false;
		columns.push_back(col2);

		ColumnInfo col3;
		col3.name = "highly_variable";
		col3.type = LogicalType::BOOLEAN;
		col3.is_categorical = false;
		columns.push_back(col3);

		ColumnInfo col4;
		col4.name = "mean_counts";
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
		file->openGroup(group_name);
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
		} else if (IsGroupPresent("/X")) {
			// Sparse matrix - not yet implemented
			info.is_sparse = true;
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

void H5Reader::ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count,
                           std::vector<double> &values) {
	values.clear();
	values.resize(obs_count * var_count, 0.0);

	try {
		if (IsDatasetPresent("/", "X")) {
			// Dense matrix
			H5::DataSet dataset = file->openDataSet("/X");
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
				std::vector<int32_t> int_values(obs_count * var_count);
				dataset.read(int_values.data(), H5::PredType::NATIVE_INT32, mem_space, dataspace);
				for (size_t i = 0; i < int_values.size(); i++) {
					values[i] = static_cast<double>(int_values[i]);
				}
			}
		} else if (IsGroupPresent("/X")) {
			// Sparse matrix in CSR format - convert to dense for backward compatibility
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
	SparseMatrixData sparse_data;

	try {
		// Check if it's CSR or CSC format
		bool is_csr = IsDatasetPresent("/X", "indptr");
		if (!is_csr) {
			// CSC format not yet supported - would need to transpose
			return sparse_data;
		}

		// Read CSR components
		H5::DataSet data_ds = file->openDataSet("/X/data");
		H5::DataSet indices_ds = file->openDataSet("/X/indices");
		H5::DataSet indptr_ds = file->openDataSet("/X/indptr");

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

} // namespace duckdb
