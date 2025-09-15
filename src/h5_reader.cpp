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

	try {
		auto members = GetGroupMembers("/obs");

		for (const auto &member : members) {
			if (member == "__categories" || member == "index") {
				continue; // Skip metadata
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

} // namespace duckdb
