#pragma once

#include "duckdb.hpp"
#include <hdf5.h>
#include <H5Cpp.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace duckdb {

// Utility class for reading HDF5 AnnData files
class H5Reader {
public:
	H5Reader(const std::string &file_path);
	~H5Reader();

	// Check if file is valid AnnData format
	bool IsValidAnnData();

	// Get dimensions
	size_t GetObsCount();
	size_t GetVarCount();

	// Get column names and types for obs/var
	struct ColumnInfo {
		std::string name;
		LogicalType type;
		bool is_categorical = false;
		std::vector<std::string> categories;
	};

	std::vector<ColumnInfo> GetObsColumns();
	std::vector<ColumnInfo> GetVarColumns();

	// Read data
	void ReadObsColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count);
	void ReadVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count);

	// Read categorical data
	std::string GetCategoricalValue(const std::string &group_path, const std::string &column_name, idx_t index);

private:
	std::unique_ptr<H5::H5File> file;
	std::string file_path;

	// Helper methods
	LogicalType H5TypeToDuckDBType(const H5::DataType &h5_type);
	bool IsGroupPresent(const std::string &group_name);
	bool IsDatasetPresent(const std::string &group_name, const std::string &dataset_name);
	std::vector<std::string> GetGroupMembers(const std::string &group_name);

	// Cache for categorical mappings
	struct CategoricalCache {
		std::vector<std::string> categories;
		std::vector<int> codes;
	};
	std::unordered_map<std::string, CategoricalCache> categorical_cache;
};

} // namespace duckdb
