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

	// Read a single string value from var column (for getting gene names)
	std::string ReadVarColumnString(const std::string &column_name, idx_t index);

	// Read categorical data
	std::string GetCategoricalValue(const std::string &group_path, const std::string &column_name, idx_t index);

	// Read X matrix
	struct XMatrixInfo {
		size_t n_obs;
		size_t n_var;
		bool is_sparse = false;
		std::string sparse_format; // "csr" or "csc" if sparse
		LogicalType dtype = LogicalType::DOUBLE;
	};
	XMatrixInfo GetXMatrixInfo();

	// Read gene names from var
	std::vector<std::string> GetVarNames(const std::string &column_name = "_index");

	// Read X matrix values for a row range and column range (legacy - returns vector)
	void ReadXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count, std::vector<double> &values);

	// Read X matrix directly into DataChunk for better performance
	void ReadXMatrixBatch(idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count, DataChunk &output);

	// Sparse matrix data structure
	struct SparseMatrixData {
		std::vector<idx_t> row_indices;
		std::vector<idx_t> col_indices;
		std::vector<double> values;
	};

	// Read sparse X matrix - automatically detects format and dispatches to appropriate reader
	SparseMatrixData ReadSparseXMatrix(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count);

	// Read CSR format sparse matrix
	SparseMatrixData ReadSparseXMatrixCSR(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count);

	// Read CSC format sparse matrix
	SparseMatrixData ReadSparseXMatrixCSC(idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count);

	// Get obsm/varm matrix info
	struct MatrixInfo {
		std::string name;
		size_t rows;
		size_t cols;
		LogicalType dtype;
	};

	// Get list of obsm matrices
	std::vector<MatrixInfo> GetObsmMatrices();

	// Get list of varm matrices
	std::vector<MatrixInfo> GetVarmMatrices();

	// Read obsm matrix
	void ReadObsmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx,
	                    Vector &result);

	// Read varm matrix
	void ReadVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx,
	                    Vector &result);

	// Layer information structure
	struct LayerInfo {
		std::string name;
		size_t rows;
		size_t cols;
		LogicalType dtype;
		bool is_sparse;
		std::string sparse_format; // "csr" or "csc" if sparse
	};

	// Get list of available layers
	std::vector<LayerInfo> GetLayers();

	// Read layer matrix data (similar to X matrix) - single row version (deprecated)
	void ReadLayerMatrix(const std::string &layer_name, idx_t row_idx, idx_t start_col, idx_t count, DataChunk &output,
	                     const std::vector<std::string> &var_names);

	// Read layer matrix data in batches for better performance
	void ReadLayerMatrixBatch(const std::string &layer_name, idx_t row_start, idx_t row_count, idx_t col_start,
	                          idx_t col_count, DataChunk &output);

	// Unified matrix reading interface - reads any matrix (X or layer) into a DataChunk
	void ReadMatrixBatch(const std::string &path, idx_t row_start, idx_t row_count, idx_t col_start, idx_t col_count,
	                     DataChunk &output, bool is_layer = false);

private:
	// Helper to set value in vector based on type
	static void SetTypedValue(Vector &vec, idx_t row, double value);

	// Helper to initialize vector with zeros based on type
	static void InitializeZeros(Vector &vec, idx_t count);
	// Helper function to read sparse matrix at any path
	SparseMatrixData ReadSparseMatrixAtPath(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start,
	                                        idx_t var_count);
	SparseMatrixData ReadSparseMatrixCSR(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start,
	                                     idx_t var_count);
	SparseMatrixData ReadSparseMatrixCSC(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start,
	                                     idx_t var_count);
	// Helper function to read dense matrix at any path
	void ReadDenseMatrix(const std::string &path, idx_t obs_start, idx_t obs_count, idx_t var_start, idx_t var_count,
	                     std::vector<double> &values);
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
