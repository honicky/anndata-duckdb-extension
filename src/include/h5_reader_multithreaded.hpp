#pragma once

#include "duckdb.hpp"
#include "h5_handles.hpp"
#include "h5_file_cache.hpp"
#include <hdf5.h>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace duckdb {

// New H5Reader implementation using HDF5 C API with thread-safe support
class H5ReaderMultithreaded {
public:
	// Constructor with optional remote config for S3/HTTP access
	H5ReaderMultithreaded(const std::string &file_path, const H5FileCache::RemoteConfig *remote_config = nullptr);
	~H5ReaderMultithreaded();

	// Delete copy constructor and copy assignment to make class move-only
	H5ReaderMultithreaded(const H5ReaderMultithreaded &) = delete;
	H5ReaderMultithreaded &operator=(const H5ReaderMultithreaded &) = delete;

	// Allow move operations (default implementation is fine)
	H5ReaderMultithreaded(H5ReaderMultithreaded &&) = default;
	H5ReaderMultithreaded &operator=(H5ReaderMultithreaded &&) = default;

	// Check if file is valid AnnData format (at least obs or var must exist)
	bool IsValidAnnData();

	// Check which core components are present
	bool HasObs();
	bool HasVar();
	bool HasX();
	bool HasGroup(const std::string &group_name);

	// Get dimensions
	size_t GetObsCount();
	size_t GetVarCount();

	// Get column names and types for obs/var
	struct ColumnInfo {
		std::string name;          // Display name (may be mangled for duplicates)
		std::string original_name; // Original HDF5 dataset name
		LogicalType type;
		bool is_categorical = false;
		std::vector<std::string> categories;
	};

	// original_name of the synthetic row-index columns (displayed as obs_idx / var_idx). A leading '/'
	// can never occur in an HDF5 link name, so a user column that is literally called obs_idx
	// (displayed as obs_idx_) is never mistaken for the index when a column is read by original name.
	static constexpr const char *OBS_INDEX_ORIGINAL_NAME = "/obs_idx";
	static constexpr const char *VAR_INDEX_ORIGINAL_NAME = "/var_idx";

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
		size_t n_obs = 0;
		size_t n_var = 0;
		bool is_sparse = false;
		std::string sparse_format; // "csr" or "csc" if sparse
		LogicalType dtype = LogicalType::DOUBLE;
	};
	XMatrixInfo GetXMatrixInfo();

	// Read gene names from var
	std::vector<std::string> GetVarNames(const std::string &column_name = "_index");

	// Auto-detect var columns for gene names and IDs
	struct VarColumnDetection {
		std::string name_column; // Column for gene symbols (e.g., "gene_symbols")
		std::string id_column;   // Column for gene IDs (e.g., "ensembl_id")
	};
	VarColumnDetection DetectVarColumns();

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

	// Raw section support (/raw group in h5ad files)
	bool HasRawData();
	XMatrixInfo GetRawXMatrixInfo();
	size_t GetRawVarCount();
	std::vector<ColumnInfo> GetRawVarColumns();
	std::vector<std::string> GetRawVarNames(const std::string &column_name = "_index");
	void ReadRawVarColumn(const std::string &column_name, Vector &result, idx_t offset, idx_t count);
	VarColumnDetection DetectRawVarColumns();
	std::vector<MatrixInfo> GetRawVarmMatrices();
	void ReadRawVarmMatrix(const std::string &matrix_name, idx_t row_start, idx_t row_count, idx_t col_idx,
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

	// Projection pushdown: Read only specific columns from a matrix
	// column_ids maps output column indices to matrix column indices
	// For example, if user requests columns [0, 5, 10], output.data[0] gets obs_idx,
	// output.data[1] gets matrix column 5, output.data[2] gets matrix column 10
	void ReadMatrixColumns(const std::string &path, idx_t row_start, idx_t row_count,
	                       const std::vector<idx_t> &matrix_col_indices, DataChunk &output, bool is_layer = false);

	// Uns (unstructured) data information structure
	struct UnsInfo {
		std::string key;
		std::string type; // "scalar", "array", "group", "dataframe"
		LogicalType dtype;
		std::vector<hsize_t> shape;
		std::string value_str;                 // For scalar values (as string)
		std::vector<std::string> array_values; // For array values (as strings)
	};

	// Get list of uns keys. With read_array_values = false, array entries carry key/type/dtype/shape
	// only and their values are left to ReadUnsArrayAsStrings, which keeps discovery cheap.
	std::vector<UnsInfo> GetUnsKeys(bool read_array_values = true);

	// Read every element of a uns array dataset as strings (same formatting as GetUnsKeys)
	std::vector<std::string> ReadUnsArrayAsStrings(const std::string &key);

	// Read uns scalar value
	Value ReadUnsScalar(const std::string &key);

	// Read uns array
	void ReadUnsArray(const std::string &key, Vector &result, idx_t offset, idx_t count);

	// Get list of obsp/varp matrix names
	std::vector<std::string> GetObspKeys();
	std::vector<std::string> GetVarpKeys();

	// Sparse matrix info structure
	struct SparseMatrixInfo {
		std::string format; // "csr" or "csc"
		idx_t nrows;
		idx_t ncols;
		idx_t nnz; // number of non-zero elements
	};

	// Get info about a sparse matrix
	SparseMatrixInfo GetObspMatrixInfo(const std::string &key);
	SparseMatrixInfo GetVarpMatrixInfo(const std::string &key);

	// Read sparse matrix triplets (row, col, value)
	void ReadObspMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result,
	                    idx_t offset, idx_t count);
	void ReadVarpMatrix(const std::string &key, Vector &row_result, Vector &col_result, Vector &value_result,
	                    idx_t offset, idx_t count);

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

	// File handle using shared cache
	std::shared_ptr<hid_t> file_handle;
	std::string file_path;

	// Helper methods
	static LogicalType H5TypeToDuckDBType(hid_t h5_type);
	bool IsGroupPresent(const std::string &group_name);
	bool IsDatasetPresent(const std::string &group_name, const std::string &dataset_name);
	std::vector<std::string> GetGroupMembers(const std::string &group_name);

	// Check if a path is a compound dataset (older AnnData format)
	bool IsCompoundDataset(const std::string &path);

	// Get column info from a compound dataset
	std::vector<ColumnInfo> GetCompoundDatasetColumns(const std::string &path, const std::string &idx_col_name);

	// Read a column from a compound dataset
	void ReadCompoundDatasetColumn(const std::string &path, const std::string &column_name, Vector &result,
	                               idx_t offset, idx_t count);

	// Cache for categorical mappings. categories[i] is NULL when is_null[i] is set (a category stored
	// in a nullable-string-array group with its mask bit on).
	struct CategoricalCache {
		std::vector<std::string> categories;
		std::vector<bool> is_null;
	};

	// Get cached categories for a categorical column (reads from HDF5 only once). The categories
	// member may be a plain dataset or, for pandas string columns written with
	// allow_write_nullable_strings, a nullable-string-array group (values + mask).
	const CategoricalCache &GetCachedCategories(const std::string &group_path);

	// How a group-valued obs/var column is encoded: AnnData's "encoding-type" attribute, falling back
	// to the datasets present (categories+codes, or values+mask). Cached per group path.
	enum class ColumnGroupEncoding { CATEGORICAL, NULLABLE, UNKNOWN };
	ColumnGroupEncoding GetColumnGroupEncoding(const std::string &group_path);
	std::unordered_map<std::string, ColumnGroupEncoding> group_encoding_cache;

	// Read a string attribute of an object; "" when absent or not a string
	std::string ReadStringAttribute(hid_t obj_id, const std::string &attr_name);

	// Fill col.type / col.is_categorical for a group-valued obs/var column; a malformed group binds as
	// VARCHAR (and reads as NULL) instead of aborting discovery
	void SetGroupColumnType(const std::string &group_path, ColumnInfo &col);
	void SetGroupColumnTypeUnchecked(const std::string &group_path, ColumnInfo &col);

	// Read one column of a new-format (group of datasets) obs/var frame at frame_path
	void ReadDataFrameColumn(const std::string &frame_path, const std::string &column_name, Vector &result,
	                         idx_t offset, idx_t count);

	// Read a categorical group column (codes mapped through the cached categories)
	void ReadCategoricalColumn(const std::string &group_path, Vector &result, idx_t offset, idx_t count);

	// Read a nullable (values + mask) group column: values first, then NULL wherever mask is set
	void ReadNullableColumn(const std::string &group_path, Vector &result, idx_t offset, idx_t count);

	// Read rows [offset, offset + count) of a 1-D dataset into a DuckDB vector. Integers and floats are
	// written through Vector::SetValue, so they cast to whatever type the vector was bound (or
	// harmonized) to. HDF5 enums (h5py booleans) fill a BOOLEAN vector directly, become "True"/"False"
	// in a VARCHAR vector, and are cast for anything else.
	void ReadDatasetRange(const std::string &dataset_path, Vector &result, idx_t offset, idx_t count);

	// Path-parameterized helpers for var/varm (shared between main and raw)
	size_t GetVarCountAtPath(const std::string &var_path, const std::string &x_path);
	std::vector<ColumnInfo> GetVarColumnsAtPath(const std::string &var_path, const std::string &idx_col_name);
	void ReadVarColumnAtPath(const std::string &var_path, const std::string &column_name, Vector &result, idx_t offset,
	                         idx_t count);
	std::string ReadVarColumnStringAtPath(const std::string &var_path, const std::string &column_name, idx_t index);
	std::vector<std::string> GetVarNamesAtPath(const std::string &var_path, const std::string &column_name,
	                                           size_t var_count);
	VarColumnDetection DetectVarColumnsAtPath(const std::string &var_path, const std::string &x_path);
	std::vector<MatrixInfo> GetVarmMatricesAtPath(const std::string &varm_path);
	void ReadVarmMatrixAtPath(const std::string &varm_path, const std::string &matrix_name, idx_t row_start,
	                          idx_t row_count, idx_t col_idx, Vector &result);

	std::unordered_map<std::string, CategoricalCache> categorical_cache;
};

} // namespace duckdb
