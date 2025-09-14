#define DUCKDB_EXTENSION_MAIN

#include "include/anndata_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"

#include <iostream>

namespace duckdb {

// Scalar function that returns the version
static void AnndataVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("0.1.0"));
}

// Scalar function that returns a hello world message
static void AnndataHelloFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("Hello from AnnData DuckDB Extension v2!"));
}

// Forward declaration
void RegisterAnndataTableFunctions(DatabaseInstance &db);

void AnndataExtension::Load(DuckDB &db) {
	// Register the version function
	auto version_fun = ScalarFunction("anndata_version", {}, LogicalType::VARCHAR, AnndataVersionFunction);
	version_fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	ExtensionUtil::RegisterFunction(*db.instance, version_fun);

	// Register the hello function
	auto hello_fun = ScalarFunction("anndata_hello", {}, LogicalType::VARCHAR, AnndataHelloFunction);
	hello_fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	ExtensionUtil::RegisterFunction(*db.instance, hello_fun);

	// Register the AnnData table functions
	RegisterAnndataTableFunctions(*db.instance);

	// Log that the extension is loaded
	std::cout << "AnnData DuckDB Extension loaded successfully!" << std::endl;
}

std::string AnndataExtension::Name() {
	return "anndata";
}

std::string AnndataExtension::Version() const {
	return "0.1.0";
}

} // namespace duckdb

// Export the version function for compatibility
extern "C" {

DUCKDB_EXTENSION_API void anndata_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::AnndataExtension ext;
	ext.Load(db_wrapper);
}

DUCKDB_EXTENSION_API const char *anndata_version() {
	return "0.1.0";
}
}
