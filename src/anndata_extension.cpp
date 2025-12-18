#include "include/anndata_extension.hpp"
#include "include/anndata_storage.hpp"
#include "anndata_version.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"

#include <iostream>

namespace duckdb {

// Scalar function that returns the version
static void AnndataVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value(ANNDATA_EXTENSION_VERSION));
}

// Scalar function that returns a hello world message
static void AnndataHelloFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("Hello from AnnData DuckDB Extension v" ANNDATA_EXTENSION_VERSION "!"));
}

// Forward declaration
void RegisterAnndataTableFunctions(ExtensionLoader &loader);

// Internal load function
static void LoadInternal(ExtensionLoader &loader) {
	// Get the database instance
	auto &db = loader.GetDatabaseInstance();

	// Register the version function
	auto version_fun = ScalarFunction("anndata_version", {}, LogicalType::VARCHAR, AnndataVersionFunction);
	version_fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	loader.RegisterFunction(version_fun);

	// Register the hello function
	auto hello_fun = ScalarFunction("anndata_hello", {}, LogicalType::VARCHAR, AnndataHelloFunction);
	hello_fun.null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING;
	loader.RegisterFunction(hello_fun);

	// Register the AnnData table functions
	RegisterAnndataTableFunctions(loader);

	// Register the AnnData storage extension for ATTACH support
	auto &config = DBConfig::GetConfig(db);
	config.storage_extensions["anndata"] = CreateAnndataStorageExtension();

	// Log that the extension is loaded
	std::cout << "AnnData DuckDB Extension loaded successfully!" << std::endl;
}

void AnndataExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string AnndataExtension::Name() {
	return "anndata";
}

std::string AnndataExtension::Version() const {
	return "0.2.0";
}

} // namespace duckdb

extern "C" {

// DuckDB 1.4.x uses the DUCKDB_CPP_EXTENSION_ENTRY macro for extension entry points
DUCKDB_CPP_EXTENSION_ENTRY(anndata, loader) {
	duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *anndata_version() {
	return ANNDATA_EXTENSION_VERSION;
}
}
