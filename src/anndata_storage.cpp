#include "include/anndata_storage.hpp"
#include "include/h5_reader_multithreaded.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// AnndataDefaultGenerator Implementation
//===--------------------------------------------------------------------===//

AnndataDefaultGenerator::AnndataDefaultGenerator(Catalog &catalog, SchemaCatalogEntry &schema, string file_path_p,
                                                 vector<TableViewInfo> tables_p)
    : DefaultGenerator(catalog), schema(schema), file_path(std::move(file_path_p)), tables(std::move(tables_p)) {
	// Build lookup map for fast access
	for (const auto &table : tables) {
		table_map[table.name] = table;
	}
}

string AnndataDefaultGenerator::GenerateViewSQL(const TableViewInfo &info) const {
	if (info.table_type == "obs") {
		return StringUtil::Format("SELECT * FROM anndata_scan_obs(%s)", SQLString(file_path));
	} else if (info.table_type == "var") {
		return StringUtil::Format("SELECT * FROM anndata_scan_var(%s)", SQLString(file_path));
	} else if (info.table_type == "X") {
		return StringUtil::Format("SELECT * FROM anndata_scan_x(%s)", SQLString(file_path));
	} else if (info.table_type == "obsm") {
		return StringUtil::Format("SELECT * FROM anndata_scan_obsm(%s, %s)", SQLString(file_path),
		                          SQLString(info.param));
	} else if (info.table_type == "varm") {
		return StringUtil::Format("SELECT * FROM anndata_scan_varm(%s, %s)", SQLString(file_path),
		                          SQLString(info.param));
	} else if (info.table_type == "layers") {
		return StringUtil::Format("SELECT * FROM anndata_scan_layers(%s, %s)", SQLString(file_path),
		                          SQLString(info.param));
	} else if (info.table_type == "obsp") {
		return StringUtil::Format("SELECT * FROM anndata_scan_obsp(%s, %s)", SQLString(file_path),
		                          SQLString(info.param));
	} else if (info.table_type == "varp") {
		return StringUtil::Format("SELECT * FROM anndata_scan_varp(%s, %s)", SQLString(file_path),
		                          SQLString(info.param));
	} else if (info.table_type == "uns") {
		return StringUtil::Format("SELECT * FROM anndata_scan_uns(%s)", SQLString(file_path));
	}

	throw InternalException("Unknown table type: " + info.table_type);
}

unique_ptr<CatalogEntry> AnndataDefaultGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	auto it = table_map.find(entry_name);
	if (it == table_map.end()) {
		// Check case-insensitively
		for (const auto &pair : table_map) {
			if (StringUtil::CIEquals(pair.first, entry_name)) {
				it = table_map.find(pair.first);
				break;
			}
		}
	}

	if (it == table_map.end()) {
		return nullptr;
	}

	const auto &info = it->second;

	auto result = make_uniq<CreateViewInfo>();
	result->schema = DEFAULT_SCHEMA;
	result->view_name = info.name;
	result->sql = GenerateViewSQL(info);

	auto view_info = CreateViewInfo::FromSelect(context, std::move(result));
	return make_uniq_base<CatalogEntry, ViewCatalogEntry>(catalog, schema, *view_info);
}

vector<string> AnndataDefaultGenerator::GetDefaultEntries() {
	vector<string> entries;
	entries.reserve(tables.size());
	for (const auto &table : tables) {
		entries.push_back(table.name);
	}
	return entries;
}

//===--------------------------------------------------------------------===//
// Table Discovery
//===--------------------------------------------------------------------===//

vector<TableViewInfo> DiscoverAnndataTables(const string &file_path) {
	vector<TableViewInfo> tables;

	H5ReaderMultithreaded reader(file_path);

	if (!reader.IsValidAnnData()) {
		throw IOException("File is not a valid AnnData file: " + file_path);
	}

	// Always add obs and var
	tables.push_back({"obs", "obs", ""});
	tables.push_back({"var", "var", ""});

	// Check for X matrix
	try {
		auto x_info = reader.GetXMatrixInfo();
		if (x_info.n_obs > 0 && x_info.n_var > 0) {
			tables.push_back({"X", "X", ""});
		}
	} catch (...) {
		// X matrix may not exist, that's ok
	}

	// Get obsm matrices
	try {
		auto obsm_matrices = reader.GetObsmMatrices();
		for (const auto &m : obsm_matrices) {
			tables.push_back({"obsm_" + m.name, "obsm", m.name});
		}
	} catch (...) {
		// obsm may not exist
	}

	// Get varm matrices
	try {
		auto varm_matrices = reader.GetVarmMatrices();
		for (const auto &m : varm_matrices) {
			tables.push_back({"varm_" + m.name, "varm", m.name});
		}
	} catch (...) {
		// varm may not exist
	}

	// Get layers
	try {
		auto layers = reader.GetLayers();
		for (const auto &l : layers) {
			tables.push_back({"layers_" + l.name, "layers", l.name});
		}
	} catch (...) {
		// layers may not exist
	}

	// Get obsp matrices
	try {
		auto obsp_keys = reader.GetObspKeys();
		for (const auto &k : obsp_keys) {
			tables.push_back({"obsp_" + k, "obsp", k});
		}
	} catch (...) {
		// obsp may not exist
	}

	// Get varp matrices
	try {
		auto varp_keys = reader.GetVarpKeys();
		for (const auto &k : varp_keys) {
			tables.push_back({"varp_" + k, "varp", k});
		}
	} catch (...) {
		// varp may not exist
	}

	// Check for uns data
	try {
		auto uns_keys = reader.GetUnsKeys();
		if (!uns_keys.empty()) {
			tables.push_back({"uns", "uns", ""});
		}
	} catch (...) {
		// uns may not exist
	}

	return tables;
}

//===--------------------------------------------------------------------===//
// Storage Extension Callbacks
//===--------------------------------------------------------------------===//

static unique_ptr<Catalog> AnndataStorageAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                                AttachedDatabase &db, const string &name, AttachInfo &info,
                                                AttachOptions &options) {
	// Get the file path
	auto file_path = info.path;

	// Verify file exists and is valid AnnData
	auto tables = DiscoverAnndataTables(file_path);

	// Create an in-memory catalog for the attached database
	info.path = ":memory:";
	auto catalog = make_uniq<DuckCatalog>(db);
	catalog->Initialize(false);

	// Set up the default view generator
	auto system_transaction = CatalogTransaction::GetSystemTransaction(db.GetDatabase());
	auto &schema = catalog->GetSchema(system_transaction, DEFAULT_SCHEMA);
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &catalog_set = duck_schema.GetCatalogSet(CatalogType::VIEW_ENTRY);

	auto default_generator =
	    make_uniq<AnndataDefaultGenerator>(*catalog, schema, std::move(file_path), std::move(tables));
	catalog_set.SetDefaultGenerator(std::move(default_generator));

	return std::move(catalog);
}

static unique_ptr<TransactionManager> AnndataStorageTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                       AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<DuckTransactionManager>(db);
}

//===--------------------------------------------------------------------===//
// Factory Function
//===--------------------------------------------------------------------===//

unique_ptr<StorageExtension> CreateAnndataStorageExtension() {
	auto result = make_uniq<StorageExtension>();
	result->attach = AnndataStorageAttach;
	result->create_transaction_manager = AnndataStorageTransactionManager;
	return result;
}

} // namespace duckdb
