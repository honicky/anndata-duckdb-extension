#pragma once

#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/catalog/default/default_generator.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include <map>

namespace duckdb {

//! TableViewInfo holds information about a table/view to be created
struct TableViewInfo {
	string name;            //! Table name (e.g., "obs", "obsm_pca")
	string table_type;      //! Table type (e.g., "obs", "obsm", "layers")
	string param;           //! Additional parameter (e.g., matrix name for obsm)
	string var_name_column; //! Column for gene names (e.g., "gene_symbols")
	string var_id_column;   //! Column for gene IDs (e.g., "ensembl_id")
};

//! AnndataDefaultGenerator creates virtual views for AnnData tables
class AnndataDefaultGenerator : public DefaultGenerator {
public:
	AnndataDefaultGenerator(Catalog &catalog, SchemaCatalogEntry &schema, string file_path,
	                        vector<TableViewInfo> tables);

	unique_ptr<CatalogEntry> CreateDefaultEntry(ClientContext &context, const string &entry_name) override;
	vector<string> GetDefaultEntries() override;

private:
	SchemaCatalogEntry &schema;
	string file_path;
	vector<TableViewInfo> tables;
	std::map<string, TableViewInfo> table_map;

	//! Generate SQL for a table view
	string GenerateViewSQL(const TableViewInfo &info) const;
};

//! Create the AnnData storage extension
unique_ptr<StorageExtension> CreateAnndataStorageExtension();

//! Discover available tables from an AnnData file
vector<TableViewInfo> DiscoverAnndataTables(const string &file_path, const string &var_name_column,
                                            const string &var_id_column);

} // namespace duckdb
