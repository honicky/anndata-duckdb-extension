#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "h5_file_cache.hpp"

namespace duckdb {

// Helper function to get S3 credentials from DuckDB's SecretManager
// Returns true if credentials were found and config was populated
inline bool GetS3ConfigFromSecrets(ClientContext &context, const string &path, H5FileCache::RemoteConfig &config) {
	// Only look up secrets for S3 URLs
	bool is_s3 = (path.rfind("s3://", 0) == 0 || path.rfind("s3a://", 0) == 0);
	if (!is_s3) {
		return false;
	}

	try {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

		auto secret_match = secret_manager.LookupSecret(transaction, path, "s3");

		if (!secret_match.HasMatch()) {
			// No secret found - will use anonymous access
			return false;
		}

		const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_match.secret_entry->secret);

		// Extract credentials from the secret
		// Note: TryGetValue returns Value with IsNull() for missing keys
		auto key_id_val = kv_secret.TryGetValue("key_id");
		auto secret_val = kv_secret.TryGetValue("secret");
		// DuckDB S3 secrets use "session_token" for the token
		auto session_token_val = kv_secret.TryGetValue("session_token");
		auto region_val = kv_secret.TryGetValue("region");
		auto endpoint_val = kv_secret.TryGetValue("endpoint");
		auto use_ssl_val = kv_secret.TryGetValue("use_ssl");

		// Convert to strings, handling NULL values properly
		string key_id = key_id_val.IsNull() ? "" : key_id_val.ToString();
		string secret = secret_val.IsNull() ? "" : secret_val.ToString();
		string session_token = session_token_val.IsNull() ? "" : session_token_val.ToString();
		string region = region_val.IsNull() ? "" : region_val.ToString();
		string endpoint = endpoint_val.IsNull() ? "" : endpoint_val.ToString();

		// Populate the config
		config.s3_access_key = key_id;
		config.s3_secret_key = secret;
		config.s3_session_token = session_token;
		config.s3_region = region.empty() ? "us-east-1" : region;
		config.s3_endpoint = endpoint;
		config.s3_use_ssl = use_ssl_val.IsNull() ? true : use_ssl_val.GetValue<bool>();

		return !key_id.empty() || !secret.empty();
	} catch (const std::exception &e) {
		// Failed to get secrets - continue with anonymous access
		return false;
	}
}

} // namespace duckdb
