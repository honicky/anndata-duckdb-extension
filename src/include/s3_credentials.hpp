#pragma once

#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "h5_file_cache.hpp"

namespace duckdb {

// Helper: try to read a single DuckDB setting into a string, returning empty on failure
inline string TryGetSettingString(const ClientContext &context, const string &key) {
	Value result;
	auto lookup = context.TryGetCurrentSetting(key, result);
	if (lookup && !result.IsNull()) {
		return result.ToString();
	}
	return "";
}

// Helper: try to read S3 credentials from DuckDB global settings
// (set by load_aws_credentials() or SET s3_access_key_id = '...')
inline bool GetS3ConfigFromSettings(ClientContext &context, H5FileCache::RemoteConfig &config) {
	try {
		string key_id = TryGetSettingString(context, "s3_access_key_id");
		string secret = TryGetSettingString(context, "s3_secret_access_key");

		if (key_id.empty() && secret.empty()) {
			return false;
		}

		config.s3_access_key = key_id;
		config.s3_secret_key = secret;
		config.s3_session_token = TryGetSettingString(context, "s3_session_token");
		string region = TryGetSettingString(context, "s3_region");
		config.s3_region = region.empty() ? "us-east-1" : region;
		config.s3_endpoint = TryGetSettingString(context, "s3_endpoint");

		Value ssl_val;
		auto ssl_lookup = context.TryGetCurrentSetting("s3_use_ssl", ssl_val);
		config.s3_use_ssl = (ssl_lookup && !ssl_val.IsNull()) ? ssl_val.GetValue<bool>() : true;

		return true;
	} catch (...) {
		return false;
	}
}

// Helper function to get S3 credentials from DuckDB's SecretManager,
// falling back to global settings (set by load_aws_credentials()).
// Returns true if credentials were found and config was populated.
inline bool GetS3ConfigFromSecrets(ClientContext &context, const string &path, H5FileCache::RemoteConfig &config) {
	// Only look up secrets for S3 URLs
	bool is_s3 = (path.rfind("s3://", 0) == 0 || path.rfind("s3a://", 0) == 0);
	if (!is_s3) {
		return false;
	}

	// First: try the SecretManager (CREATE SECRET ...)
	try {
		auto &secret_manager = SecretManager::Get(context);
		auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

		auto secret_match = secret_manager.LookupSecret(transaction, path, "s3");

		if (secret_match.HasMatch()) {
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

			if (!key_id.empty() || !secret.empty()) {
				return true;
			}
		}
	} catch (const std::exception &e) {
		// SecretManager lookup failed - fall through to settings
	}

	// Fallback: try DuckDB global settings (set by load_aws_credentials() or SET commands)
	return GetS3ConfigFromSettings(context, config);
}

} // namespace duckdb
