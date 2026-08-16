#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table_function.hpp"

#include <type_traits>

// DuckDB main (post-v1.5.x) refactored FlatVector into its own header and
// made GetData<T>() always return const T*. Mutable write access now requires
// GetDataMutable<T>() and ValidityMutable(). These APIs don't exist in v1.5.x.
//
// DuckDB main also requires SetChildCardinality() to propagate sizes to per-vector
// buffers, while v1.5.x only needs SetCardinality(). Without SetChildCardinality,
// the per-vector buffer size stays at 0, causing the query engine to treat entries
// as uninitialized.
//
// Detect which version we're building against and provide unified wrappers.
#if __has_include("duckdb/common/vector/flat_vector.hpp")
#include "duckdb/common/vector/flat_vector.hpp"
#define DUCKDB_FLAT_VECTOR_HAS_MUTABLE 1
#endif

// DuckDB main (post-v1.5.x) introduced a dedicated Identifier type and reworked
// the string-based catalog APIs around it:
//   * DefaultGenerator::CreateDefaultEntry / GetDefaultEntries now take/return
//     Identifier instead of string.
//   * CreateViewInfo dropped its public `schema` / `view_name` members in favor
//     of SetSchema() / SetViewName().
//   * table_function_bind_t reports its output column names as vector<Identifier>.
//   * ClientContext::TryGetCurrentSetting takes the setting key as an Identifier.
// Identifier is implicitly constructible from a string literal but *explicitly*
// from a runtime string, so any call site that passes a `string` needs a wrapper.
// identifier.hpp doesn't exist in v1.5.x, so use it as the version probe.
#if __has_include("duckdb/common/identifier.hpp")
#include "duckdb/common/identifier.hpp"
#define DUCKDB_HAS_IDENTIFIER 1
#endif

namespace duckdb {
namespace compat {

// Types used to satisfy the DefaultGenerator virtual signatures on each version, plus
// the `names` out-parameter of a table function bind (see table_function_bind_t).
// Append to BindColumnNames with emplace_back(), which works for both string and
// Identifier; push_back() only compiles for string literals, because Identifier's
// constructor from a runtime string is explicit.
#ifdef DUCKDB_HAS_IDENTIFIER
using DefaultEntryName = Identifier;
using DefaultEntryList = vector<Identifier>;
using BindColumnNames = vector<Identifier>;
#else
using DefaultEntryName = string;
using DefaultEntryList = vector<string>;
using BindColumnNames = vector<string>;
#endif

// The probe above infers the name type from the *presence of a header*, which is a proxy
// rather than the fact itself. Pull the real element type out of DuckDB's own typedef and
// assert the two agree, so a DuckDB change that moves the name type without adding or
// removing identifier.hpp fails here with one readable message instead of a cascade of
// signature mismatches at every bind function.
template <class T>
struct bind_names_of;
template <class R, class A, class B, class C, class N>
struct bind_names_of<R (*)(A, B, C, vector<N> &)> {
	using type = N;
};

static_assert(std::is_same<bind_names_of<table_function_bind_t>::type, DefaultEntryName>::value,
              "DuckDB changed the table_function_bind_t column-name type. Update the DUCKDB_HAS_IDENTIFIER "
              "probe and the aliases in duckdb_compat.hpp.");

//! Build the version-appropriate default-entry name from a runtime string.
static inline DefaultEntryName MakeDefaultEntryName(const string &name) {
#ifdef DUCKDB_HAS_IDENTIFIER
	return Identifier(name);
#else
	return name;
#endif
}

//! Get the raw string from a default-entry name (for internal string-keyed lookups).
static inline const string &DefaultEntryNameToString(const DefaultEntryName &name) {
#ifdef DUCKDB_HAS_IDENTIFIER
	return name.GetIdentifierName();
#else
	return name;
#endif
}

//! Copy bind column names into a plain vector<string>, for the bind data's own bookkeeping.
static inline vector<string> BindColumnNamesToStrings(const BindColumnNames &names) {
#ifdef DUCKDB_HAS_IDENTIFIER
	vector<string> result;
	result.reserve(names.size());
	for (const auto &name : names) {
		result.emplace_back(name.GetIdentifierName());
	}
	return result;
#else
	return names;
#endif
}

//! Wrap a runtime string as the key type ClientContext::TryGetCurrentSetting expects.
//! String literals can be passed straight through on both versions.
//! Returns by value on both branches deliberately: an asymmetric `const string &`
//! return would let `const auto &k = SettingKey(MakeKey());` dangle on v1.5.x while
//! staying safe on duckdb/main. One short setting key is worth the copy.
#ifdef DUCKDB_HAS_IDENTIFIER
static inline Identifier SettingKey(const string &key) {
	return Identifier(key);
}
#else
static inline string SettingKey(const string &key) {
	return key;
}
#endif

template <class T>
static inline T *FlatVectorGetData(Vector &vector) {
#ifdef DUCKDB_FLAT_VECTOR_HAS_MUTABLE
	return FlatVector::GetDataMutable<T>(vector);
#else
	return FlatVector::GetData<T>(vector);
#endif
}

static inline ValidityMask &FlatVectorValidity(Vector &vector) {
#ifdef DUCKDB_FLAT_VECTOR_HAS_MUTABLE
	return FlatVector::ValidityMutable(vector);
#else
	return FlatVector::Validity(vector);
#endif
}

static inline void SetChunkCardinality(DataChunk &chunk, idx_t count) {
#ifdef DUCKDB_FLAT_VECTOR_HAS_MUTABLE
	chunk.SetChildCardinality(count);
#else
	chunk.SetCardinality(count);
#endif
}

} // namespace compat
} // namespace duckdb
