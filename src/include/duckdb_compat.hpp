#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"

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
// identifier.hpp doesn't exist in v1.5.x, so use it as the version probe.
#if __has_include("duckdb/common/identifier.hpp")
#include "duckdb/common/identifier.hpp"
#define DUCKDB_HAS_IDENTIFIER 1
#endif

namespace duckdb {
namespace compat {

// Types used to satisfy the DefaultGenerator virtual signatures on each version.
#ifdef DUCKDB_HAS_IDENTIFIER
using DefaultEntryName = Identifier;
using DefaultEntryList = vector<Identifier>;
#else
using DefaultEntryName = string;
using DefaultEntryList = vector<string>;
#endif

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
