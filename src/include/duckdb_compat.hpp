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

// DuckDB main introduced the Identifier class for case-insensitive SQL
// identifiers and updated DefaultGenerator and CreateViewInfo to use it.
// In v1.5.x these APIs use plain strings.
#if __has_include("duckdb/common/identifier.hpp")
#include "duckdb/common/identifier.hpp"
#define DUCKDB_HAS_IDENTIFIER 1
#endif

namespace duckdb {
namespace compat {

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

// Type aliases for DefaultGenerator signatures that changed from string to Identifier.
#ifdef DUCKDB_HAS_IDENTIFIER
using DefaultEntryName = Identifier;
using DefaultEntryList = vector<Identifier>;
#else
using DefaultEntryName = string;
using DefaultEntryList = vector<string>;
#endif

} // namespace compat
} // namespace duckdb
