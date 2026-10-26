//===----------------------------------------------------------------------===//
//                         DuckDB
//
// catalog/rest/iceberg_table_prefetch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/iceberg_request_executor.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class IcebergCatalog;
class IcebergSchemaEntry;
class IcebergTransaction;
class LoadTableCachePublication;

//! Binding resolves the tables of a query one at a time, and each lookup of an Iceberg table waits for its own
//! LoadTable request. On the first lookup in a schema, this schedules the LoadTable requests of every other table of
//! that schema the query references, so they are in flight together.
class IcebergTablePrefetch {
public:
	IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog);
	~IcebergTablePrefetch();

	//! Register the state that tells lookups whether a query is active, so they can read its text
	static void RegisterQueryState(ClientContext &context);
	//! Schedule LoadTable requests for the tables of `schema` that the current query references and that the
	//! transaction has not resolved yet
	static void PrefetchQueryTables(ClientContext &context, IcebergTransaction &transaction,
	                                IcebergSchemaEntry &schema);
	//! Take the response of a prefetched LoadTable request for `table_key`, waiting for it if needed.
	//! Returns false when no request was scheduled for the table, or when it failed to run
	bool TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
	                   unique_ptr<LoadTableCachePublication> &publication);

private:
	struct PendingLoad {
		unique_ptr<LoadTableCachePublication> publication;
		shared_ptr<IcebergRequestResult<IcebergLoadTableResult>> result;
	};

	IcebergCatalog &catalog;
	IcebergRequestExecutor executor;
	unordered_map<string, PendingLoad> pending;
	//! Queries already scanned for table references, keyed by schema and query text
	unordered_set<string> scanned_queries;
};

} // namespace duckdb
