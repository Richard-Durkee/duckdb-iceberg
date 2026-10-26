#include "catalog/rest/iceberg_table_prefetch.hpp"

#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

namespace {

//! Collects the base table references of a query, including those in CTEs and in subquery expressions
class TableReferenceCollector {
public:
	vector<QualifiedName> tables;
	unordered_set<string> cte_names;

	void VisitQueryNode(QueryNode &node) {
		for (auto &cte : node.cte_map.map) {
			cte_names.insert(StringUtil::Lower(cte.first.GetIdentifierName()));
			if (cte.second && cte.second->query_node) {
				VisitQueryNode(*cte.second->query_node);
			}
		}
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    node, [&](unique_ptr<ParsedExpression> &child) { VisitExpression(*child); },
		    [&](TableRef &ref) { VisitTableRef(ref); });
	}

private:
	void VisitExpression(ParsedExpression &expr) {
		if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
			auto &subquery = expr.Cast<SubqueryExpression>();
			auto &statement = subquery.Subquery();
			if (statement && statement->node) {
				VisitQueryNode(*statement->node);
			}
		}
		ParsedExpressionIterator::EnumerateChildren(expr, [&](ParsedExpression &child) { VisitExpression(child); });
	}

	void VisitTableRef(TableRef &ref) {
		if (ref.type == TableReferenceType::BASE_TABLE) {
			tables.push_back(ref.Cast<BaseTableRef>().GetQualifiedName());
		}
	}
};

//! Whether a table reference resolves to `schema` of `catalog`. Unqualified references are matched against the
//! default catalog and schema only
bool ReferencesSchema(ClientContext &context, const QualifiedName &name, const Identifier &catalog_name,
                      const Identifier &schema_name) {
	auto &default_entry = ClientData::Get(context).catalog_search_path->GetDefault();
	if (!name.Catalog().empty()) {
		return name.Catalog() == catalog_name && name.Schema() == schema_name;
	}
	if (!name.Schema().empty()) {
		return name.Schema() == schema_name && default_entry.GetCatalog() == catalog_name;
	}
	return default_entry.GetCatalog() == catalog_name && default_entry.GetSchema() == schema_name;
}

//! Tracks whether a query is active: catalog lookups can also run outside of one, without a query text
class IcebergQueryState : public ClientContextState {
public:
	static constexpr const char *NAME = "iceberg_query_state";

	void QueryBegin(ClientContext &context) override {
		in_query = true;
	}
	void QueryEnd(ClientContext &context) override {
		in_query = false;
	}

	atomic<bool> in_query {false};
};

} // namespace

void IcebergTablePrefetch::RegisterQueryState(ClientContext &context) {
	context.registered_state->GetOrCreate<IcebergQueryState>(IcebergQueryState::NAME);
}

IcebergTablePrefetch::IcebergTablePrefetch(ClientContext &context, IcebergCatalog &catalog)
    : catalog(catalog), executor(context, catalog) {
}

IcebergTablePrefetch::~IcebergTablePrefetch() {
}

void IcebergTablePrefetch::PrefetchQueryTables(ClientContext &context, IcebergTransaction &transaction,
                                               IcebergSchemaEntry &schema) {
	auto &ic_catalog = schema.ParentCatalog().Cast<IcebergCatalog>();
	auto query_state = context.registered_state->Get<IcebergQueryState>(IcebergQueryState::NAME);
	if (!query_state) {
		//! Connections other than the attaching one prefetch from their next query on
		RegisterQueryState(context);
		return;
	}
	if (!query_state->in_query) {
		return;
	}
	auto &query = context.GetCurrentQuery();
	if (query.empty()) {
		return;
	}
	if (!transaction.table_prefetch) {
		transaction.table_prefetch = make_uniq<IcebergTablePrefetch>(context, ic_catalog);
	}
	auto &prefetch = *transaction.table_prefetch;
	auto query_key = schema.name.GetIdentifierName() + "\n" + query;
	if (!prefetch.scanned_queries.insert(std::move(query_key)).second) {
		return;
	}

	TableReferenceCollector collector;
	try {
		Parser parser(context);
		parser.ParseQuery(query);
		for (auto &statement : parser.statements) {
			if (statement->type == StatementType::SELECT_STATEMENT) {
				auto &select = statement->Cast<SelectStatement>();
				if (select.node) {
					collector.VisitQueryNode(*select.node);
				}
			}
		}
	} catch (std::exception &) {
		//! Prefetching is an optimization: queries it can't parse are resolved one table at a time
		return;
	}

	auto &catalog_name = ic_catalog.GetName();
	for (auto &name : collector.tables) {
		auto &table_name = name.Name().GetIdentifierName();
		if (table_name.find_first_of("/\\") != string::npos) {
			continue;
		}
		if (name.Catalog().empty() && name.Schema().empty() &&
		    collector.cte_names.count(StringUtil::Lower(table_name))) {
			continue;
		}
		if (!ReferencesSchema(context, name, catalog_name, schema.name)) {
			continue;
		}
		auto table_key = IcebergTable::GetTableKey(ic_catalog, schema.namespace_items, table_name);
		if (prefetch.pending.count(table_key) || transaction.GetLatestTableState(table_key)) {
			continue;
		}
		if (ic_catalog.attach_options.max_table_staleness_micros.IsValid() &&
		    ic_catalog.table_request_cache.Get(context, table_key, [](const rest_api_objects::LoadTableResult &) {})) {
			continue;
		}
		PendingLoad load;
		load.publication = ic_catalog.table_request_cache.BeginLoad(table_key);
		load.result = prefetch.executor.Schedule(IcebergLoadTableRequest(schema.namespace_items, table_name));
		prefetch.pending.emplace(std::move(table_key), std::move(load));
	}
}

bool IcebergTablePrefetch::TryTakeResult(const string &table_key, IcebergLoadTableResult &result,
                                         unique_ptr<LoadTableCachePublication> &publication) {
	auto entry = pending.find(table_key);
	if (entry == pending.end()) {
		return false;
	}
	auto load = std::move(entry->second);
	pending.erase(entry);
	try {
		result = executor.WaitAndTakeResult(*load.result);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		//! The caller requests the table itself
		return false;
	}
	publication = std::move(load.publication);
	return true;
}

} // namespace duckdb
