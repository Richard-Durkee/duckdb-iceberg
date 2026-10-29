#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

//! Spark's default for older_than: snapshots more than five days old
constexpr int64_t DEFAULT_MAX_SNAPSHOT_AGE_MS = 5LL * 24 * 60 * 60 * 1000;
//! The table properties that set the defaults for older_than and retain_last
constexpr const char *MAX_SNAPSHOT_AGE_PROPERTY = "history.expire.max-snapshot-age-ms";
constexpr const char *MIN_SNAPSHOTS_TO_KEEP_PROPERTY = "history.expire.min-snapshots-to-keep";

struct IcebergExpireSnapshotsBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	optional<int64_t> older_than_ms;
	optional<int64_t> retain_last;
	optional<vector<int64_t>> snapshot_ids;
	//! Report what would be expired without changing the table
	bool dry_run = false;
};

struct IcebergExpireSnapshotsGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergExpireSnapshotsGlobalState>();
	}

	bool finished = false;
};

bool CheckTableIsIcebergTable(optional_ptr<CatalogEntry> entry) {
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	if (catalog_entry.catalog.GetCatalogType() != "iceberg") {
		return false;
	}
	return entry->type == CatalogType::TABLE_ENTRY;
}

//! The current snapshot and the snapshots a branch or tag points at: expiring one would leave a ref dangling
unordered_set<int64_t> ReferencedSnapshots(const IcebergTableMetadata &metadata) {
	unordered_set<int64_t> result;
	if (metadata.refs) {
		for (auto &ref : metadata.refs->additional_properties) {
			result.insert(ref.second.snapshot_id);
		}
	}
	auto current = metadata.GetLatestSnapshot();
	if (current) {
		result.insert(current->snapshot_id);
	}
	return result;
}

//! Walks back from a branch head: an ancestor is kept while fewer than 'count' are kept, or while it is not older
//! than 'cutoff_ms'
void RetainBranchAncestors(const IcebergTableMetadata &metadata, int64_t head, int64_t count, int64_t cutoff_ms,
                           unordered_set<int64_t> &result) {
	auto snapshot = metadata.FindSnapshotByIdInternal(head);
	// stops on a parent cycle in broken metadata; separate from 'result', which other branches share
	unordered_set<int64_t> visited;
	while (snapshot && (NumericCast<int64_t>(visited.size()) < count || snapshot->timestamp_ms.value >= cutoff_ms)) {
		if (!visited.insert(snapshot->snapshot_id).second) {
			break;
		}
		result.insert(snapshot->snapshot_id);
		if (!snapshot->parent_snapshot_id) {
			break;
		}
		snapshot = metadata.FindSnapshotByIdInternal(*snapshot->parent_snapshot_id);
	}
}

//! The ancestors each branch keeps (the current snapshot's history if 'main' is not a ref)
unordered_set<int64_t> RetainedAncestors(const IcebergTableMetadata &metadata, int64_t retain_last, int64_t cutoff_ms) {
	unordered_set<int64_t> result;
	bool has_main = false;
	if (metadata.refs) {
		for (auto &entry : metadata.refs->additional_properties) {
			if (StringUtil::Lower(entry.second.type) != "branch") {
				continue;
			}
			has_main |= entry.first == "main";
			RetainBranchAncestors(metadata, entry.second.snapshot_id, retain_last, cutoff_ms, result);
		}
	}
	auto current = metadata.GetLatestSnapshot();
	if (current && !has_main) {
		RetainBranchAncestors(metadata, current->snapshot_id, retain_last, cutoff_ms, result);
	}
	return result;
}

unique_ptr<FunctionData> IcebergExpireSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto ret = make_uniq<IcebergExpireSnapshotsBindData>();

	auto input_string = input.inputs[0].ToString();
	auto parts = QualifiedName::ParseComponents(input_string);
	if (parts.size() != 3) {
		throw InvalidInputException(
		    "iceberg_expire_snapshots: table identifier must be 'catalog.schema.table', got '%s'", input_string);
	}
	auto table_entry = IcebergUtils::GetTableEntry(context, input_string);
	if (!CheckTableIsIcebergTable(table_entry)) {
		throw InvalidInputException("iceberg_expire_snapshots: '%s' is not an Iceberg table", input_string);
	}
	ret->iceberg_table = table_entry->Cast<IcebergTableSchemaVersion>();

	for (auto &kv : input.named_parameters) {
		auto option = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &value = kv.second;
		if (value.IsNull()) {
			throw InvalidInputException("iceberg_expire_snapshots: '%s' cannot be NULL", option);
		}
		if (option == "older_than") {
			ret->older_than_ms = Timestamp::GetEpochMs(timestamp_t(value.GetValue<timestamp_tz_t>().value));
		} else if (option == "retain_last") {
			ret->retain_last = value.GetValue<int64_t>();
			if (*ret->retain_last < 1) {
				throw InvalidInputException("iceberg_expire_snapshots: retain_last must be at least 1, got %lld",
				                            *ret->retain_last);
			}
		} else if (option == "dry_run") {
			ret->dry_run = BooleanValue::Get(value);
		} else if (option == "snapshot_ids") {
			ret->snapshot_ids.emplace();
			for (auto &id : ListValue::GetChildren(value)) {
				if (id.IsNull()) {
					throw InvalidInputException("iceberg_expire_snapshots: snapshot_ids cannot contain NULL");
				}
				ret->snapshot_ids->push_back(id.GetValue<int64_t>());
			}
		}
	}

	if (input.binder && !ret->dry_run) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(table_entry->ParentCatalog(), context, modification);
	}

	names.emplace_back("expired_snapshots_count");
	return_types.push_back(LogicalType::BIGINT);
	return std::move(ret);
}

//! A non-negative number from a table property, or 'default_value' if the property is not set
int64_t GetTablePropertyOrDefault(const IcebergTableMetadata &metadata, const char *property, int64_t default_value,
                                  int64_t minimum) {
	auto value = metadata.GetTableProperty(property);
	if (value.empty()) {
		return default_value;
	}
	int64_t result;
	if (!TryCast::Operation<string_t, int64_t>(string_t(value), result) || result < minimum) {
		throw InvalidInputException("iceberg_expire_snapshots: table property '%s' must be a number of at least %lld, "
		                            "got '%s'",
		                            property, minimum, value);
	}
	return result;
}

//! The snapshots to expire, chosen against the table state this transaction reads
vector<int64_t> SelectExpiredSnapshots(const IcebergExpireSnapshotsBindData &bind_data,
                                       const IcebergTableMetadata &metadata) {
	if (!metadata.snapshots.empty() && !metadata.GetLatestSnapshot()) {
		// the commit asserts main against the current snapshot: without one, a concurrent commit could point main at
		// a snapshot this expires
		throw InvalidInputException("iceberg_expire_snapshots: the table has no current snapshot");
	}
	auto retain_last = bind_data.retain_last
	                       ? *bind_data.retain_last
	                       : GetTablePropertyOrDefault(metadata, MIN_SNAPSHOTS_TO_KEEP_PROPERTY, 1, 1);
	auto &older_than_ms = bind_data.older_than_ms;
	auto cutoff = older_than_ms ? *older_than_ms
	                            : Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()) -
	                                  GetTablePropertyOrDefault(metadata, MAX_SNAPSHOT_AGE_PROPERTY,
	                                                            DEFAULT_MAX_SNAPSHOT_AGE_MS, 0);
	auto referenced = ReferencedSnapshots(metadata);
	auto retained = RetainedAncestors(metadata, retain_last, cutoff);

	set<int64_t> expired;
	auto &snapshot_ids = bind_data.snapshot_ids;
	if (snapshot_ids) {
		for (auto id : *snapshot_ids) {
			if (!metadata.FindSnapshotByIdInternal(id)) {
				throw InvalidInputException("iceberg_expire_snapshots: snapshot %lld does not exist", id);
			}
			if (referenced.count(id)) {
				throw InvalidInputException("iceberg_expire_snapshots: snapshot %lld is the current snapshot or is "
				                            "referenced by a branch or tag, and cannot be expired",
				                            id);
			}
			expired.insert(id);
		}
	}
	// named snapshots are expired as given; by age only when no ids are named, or older_than is given as well
	if (!snapshot_ids || older_than_ms) {
		for (auto &entry : metadata.snapshots) {
			auto &snapshot = entry.second;
			if (snapshot.timestamp_ms.value < cutoff && !referenced.count(snapshot.snapshot_id) &&
			    !retained.count(snapshot.snapshot_id)) {
				expired.insert(snapshot.snapshot_id);
			}
		}
	}
	return vector<int64_t>(expired.begin(), expired.end());
}

void IcebergExpireSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergExpireSnapshotsBindData>();
	auto &global_state = data.global_state->Cast<IcebergExpireSnapshotsGlobalState>();
	if (global_state.finished) {
		output.SetChildCardinality(0);
		return;
	}
	global_state.finished = true;

	auto &iceberg_transaction = IcebergTransaction::Get(context, bind_data.iceberg_table->catalog);
	// changes queued earlier in the transaction, such as a rollback, are not in the metadata the snapshots are chosen
	// against
	if (!bind_data.dry_run && iceberg_transaction.HasTableUpdate()) {
		throw InvalidInputException("iceberg_expire_snapshots: cannot run in a transaction that has uncommitted "
		                            "changes to the catalog's tables");
	}
	auto &metadata = bind_data.iceberg_table->table_info.table_metadata;
	auto expired_snapshot_ids = SelectExpiredSnapshots(bind_data, metadata);
	if (!expired_snapshot_ids.empty() && !bind_data.dry_run) {
		ApplyTableUpdate(bind_data.iceberg_table->table_info, iceberg_transaction, [&](IcebergTable &table) {
			auto &transaction_data = table.GetOrCreateTransactionData(iceberg_transaction);
			transaction_data.TableRemoveSnapshots(expired_snapshot_ids);
		});
	}
	FlatVector::GetDataMutable<int64_t>(output.data[0])[0] = NumericCast<int64_t>(expired_snapshot_ids.size());
	output.SetChildCardinality(1);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergExpireSnapshotsFunction() {
	TableFunctionSet function_set("iceberg_expire_snapshots");
	TableFunction function(
	    "iceberg_expire_snapshots", FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR),
	    IcebergExpireSnapshotsFunction, IcebergExpireSnapshotsBind, IcebergExpireSnapshotsGlobalState::Init);
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("older_than", LogicalType::TIMESTAMP_TZ)
		    .Add("retain_last", LogicalType::BIGINT)
		    .Add("snapshot_ids", LogicalType::LIST(LogicalType::BIGINT))
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
