#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "catalog/rest/transaction/iceberg_transaction_data.hpp"
#include "common/iceberg_utils.hpp"
#include "function/iceberg_functions.hpp"
#include "maintenance/expire_snapshots_planner.hpp"

namespace duckdb {

namespace {

struct IcebergExpireSnapshotsBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	ExpireSnapshotsPlanInput plan_input;
	//! Report what would be expired and deleted without changing the table
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

unique_ptr<FunctionData> IcebergExpireSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto ret = make_uniq<IcebergExpireSnapshotsBindData>();

	auto input_string = input.inputs[0].ToString();
	auto table_entry = IcebergUtils::GetTableEntry(context, input_string);
	if (!CheckTableIsIcebergTable(table_entry)) {
		throw InvalidInputException("iceberg_expire_snapshots: '%s' is not an Iceberg table", input_string);
	}
	ret->iceberg_table = table_entry->Cast<IcebergTableSchemaVersion>();

	ret->plan_input.delete_files =
	    ret->iceberg_table->catalog.Cast<IcebergCatalog>().attach_options.remove_files_on_delete;
	for (auto &kv : input.named_parameters) {
		auto option = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &value = kv.second;
		if (value.IsNull()) {
			throw InvalidInputException("iceberg_expire_snapshots: '%s' cannot be NULL", option);
		}
		if (option == "older_than") {
			ret->plan_input.older_than_ms = Timestamp::GetEpochMs(timestamp_t(value.GetValue<timestamp_tz_t>().value));
		} else if (option == "retain_last") {
			ret->plan_input.retain_last = value.GetValue<int64_t>();
			if (*ret->plan_input.retain_last < 1) {
				throw InvalidInputException("iceberg_expire_snapshots: retain_last must be at least 1, got %lld",
				                            *ret->plan_input.retain_last);
			}
		} else if (option == "delete_files") {
			ret->plan_input.delete_files = BooleanValue::Get(value);
		} else if (option == "dry_run") {
			ret->dry_run = BooleanValue::Get(value);
		} else if (option == "snapshot_ids") {
			ret->plan_input.snapshot_ids.emplace();
			for (auto &id : ListValue::GetChildren(value)) {
				if (id.IsNull()) {
					throw InvalidInputException("iceberg_expire_snapshots: snapshot_ids cannot contain NULL");
				}
				ret->plan_input.snapshot_ids->push_back(id.GetValue<int64_t>());
			}
		}
	}

	if (input.binder && !ret->dry_run) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(table_entry->ParentCatalog(), context, modification);
	}

	// Spark's result columns, without the statistics files it also counts, and the number of snapshots expired
	for (auto &name :
	     {"deleted_data_files_count", "deleted_position_delete_files_count", "deleted_equality_delete_files_count",
	      "deleted_manifest_files_count", "deleted_manifest_lists_count", "expired_snapshots_count"}) {
		names.emplace_back(name);
		return_types.push_back(LogicalType::BIGINT);
	}
	return std::move(ret);
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
	if (bind_data.plan_input.delete_files) {
		// the manifests are read to find the files
		bind_data.iceberg_table->PrepareIcebergScanFromEntry(context);
	}
	auto plan = PlanExpireSnapshots(context, bind_data.iceberg_table->table_info.table_metadata, bind_data.plan_input);
	auto &files = plan.unreferenced_files;
	if (!plan.expired_snapshot_ids.empty() && !bind_data.dry_run) {
		ApplyTableUpdate(bind_data.iceberg_table->table_info, iceberg_transaction, [&](IcebergTable &table) {
			auto &transaction_data = table.GetOrCreateTransactionData(iceberg_transaction);
			transaction_data.TableRemoveSnapshots(plan.expired_snapshot_ids, files.AllFiles());
		});
	}
	idx_t col = 0;
	for (auto count : {files.data_files.size(), files.position_delete_files.size(), files.equality_delete_files.size(),
	                   files.manifests.size(), files.manifest_lists.size(), plan.expired_snapshot_ids.size()}) {
		FlatVector::GetDataMutable<int64_t>(output.data[col++])[0] = NumericCast<int64_t>(count);
	}
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
		    .Add("delete_files", LogicalType::BOOLEAN)
		    .Add("dry_run", LogicalType::BOOLEAN);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
