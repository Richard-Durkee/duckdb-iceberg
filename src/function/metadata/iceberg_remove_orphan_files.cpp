#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include "catalog/rest/api/catalog_api.hpp"
#include "catalog/rest/catalog_entry/schema/iceberg_schema_entry.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table.hpp"
#include "catalog/rest/catalog_entry/table/iceberg_table_schema_version.hpp"
#include "catalog/rest/iceberg_catalog.hpp"
#include "catalog/rest/transaction/iceberg_transaction.hpp"
#include "common/iceberg_utils.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "function/iceberg_functions.hpp"

namespace duckdb {

namespace {

//! Spark's default for older_than: files more than three days old, so files of writes still in progress are kept
static constexpr int64_t DEFAULT_MIN_FILE_AGE_MS = 3LL * 24 * 60 * 60 * 1000;

struct IcebergRemoveOrphanFilesBindData : public TableFunctionData {
	optional_ptr<IcebergTableSchemaVersion> iceberg_table;
	//! Files modified at or after this instant (UTC) are kept
	timestamp_tz_t older_than;
	//! Report the orphan files without deleting them
	bool dry_run = false;
};

struct IcebergRemoveOrphanFilesGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &, TableFunctionInitInput &) {
		return make_uniq<IcebergRemoveOrphanFilesGlobalState>();
	}

	bool computed = false;
	vector<string> orphan_files;
	idx_t offset = 0;
};

static bool CheckTableIsIcebergTable(optional_ptr<CatalogEntry> entry) {
	auto &catalog_entry = entry->Cast<InCatalogEntry>();
	if (catalog_entry.catalog.GetCatalogType() != "iceberg") {
		return false;
	}
	return entry->type == CatalogType::TABLE_ENTRY;
}

static unique_ptr<FunctionData> IcebergRemoveOrphanFilesBind(ClientContext &context, TableFunctionBindInput &input,
                                                             vector<LogicalType> &return_types,
                                                             vector<Identifier> &names) {
	auto ret = make_uniq<IcebergRemoveOrphanFilesBindData>();

	auto input_string = input.inputs[0].ToString();
	auto parts = QualifiedName::ParseComponents(input_string);
	if (parts.size() != 3) {
		throw InvalidInputException(
		    "iceberg_remove_orphan_files: table identifier must be 'catalog.schema.table', got '%s'", input_string);
	}
	auto table_entry = IcebergUtils::GetTableEntry(context, input_string);
	if (!CheckTableIsIcebergTable(table_entry)) {
		throw InvalidInputException("Cannot call iceberg_remove_orphan_files on non-iceberg table");
	}
	ret->iceberg_table = table_entry->Cast<IcebergTableSchemaVersion>();

	ret->older_than = timestamp_tz_t(
	    Timestamp::FromEpochMs(Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()) - DEFAULT_MIN_FILE_AGE_MS));
	for (auto &kv : input.named_parameters) {
		auto option = StringUtil::Lower(kv.first.GetIdentifierName());
		auto &value = kv.second;
		if (value.IsNull()) {
			throw InvalidInputException("iceberg_remove_orphan_files: '%s' cannot be NULL", option);
		}
		if (option == "older_than") {
			ret->older_than = value.GetValue<timestamp_tz_t>();
		} else if (option == "dry_run") {
			ret->dry_run = BooleanValue::Get(value);
		}
	}

	if (input.binder && !ret->dry_run) {
		DatabaseModificationType modification;
		modification |= DatabaseModificationType::ALTER_TABLE;
		input.binder->GetStatementProperties().RegisterDBModify(table_entry->ParentCatalog(), context, modification);
	}

	names.emplace_back("orphan_file_location");
	return_types.push_back(LogicalType::VARCHAR);
	return std::move(ret);
}

//! Compares paths the catalog wrote with paths the file system lists: 's3a://' and 's3n://' name the same objects as
//! 's3://', 'gcs://' the same as 'gs://', and local paths may be written as 'file:///p', 'file:/p' or '/p'
static string NormalizePath(const string &path) {
	if (StringUtil::StartsWith(StringUtil::Lower(path), "file:")) {
		auto rest = path.substr(5);
		auto first = rest.find_first_not_of('/');
		return "/" + (first == string::npos ? string() : rest.substr(first));
	}
	auto scheme_end = path.find("://");
	if (scheme_end == string::npos) {
		return path;
	}
	auto scheme = StringUtil::Lower(path.substr(0, scheme_end));
	if (scheme == "s3a" || scheme == "s3n") {
		scheme = "s3";
	} else if (scheme == "gcs") {
		scheme = "gs";
	}
	return scheme + "://" + path.substr(scheme_end + 3);
}

//! Table metadata files are never removed: catalogs such as Glue return an empty metadata-log while keeping their own
//! record of earlier metadata files, so these small files are left in place. As with the metadata-log, only the
//! current metadata is protected: files only an earlier metadata file references are still orphans
static bool IsTableMetadataFile(const string &path) {
	return StringUtil::EndsWith(path, ".metadata.json") || StringUtil::EndsWith(path, ".metadata.json.gz") ||
	       StringUtil::EndsWith(path, "/version-hint.text");
}

//! The directories under the table location that hold another table: any directory with its own 'metadata'
//! directory of metadata files, other than this table's
static vector<string> FindNestedTables(const vector<OpenFileInfo> &files, const string &location) {
	set<string> result;
	for (auto &file : files) {
		if (!StringUtil::EndsWith(file.path, ".metadata.json") &&
		    !StringUtil::EndsWith(file.path, ".metadata.json.gz")) {
			continue;
		}
		auto file_dir = file.path.rfind('/');
		if (file_dir == string::npos) {
			continue;
		}
		auto metadata_dir = file.path.substr(0, file_dir);
		if (!StringUtil::EndsWith(metadata_dir, "/metadata")) {
			continue;
		}
		auto table_root = metadata_dir.substr(0, metadata_dir.size() - string("/metadata").size());
		if (table_root != location) {
			result.insert(table_root + "/");
		}
	}
	return vector<string>(result.begin(), result.end());
}

static unique_ptr<IcebergManifestList> LoadManifests(ClientContext &context, const IcebergTableMetadata &metadata,
                                                     const IcebergSnapshot &snapshot) {
	IcebergSnapshotScanInfo scan_info;
	scan_info.snapshot = &snapshot;
	scan_info.schema_id = snapshot.GetSchemaId();
	return IcebergManifestList::Load(metadata.GetLocation(), metadata, scan_info, context, IcebergOptions());
}

//! Every file any snapshot of the table can reach, and every metadata and statistics file the table metadata names.
//! Entries the manifests record as deleted are included too: the snapshots before the delete still read them.
static void AddReachableFiles(ClientContext &context, const rest_api_objects::TableMetadata &raw_metadata,
                              unordered_set<string> &reachable) {
	auto add = [&](const string &path) {
		if (!path.empty()) {
			reachable.insert(NormalizePath(path));
		}
	};
	if (raw_metadata.statistics) {
		for (auto &statistics : *raw_metadata.statistics) {
			add(statistics.statistics_path);
		}
	}
	if (raw_metadata.partition_statistics) {
		for (auto &statistics : *raw_metadata.partition_statistics) {
			add(statistics.statistics_path);
		}
	}
	auto metadata = IcebergTableMetadata::FromTableMetadata(raw_metadata);
	for (auto &log_entry : metadata.metadata_log) {
		add(log_entry.metadata_file);
	}
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		add(snapshot.manifest_list);
		auto manifests = LoadManifests(context, metadata, snapshot);
		for (auto &manifest : manifests->GetManifestFilesConst()) {
			add(manifest.GetFile().manifest_path);
			for (auto &manifest_entry : manifest.GetManifestEntries()) {
				add(manifest_entry.data_file.file_path);
			}
		}
	}
}

static unique_ptr<const rest_api_objects::LoadTableResult> LoadTable(ClientContext &context, IcebergTable &table) {
	auto load_result = IRCAPI::GetTable(context, table.catalog, table.schema, table.name);
	if (load_result.error_ || !load_result.result_) {
		throw IOException("iceberg_remove_orphan_files: could not load table '%s' (status %s)", table.name,
		                  EnumUtil::ToString(load_result.status_));
	}
	return std::move(load_result.result_);
}

//! Identifies the table state the orphans were computed against
static string TableStateKey(const rest_api_objects::LoadTableResult &load_table_result) {
	if (load_table_result.metadata_location) {
		return *load_table_result.metadata_location;
	}
	auto &metadata = load_table_result.metadata;
	return StringUtil::Format("%lld/%lld", metadata.current_snapshot_id ? *metadata.current_snapshot_id : -1LL,
	                          metadata.last_updated_ms ? *metadata.last_updated_ms : -1LL);
}

static vector<string> FindOrphanFiles(ClientContext &context, const IcebergRemoveOrphanFilesBindData &bind_data) {
	auto &table = bind_data.iceberg_table->table_info;
	//! Files an uncommitted write of this transaction already wrote are referenced by no committed metadata yet
	if (IcebergTransaction::Get(context, table.catalog).HasTableUpdate()) {
		throw InvalidInputException("iceberg_remove_orphan_files can not run in a transaction that has uncommitted "
		                            "changes to the catalog's tables");
	}
	bind_data.iceberg_table->PrepareIcebergScanFromEntry(context);

	//! Load the table again: a commit since this query bound may reference files that are now older than the cutoff
	auto load_table_result_ptr = LoadTable(context, table);
	auto &load_table_result = *load_table_result_ptr;

	unordered_set<string> reachable;
	AddReachableFiles(context, load_table_result.metadata, reachable);
	//! Some catalogs rebuild the returned metadata from their own state, so also keep what the metadata file names
	if (load_table_result.metadata_location) {
		auto &metadata_location = *load_table_result.metadata_location;
		reachable.insert(NormalizePath(metadata_location));
		auto &fs = FileSystem::GetFileSystem(context);
		AddReachableFiles(context, IcebergTableMetadata::Parse(metadata_location, fs, "none"), reachable);
	}

	auto &raw_location = load_table_result.metadata.location;
	string location = raw_location ? *raw_location : string();
	if (location.empty()) {
		throw InvalidInputException("iceberg_remove_orphan_files: table '%s' has no location", table.name);
	}
	while (StringUtil::EndsWith(location, "/")) {
		location.pop_back();
	}
	//! The location is listed with a glob, so a location that is itself a pattern would match other directories
	if (FileSystem::HasGlob(location)) {
		throw InvalidInputException("iceberg_remove_orphan_files: table location '%s' contains glob characters",
		                            location);
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto files = fs.GlobFiles(location + "/**", FileGlobOptions::ALLOW_EMPTY);
	auto nested_tables = FindNestedTables(files, location);
	vector<string> result;
	for (auto &file : files) {
		if (IsTableMetadataFile(file.path) || reachable.count(NormalizePath(file.path))) {
			continue;
		}
		bool in_nested_table = false;
		for (auto &nested_table : nested_tables) {
			if (StringUtil::StartsWith(file.path, nested_table)) {
				in_nested_table = true;
				break;
			}
		}
		if (in_nested_table) {
			continue;
		}
		//! A file whose age is unknown may belong to a write in progress
		timestamp_t last_modified;
		if (!file.extended_info || !file.extended_info->TryGetOption("last_modified", last_modified) ||
		    last_modified.value >= bind_data.older_than.value) {
			continue;
		}
		result.push_back(file.path);
	}

	//! A commit between loading the table and listing may have added references to files that were already there
	if (!result.empty() && TableStateKey(*LoadTable(context, table)) != TableStateKey(load_table_result)) {
		throw InvalidInputException("iceberg_remove_orphan_files: table '%s' changed while looking for orphan files, "
		                            "try again",
		                            table.name);
	}
	return result;
}

static void IcebergRemoveOrphanFilesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<IcebergRemoveOrphanFilesBindData>();
	auto &global_state = data.global_state->Cast<IcebergRemoveOrphanFilesGlobalState>();
	if (!global_state.computed) {
		global_state.computed = true;
		global_state.orphan_files = FindOrphanFiles(context, bind_data);
		if (!bind_data.dry_run && !global_state.orphan_files.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			try {
				fs.RemoveFiles(global_state.orphan_files);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				throw IOException("iceberg_remove_orphan_files: failed to delete some of the %llu orphan files, some "
				                  "may already be deleted (run with dry_run := true to list what is left): %s",
				                  static_cast<uint64_t>(global_state.orphan_files.size()), error.RawMessage());
			}
		}
	}
	auto &files = global_state.orphan_files;
	idx_t count = 0;
	auto &column = output.data[0];
	while (global_state.offset < files.size() && count < STANDARD_VECTOR_SIZE) {
		FlatVector::GetDataMutable<string_t>(column)[count] =
		    StringVector::AddString(column, files[global_state.offset]);
		global_state.offset++;
		count++;
	}
	output.SetChildCardinality(count);
}

} // namespace

TableFunctionSet IcebergFunctions::GetIcebergRemoveOrphanFilesFunction() {
	TableFunctionSet function_set("iceberg_remove_orphan_files");
	TableFunction function(
	    "iceberg_remove_orphan_files", FunctionSignature().AddPositionalOnly("table_name", LogicalType::VARCHAR),
	    IcebergRemoveOrphanFilesFunction, IcebergRemoveOrphanFilesBind, IcebergRemoveOrphanFilesGlobalState::Init);
	function.GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("older_than", LogicalType::TIMESTAMP_TZ).Add("dry_run", LogicalType::BOOLEAN);
	});
	function_set.AddFunction(function);
	return function_set;
}

} // namespace duckdb
