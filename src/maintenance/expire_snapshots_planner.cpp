#include "maintenance/expire_snapshots_planner.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "maintenance/snapshot_files.hpp"

namespace duckdb {

namespace {

//! Spark's default for older_than: snapshots more than five days old
constexpr int64_t DEFAULT_MAX_SNAPSHOT_AGE_MS = 5LL * 24 * 60 * 60 * 1000;
//! The table properties that set the defaults for older_than and retain_last
constexpr const char *MAX_SNAPSHOT_AGE_PROPERTY = "history.expire.max-snapshot-age-ms";
constexpr const char *MIN_SNAPSHOTS_TO_KEEP_PROPERTY = "history.expire.min-snapshots-to-keep";
//! Set to false on tables whose files other tables may use, for example the ones Spark's 'snapshot' procedure creates
constexpr const char *GC_ENABLED_PROPERTY = "gc.enabled";

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

//! A number of at least 'minimum' from a table property, or 'default_value' if the property is not set
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
vector<int64_t> SelectExpiredSnapshots(const IcebergTableMetadata &metadata, const ExpireSnapshotsPlanInput &input) {
	if (!metadata.snapshots.empty() && !metadata.GetLatestSnapshot()) {
		// the commit asserts main against the current snapshot: without one, a concurrent commit could point main at
		// a snapshot this expires
		throw InvalidInputException("iceberg_expire_snapshots: the table has no current snapshot");
	}
	auto retain_last = input.retain_last ? *input.retain_last
	                                     : GetTablePropertyOrDefault(metadata, MIN_SNAPSHOTS_TO_KEEP_PROPERTY, 1, 1);
	auto &older_than_ms = input.older_than_ms;
	auto cutoff = older_than_ms ? *older_than_ms
	                            : Timestamp::GetEpochMs(Timestamp::GetCurrentTimestamp()) -
	                                  GetTablePropertyOrDefault(metadata, MAX_SNAPSHOT_AGE_PROPERTY,
	                                                            DEFAULT_MAX_SNAPSHOT_AGE_MS, 0);
	auto referenced = ReferencedSnapshots(metadata);
	auto retained = RetainedAncestors(metadata, retain_last, cutoff);

	set<int64_t> expired;
	auto &snapshot_ids = input.snapshot_ids;
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

//! A file is unreferenced when an expired snapshot lists it and no retained snapshot uses it. A retained snapshot only
//! uses the data and delete files live in it, so a file an expired snapshot removed goes once nothing else needs it.
UnreferencedFiles FindUnreferencedFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                        const unordered_set<int64_t> &expired) {
	auto kept = CollectSnapshotFiles(
	    context, metadata, [&](const IcebergSnapshot &snapshot) { return !expired.count(snapshot.snapshot_id); });
	auto removed = CollectSnapshotFiles(
	    context, metadata, [&](const IcebergSnapshot &snapshot) { return expired.count(snapshot.snapshot_id) > 0; });

	UnreferencedFiles result;
	for (auto &path : removed.manifest_lists) {
		if (!kept.manifest_lists.count(path)) {
			result.manifest_lists.push_back(path);
		}
	}
	for (auto &path : removed.manifests) {
		if (!kept.manifests.count(path)) {
			result.manifests.push_back(path);
		}
	}
	for (auto &entry : removed.listed_files) {
		if (kept.live_files.count(entry.first)) {
			continue;
		}
		switch (entry.second) {
		case IcebergManifestEntryContentType::DATA:
			result.data_files.push_back(entry.first);
			break;
		case IcebergManifestEntryContentType::POSITION_DELETES:
			result.position_delete_files.push_back(entry.first);
			break;
		case IcebergManifestEntryContentType::EQUALITY_DELETES:
			result.equality_delete_files.push_back(entry.first);
			break;
		}
	}
	return result;
}

} // namespace

vector<string> UnreferencedFiles::AllFiles() const {
	vector<string> result;
	for (auto *kind : {&data_files, &position_delete_files, &equality_delete_files, &manifests, &manifest_lists}) {
		result.insert(result.end(), kind->begin(), kind->end());
	}
	return result;
}

ExpireSnapshotsPlan PlanExpireSnapshots(ClientContext &context, const IcebergTableMetadata &metadata,
                                        const ExpireSnapshotsPlanInput &input) {
	if (input.delete_files && StringUtil::Lower(metadata.GetTableProperty(GC_ENABLED_PROPERTY)) == "false") {
		throw InvalidInputException("iceberg_expire_snapshots: the table sets 'gc.enabled' to false, so other tables "
		                            "may use its files; pass delete_files := false to only expire the snapshots");
	}
	ExpireSnapshotsPlan plan;
	plan.expired_snapshot_ids = SelectExpiredSnapshots(metadata, input);
	if (input.delete_files && !plan.expired_snapshot_ids.empty()) {
		unordered_set<int64_t> expired(plan.expired_snapshot_ids.begin(), plan.expired_snapshot_ids.end());
		plan.unreferenced_files = FindUnreferencedFiles(context, metadata, expired);
	}
	return plan;
}

} // namespace duckdb
