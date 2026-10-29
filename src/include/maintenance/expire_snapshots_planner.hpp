#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct IcebergTableMetadata;

struct ExpireSnapshotsPlanInput {
	//! Expire snapshots older than this; defaults to the table's history.expire.max-snapshot-age-ms, or five days
	optional<int64_t> older_than_ms;
	//! The most recent snapshots of each branch to keep; defaults to history.expire.min-snapshots-to-keep, or 1
	optional<int64_t> retain_last;
	//! Expire exactly these snapshots (and, with older_than, the old ones too)
	optional<vector<int64_t>> snapshot_ids;
	//! Also find the files only the expired snapshots use
	bool delete_files = false;
};

//! The files only the expired snapshots use, by kind, in Spark's result order
struct UnreferencedFiles {
	vector<string> data_files;
	vector<string> position_delete_files;
	vector<string> equality_delete_files;
	vector<string> manifests;
	vector<string> manifest_lists;

	vector<string> AllFiles() const;
};

struct ExpireSnapshotsPlan {
	vector<int64_t> expired_snapshot_ids;
	//! Empty unless the input asks to delete files
	UnreferencedFiles unreferenced_files;
};

//! Chooses the snapshots to expire, and the files only they use, against 'metadata'. The table's storage credentials
//! must be set up when the input asks to delete files, since the manifests are read.
ExpireSnapshotsPlan PlanExpireSnapshots(ClientContext &context, const IcebergTableMetadata &metadata,
                                        const ExpireSnapshotsPlanInput &input);

} // namespace duckdb
