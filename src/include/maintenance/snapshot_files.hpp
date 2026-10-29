#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"

#include <functional>

#include "core/metadata/manifest/iceberg_manifest.hpp"

namespace duckdb {

struct IcebergSnapshot;
struct IcebergTableMetadata;

//! The files a set of a table's snapshots use
struct SnapshotFiles {
	unordered_set<string> manifest_lists;
	unordered_set<string> manifests;
	//! Data and delete files that are live in at least one of the snapshots: listed with a status other than DELETED
	unordered_set<string> live_files;
	//! Every data and delete file the snapshots' manifests list, whatever its status, with its content type
	unordered_map<string, IcebergManifestEntryContentType> listed_files;
};

//! Collects the files of every snapshot 'include' accepts. Snapshots share most of their manifests, so each manifest
//! is read once.
SnapshotFiles CollectSnapshotFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                   const std::function<bool(const IcebergSnapshot &)> &include);

} // namespace duckdb
