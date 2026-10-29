#include "maintenance/snapshot_files.hpp"

#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/manifest/iceberg_manifest_list.hpp"
#include "iceberg_options.hpp"
#include "planning/metadata_io/avro/avro_scan.hpp"
#include "planning/metadata_io/manifest/iceberg_manifest_reader.hpp"

namespace duckdb {

SnapshotFiles CollectSnapshotFiles(ClientContext &context, const IcebergTableMetadata &metadata,
                                   const std::function<bool(const IcebergSnapshot &)> &include) {
	SnapshotFiles result;
	auto &fs = FileSystem::GetFileSystem(context);
	for (auto &entry : metadata.snapshots) {
		auto &snapshot = entry.second;
		if (!include(snapshot)) {
			continue;
		}
		if (!snapshot.manifest_list.empty()) {
			result.manifest_lists.insert(snapshot.manifest_list);
		}
		IcebergSnapshotScanInfo snapshot_info;
		snapshot_info.snapshot = &snapshot;
		snapshot_info.schema_id = snapshot.GetSchemaId();
		vector<IcebergManifestListEntry> manifest_list;
		IcebergManifestList::LoadManifestFiles(snapshot_info, metadata, context, manifest_list);

		// only read the entries of the manifests no earlier snapshot already had
		vector<IcebergManifestListEntry> new_manifests;
		for (auto &manifest : manifest_list) {
			if (result.manifests.insert(manifest.GetFile().manifest_path).second) {
				new_manifests.push_back(std::move(manifest));
			}
		}
		if (new_manifests.empty()) {
			continue;
		}
		auto scan = AvroScan::ScanManifest(snapshot_info, new_manifests, IcebergOptions(), fs, metadata.GetLocation(),
		                                   metadata, context);
		manifest_file::ManifestReader reader(*scan);
		while (!reader.Finished()) {
			reader.Read();
		}
		for (auto &manifest : new_manifests) {
			for (auto &manifest_entry : manifest.GetManifestEntries()) {
				auto &data_file = manifest_entry.data_file;
				result.listed_files.emplace(data_file.file_path, data_file.content);
				if (manifest_entry.status != IcebergManifestEntryStatusType::DELETED) {
					result.live_files.insert(data_file.file_path);
				}
			}
		}
	}
	return result;
}

} // namespace duckdb
