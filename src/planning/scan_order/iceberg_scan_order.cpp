#include "planning/scan_order/iceberg_scan_order.hpp"

#include "core/expression/iceberg_predicate_stats.hpp"
#include "core/metadata/schema/iceberg_table_schema.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/storage/table/row_group_reorderer.hpp"
#include "iceberg_logging.hpp"

#include <algorithm>

namespace duckdb {

namespace {

static bool ScanOrderCompare(const Value &v1, const Value &v2, OrderByStatistics stat_type) {
	return (stat_type == OrderByStatistics::MAX && v1 < v2) || (stat_type == OrderByStatistics::MIN && v1 > v2);
}

struct IcebergOrderEntry {
	idx_t entry_idx;
	Value lower;
	Value upper;
	idx_t count;
};

} // namespace

IcebergScanOrder::IcebergScanOrder() {
}

IcebergScanOrder::~IcebergScanOrder() {
}

void IcebergScanOrder::Set(unique_ptr<RowGroupOrderOptions> new_options) {
	options = std::move(new_options);
	applied = false;
}

unique_ptr<RowGroupOrderOptions> IcebergScanOrder::CopyOptions() const {
	return options ? make_uniq<RowGroupOrderOptions>(*options) : nullptr;
}

optional_ptr<const RowGroupOrderOptions> IcebergScanOrder::GetOptions() const {
	return options.get();
}

bool IcebergScanOrder::IsPending() const {
	return options && !applied;
}

void IcebergScanOrder::Apply(ClientContext &context, const IcebergTableSchema &schema,
                             bool has_matching_delete_manifests, vector<BoundIcebergManifestEntry> &manifest_entries) {
	if (!options || applied) {
		return;
	}
	applied = true;

	auto &opts = *options;
	if (opts.column_type != OrderByColumnType::NUMERIC || opts.column_idx.HasChildren()) {
		return;
	}
	if (manifest_entries.size() <= 1) {
		return;
	}

	auto &schema_columns = schema.columns;
	auto schema_idx = opts.column_idx.GetPrimaryIndex();
	if (schema_idx >= schema_columns.size()) {
		return;
	}
	auto &order_column = *schema_columns[schema_idx];
	auto field_id = order_column.id;

	const bool nulls_first = opts.null_order == OrderByNullType::NULLS_FIRST;
	bool can_prune = opts.row_limit.IsValid();
	//! Files are ordered like DuckDB's RowGroupReorderer orders row groups: files whose order column is all NULL,
	//! files without usable bounds and, with NULLS FIRST, files that may hold NULLs are scanned before (NULLS FIRST) or
	//! after (NULLS LAST) the files ordered by their bounds
	vector<idx_t> null_only_entries;
	vector<idx_t> unordered_entries;
	vector<IcebergOrderEntry> order_entries;
	order_entries.reserve(manifest_entries.size());
	for (idx_t i = 0; i < manifest_entries.size(); i++) {
		auto &data_file = manifest_entries[i].entry.data_file;
		auto record_count = NumericCast<idx_t>(data_file.record_count);
		optional_idx null_count;
		auto null_it = data_file.null_value_counts.find(field_id);
		if (null_it != data_file.null_value_counts.end() && null_it->second >= 0) {
			null_count = NumericCast<idx_t>(null_it->second);
		}
		if (null_count.IsValid() && null_count.GetIndex() >= record_count) {
			null_only_entries.push_back(i);
			continue;
		}
		auto lower_it = data_file.lower_bounds.find(field_id);
		auto upper_it = data_file.upper_bounds.find(field_id);
		if (lower_it == data_file.lower_bounds.end() || upper_it == data_file.upper_bounds.end()) {
			unordered_entries.push_back(i);
			continue;
		}
		auto stats = IcebergPredicateStats::DeserializeBounds(context, lower_it->second, upper_it->second,
		                                                      order_column.name, order_column.type);
		if (!stats.lower_bound || !stats.upper_bound || stats.lower_bound->IsNull() || stats.upper_bound->IsNull()) {
			unordered_entries.push_back(i);
			continue;
		}
		const bool may_have_nulls = !null_count.IsValid() || null_count.GetIndex() > 0;
		if (nulls_first && may_have_nulls) {
			unordered_entries.push_back(i);
			continue;
		}
		//! Only non-NULL values count towards the limit. Without a NULL count, the bounds still prove one non-NULL
		//! value, or two when they differ
		idx_t non_null_count;
		if (null_count.IsValid()) {
			non_null_count = record_count - null_count.GetIndex();
		} else {
			non_null_count = *stats.lower_bound == *stats.upper_bound ? 1 : 2;
		}
		order_entries.push_back({i, *stats.lower_bound, *stats.upper_bound, non_null_count});
	}

	if (has_matching_delete_manifests) {
		can_prune = false;
	}

	const auto stat_type = opts.order_by;
	const bool ascending = opts.order_type == OrderType::ASCENDING;
	auto primary = [&](const IcebergOrderEntry &entry) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? entry.upper : entry.lower;
	};
	auto opposite = [&](const IcebergOrderEntry &entry) -> const Value & {
		return stat_type == OrderByStatistics::MAX ? entry.lower : entry.upper;
	};

	std::stable_sort(order_entries.begin(), order_entries.end(),
	                 [&](const IcebergOrderEntry &left, const IcebergOrderEntry &right) {
		                 return ascending ? primary(left) < primary(right) : primary(right) < primary(left);
	                 });

	idx_t keep = order_entries.size();
	if (can_prune && opts.row_limit.GetIndex() > 0) {
		const auto row_limit = opts.row_limit.GetIndex();
		keep = 0;
		for (idx_t k = 0; k < order_entries.size(); k++) {
			const auto &frontier = primary(order_entries[k]);
			idx_t guaranteed = 0;
			for (idx_t j = 0; j < k; j++) {
				if (!ScanOrderCompare(opposite(order_entries[j]), frontier, stat_type)) {
					guaranteed += order_entries[j].count;
				}
				if (guaranteed >= row_limit) {
					break;
				}
			}
			if (guaranteed >= row_limit) {
				break;
			}
			keep = k + 1;
		}
	}

	if (keep < order_entries.size()) {
		DUCKDB_LOG(context, IcebergLogType,
		           "Iceberg Scan Order Pushdown, kept %llu of %llu 'data_file's for ORDER BY LIMIT %llu", keep,
		           order_entries.size(), opts.row_limit.GetIndex());
	}

	const bool pruned_by_limit = keep < order_entries.size();
	vector<BoundIcebergManifestEntry> reordered;
	reordered.reserve(null_only_entries.size() + unordered_entries.size() + keep);
	auto append = [&](const vector<idx_t> &entry_indexes) {
		for (auto entry_idx : entry_indexes) {
			reordered.push_back(manifest_entries[entry_idx]);
		}
	};
	if (nulls_first) {
		append(null_only_entries);
		append(unordered_entries);
	}
	for (idx_t i = 0; i < keep; i++) {
		reordered.push_back(manifest_entries[order_entries[i].entry_idx]);
	}
	if (!nulls_first) {
		append(unordered_entries);
		//! With NULLS LAST, NULLs sort after every value in either direction. Once the ordered files satisfy the limit,
		//! files of only NULLs can't contribute
		if (!pruned_by_limit) {
			append(null_only_entries);
		}
	}
	manifest_entries = std::move(reordered);
}

} // namespace duckdb
