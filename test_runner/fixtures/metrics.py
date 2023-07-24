from collections import defaultdict
from typing import Dict, List, Optional, Tuple

from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample


class Metrics:
    metrics: Dict[str, List[Sample]]
    name: str

    def __init__(self, name: str = ""):
        self.metrics = defaultdict(list)
        self.name = name

    def query_all(self, name: str, filter: Optional[Dict[str, str]] = None) -> List[Sample]:
        filter = filter or {}
        res = []
        for sample in self.metrics[name]:
            try:
                if all(sample.labels[k] == v for k, v in filter.items()):
                    res.append(sample)
            except KeyError:
                pass
        return res

    def query_one(self, name: str, filter: Optional[Dict[str, str]] = None) -> Sample:
        res = self.query_all(name, filter or {})
        assert len(res) == 1, f"expected single sample for {name} {filter}, found {res}"
        return res[0]


def parse_metrics(text: str, name: str = "") -> Metrics:
    metrics = Metrics(name)
    gen = text_string_to_metric_families(text)
    for family in gen:
        for sample in family.samples:
            metrics.metrics[sample.name].append(sample)

    return metrics


def histogram(prefix_without_trailing_underscore: str) -> List[str]:
    assert not prefix_without_trailing_underscore.endswith("_")
    return [f"{prefix_without_trailing_underscore}_{x}" for x in ["bucket", "count", "sum"]]


PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS: Tuple[str, ...] = (
    "pageserver_remote_timeline_client_calls_unfinished",
    "pageserver_remote_physical_size",
    "pageserver_remote_timeline_client_bytes_started_total",
    "pageserver_remote_timeline_client_bytes_finished_total",
)

PAGESERVER_GLOBAL_METRICS: Tuple[str, ...] = (
    "pageserver_storage_operations_seconds_global_count",
    "pageserver_storage_operations_seconds_global_sum",
    "pageserver_storage_operations_seconds_global_bucket",
    "pageserver_unexpected_ondemand_downloads_count_total",
    "libmetrics_launch_timestamp",
    "libmetrics_build_info",
    "libmetrics_tracing_event_count_total",
    "pageserver_materialized_cache_hits_total",
    "pageserver_materialized_cache_hits_direct_total",
    "pageserver_page_cache_read_hits_total",
    "pageserver_page_cache_read_accesses_total",
    "pageserver_page_cache_size_current_bytes",
    "pageserver_page_cache_size_max_bytes",
    "pageserver_getpage_reconstruct_seconds_bucket",
    "pageserver_getpage_reconstruct_seconds_count",
    "pageserver_getpage_reconstruct_seconds_sum",
    *[f"pageserver_basebackup_query_seconds_{x}" for x in ["bucket", "count", "sum"]],
    *histogram("pageserver_read_num_fs_layers"),
    *histogram("pageserver_getpage_get_reconstruct_data_seconds"),
    *histogram("pageserver_wait_lsn_seconds"),
    *histogram("pageserver_remote_operation_seconds"),
    *histogram("pageserver_remote_timeline_client_calls_started"),
    *histogram("pageserver_io_operations_seconds"),
)

PAGESERVER_PER_TENANT_METRICS: Tuple[str, ...] = (
    "pageserver_current_logical_size",
    "pageserver_resident_physical_size",
    "pageserver_io_operations_bytes_total",
    "pageserver_last_record_lsn",
    "pageserver_smgr_query_seconds_bucket",
    "pageserver_smgr_query_seconds_count",
    "pageserver_smgr_query_seconds_sum",
    "pageserver_storage_operations_seconds_count_total",
    "pageserver_storage_operations_seconds_sum_total",
    "pageserver_created_persistent_files_total",
    "pageserver_written_persistent_bytes_total",
    "pageserver_tenant_states_count",
    "pageserver_evictions_total",
    "pageserver_evictions_with_low_residence_duration_total",
    *PAGESERVER_PER_TENANT_REMOTE_TIMELINE_CLIENT_METRICS,
)
