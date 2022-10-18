from collections import defaultdict
from typing import Dict, List

from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample


class Metrics:
    metrics: Dict[str, List[Sample]]
    name: str

    def __init__(self, name: str = ""):
        self.metrics = defaultdict(list)
        self.name = name

    def query_all(self, name: str, filter: Dict[str, str]) -> List[Sample]:
        res = []
        for sample in self.metrics[name]:
            try:
                if all(sample.labels[k] == v for k, v in filter.items()):
                    res.append(sample)
            except KeyError:
                pass
        return res

    def query_one(self, name: str, filter: Dict[str, str] = {}) -> Sample:
        res = self.query_all(name, filter)
        assert len(res) == 1, f"expected single sample for {name} {filter}, found {res}"
        return res[0]


def parse_metrics(text: str, name: str = ""):
    metrics = Metrics(name)
    gen = text_string_to_metric_families(text)
    for family in gen:
        for sample in family.samples:
            metrics.metrics[sample.name].append(sample)

    return metrics


PAGESERVER_PER_TENANT_METRICS = [
    "pageserver_current_logical_size",
    "pageserver_current_physical_size",
    "pageserver_getpage_reconstruct_seconds_bucket",
    "pageserver_getpage_reconstruct_seconds_count",
    "pageserver_getpage_reconstruct_seconds_sum",
    "pageserver_io_operations_bytes_total",
    "pageserver_io_operations_seconds_bucket",
    "pageserver_io_operations_seconds_count",
    "pageserver_io_operations_seconds_sum",
    "pageserver_last_record_lsn",
    "pageserver_materialized_cache_hits_total",
    "pageserver_smgr_query_seconds_bucket",
    "pageserver_smgr_query_seconds_count",
    "pageserver_smgr_query_seconds_sum",
    "pageserver_storage_operations_seconds_bucket",
    "pageserver_storage_operations_seconds_count",
    "pageserver_storage_operations_seconds_sum",
    "pageserver_wait_lsn_seconds_bucket",
    "pageserver_wait_lsn_seconds_count",
    "pageserver_wait_lsn_seconds_sum",
    "pageserver_created_persistent_files_total",
    "pageserver_written_persistent_bytes_total",
]
