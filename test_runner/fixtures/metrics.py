from dataclasses import dataclass
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.samples import Sample
from typing import Dict, List
from collections import defaultdict

from fixtures.log_helper import log


class Metrics:
    metrics: Dict[str, List[Sample]]
    name: str

    def __init__(self, name: str = ""):
        self.metrics = defaultdict(list)
        self.name = name

    def query_all(self, name: str, filter: Dict[str, str]) -> List[Sample]:
        res = []
        for sample in self.metrics[name]:
            if all(sample.labels[k] == v for k, v in filter.items()):
                res.append(sample)
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
