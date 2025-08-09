#!/usr/bin/env python3

import argparse
import json
import time
from typing import Any, TypedDict, cast

import requests

PROMETHEUS_URL = "http://localhost:9090"
SAMPLE_INTERVAL = 1  # seconds

DEFAULT_REVISION = "unknown"
DEFAULT_PLATFORM = "unknown"
DEFAULT_SUIT = "proxy_bench"


class MetricConfig(TypedDict, total=False):
    name: str
    promql: str
    unit: str
    report: str
    labels: dict[str, str]
    is_vector: bool
    label_field: str


METRICS: list[MetricConfig] = [
    {
        "name": "latency_p99",
        "promql": 'histogram_quantile(0.99, sum(rate(proxy_compute_connection_latency_seconds_bucket{outcome="success", excluded="client_and_cplane"}[5m])) by (le))',
        "unit": "s",
        "report": "LOWER_IS_BETTER",
        "labels": {},
    },
    {
        "name": "error_rate",
        "promql": 'sum(rate(proxy_errors_total{type!~"user|clientdisconnect|quota"}[5m])) / sum(rate(proxy_accepted_connections_total[5m]))',
        "unit": "",
        "report": "LOWER_IS_BETTER",
        "labels": {},
    },
    {
        "name": "max_memory_kb",
        "promql": "max(libmetrics_maxrss_kb)",
        "unit": "kB",
        "report": "LOWER_IS_BETTER",
        "labels": {},
    },
    {
        "name": "jemalloc_active_bytes",
        "promql": "sum(jemalloc_active_bytes)",
        "unit": "bytes",
        "report": "LOWER_IS_BETTER",
        "labels": {},
    },
    {
        "name": "open_connections",
        "promql": "sum by (protocol) (proxy_opened_client_connections_total - proxy_closed_client_connections_total)",
        "unit": "",
        "report": "HIGHER_IS_BETTER",
        "labels": {},
        "is_vector": True,
        "label_field": "protocol",
    },
]


class PrometheusMetric(TypedDict):
    metric: dict[str, str]
    value: list[str | float]


class PrometheusResult(TypedDict):
    result: list[PrometheusMetric]


class PrometheusResponse(TypedDict):
    data: PrometheusResult


def query_prometheus(promql: str) -> PrometheusResponse:
    resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": promql})
    resp.raise_for_status()
    return cast("PrometheusResponse", resp.json())


def extract_scalar_metric(result_json: PrometheusResponse) -> float | None:
    try:
        return float(result_json["data"]["result"][0]["value"][1])
    except (IndexError, KeyError, ValueError, TypeError):
        return None


def extract_vector_metric(
    result_json: PrometheusResponse, label_field: str
) -> list[tuple[str | None, float, dict[str, str]]]:
    out: list[tuple[str | None, float, dict[str, str]]] = []
    for entry in result_json["data"]["result"]:
        try:
            value_str = entry["value"][1]
            if not isinstance(value_str, (str | float)):
                continue
            value = float(value_str)
        except (IndexError, KeyError, ValueError, TypeError):
            continue
        labels = entry.get("metric", {})
        label_val = labels.get(label_field, None)
        out.append((label_val, value, labels))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect Prometheus metrics and output in benchmark fixture format"
    )
    parser.add_argument("--revision", default=DEFAULT_REVISION)
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--suit", default=DEFAULT_SUIT)
    parser.add_argument("--out", default="metrics_benchmarks.json", help="Output JSON file")
    parser.add_argument(
        "--interval", default=SAMPLE_INTERVAL, type=int, help="Sampling interval (s)"
    )
    args = parser.parse_args()

    start_time = int(time.time())
    samples: list[dict[str, Any]] = []

    print("Collecting metrics (Ctrl+C to stop)...")
    try:
        while True:
            ts = int(time.time())
            for metric in METRICS:
                if metric.get("is_vector", False):
                    # Vector (per-label, e.g. per-protocol)
                    for label_val, value, labels in extract_vector_metric(
                        query_prometheus(metric["promql"]), metric["label_field"]
                    ):
                        entry = {
                            "name": f"{metric['name']}.{label_val}"
                            if label_val
                            else metric["name"],
                            "value": value,
                            "unit": metric["unit"],
                            "report": metric["report"],
                            "labels": {**metric.get("labels", {}), **labels},
                            "timestamp": ts,
                        }
                        samples.append(entry)
                else:
                    result = extract_scalar_metric(query_prometheus(metric["promql"]))
                    if result is not None:
                        entry = {
                            "name": metric["name"],
                            "value": result,
                            "unit": metric["unit"],
                            "report": metric["report"],
                            "labels": metric.get("labels", {}),
                            "timestamp": ts,
                        }
                        samples.append(entry)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Collection stopped.")

    total_duration = int(time.time()) - start_time

    # Compose output
    out = {
        "revision": args.revision,
        "platform": args.platform,
        "result": [
            {
                "suit": args.suit,
                "total_duration": total_duration,
                "data": samples,
            }
        ],
    }

    with open(args.out, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Wrote metrics in fixture format to {args.out}")


if __name__ == "__main__":
    main()
