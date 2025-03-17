from __future__ import annotations

import datetime

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.neon_fixtures import NeonEnv


@pytest.mark.timeout(120)
def test_compute_ctl_api_latencies(
    neon_simple_env: NeonEnv,
    zenbenchmark: NeonBenchmarker,
):
    """
    Test compute_ctl HTTP API performance. Do simple GET requests
    to catch any pathological degradations in the HTTP server.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")
    client = endpoint.http_client()

    NUM_REQUESTS = 10000

    status_response_latency_us = []
    metrics_response_latency_us = []

    for _i in range(NUM_REQUESTS):
        start_time = datetime.datetime.now()
        _ = client.status()
        status_response_latency_us.append((datetime.datetime.now() - start_time).microseconds)

        start_time = datetime.datetime.now()
        _ = client.metrics_json()
        metrics_response_latency_us.append((datetime.datetime.now() - start_time).microseconds)

    status_response_latency_us = sorted(status_response_latency_us)
    metrics_response_latency_us = sorted(metrics_response_latency_us)

    zenbenchmark.record(
        "status_response_latency_p50_us",
        status_response_latency_us[len(status_response_latency_us) // 2],
        "μs",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "metrics_response_latency_p50_us",
        metrics_response_latency_us[len(metrics_response_latency_us) // 2],
        "μs",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "status_response_latency_p99_us",
        status_response_latency_us[len(status_response_latency_us) * 99 // 100],
        "μs",
        MetricReport.LOWER_IS_BETTER,
    )
    zenbenchmark.record(
        "metrics_response_latency_p99_us",
        metrics_response_latency_us[len(metrics_response_latency_us) * 99 // 100],
        "μs",
        MetricReport.LOWER_IS_BETTER,
    )
