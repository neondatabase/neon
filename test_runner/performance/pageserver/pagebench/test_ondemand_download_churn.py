from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    flush_ep_to_pageserver,
)
from fixtures.remote_storage import s3_storage
from fixtures.utils import humantime_to_ms

if TYPE_CHECKING:
    from typing import Any


@pytest.mark.parametrize("duration", [30])
@pytest.mark.parametrize("io_engine", ["tokio-epoll-uring", "std-fs"])
@pytest.mark.parametrize("concurrency_per_target", [1, 10, 100])
@pytest.mark.timeout(1000)
def test_download_churn(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    io_engine: str,
    concurrency_per_target: int,
    duration: int,
):
    def record(metric, **kwargs):
        zenbenchmark.record(metric_name=f"pageserver_ondemand_download_churn.{metric}", **kwargs)

    params: dict[str, tuple[Any, dict[str, Any]]] = {}

    # params from fixtures
    params.update(
        {
            # we don't capture `duration`, but instead use the `runtime` output field from pagebench
        }
    )

    # configure cache sizes like in prod
    page_cache_size = 16384
    max_file_descriptors = 500000
    neon_env_builder.pageserver_config_override = (
        f"page_cache_size={page_cache_size}; max_file_descriptors={max_file_descriptors}"
    )
    params.update(
        {
            "pageserver_config_override.page_cache_size": (
                page_cache_size * 8192,
                {"unit": "byte"},
            ),
            "pageserver_config_override.max_file_descriptors": (max_file_descriptors, {"unit": ""}),
        }
    )

    for param, (value, kwargs) in params.items():
        record(param, metric_value=value, report=MetricReport.TEST_PARAM, **kwargs)

    # Setup env
    env = setup_env(neon_env_builder, pg_bin)
    env.pageserver.allowed_errors.append(
        f".*path=/v1/tenant/{env.initial_tenant}/timeline.* request was dropped before completing"
    )

    run_benchmark(env, pg_bin, record, io_engine, concurrency_per_target, duration)


def setup_env(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # We configure tenant conf such that SQL query below produces a lot of layers.
    # We don't care what's in the layers really, we just care that layers are created.
    bytes_per_layer = 10 * (1024**2)
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "pitr_interval": "1000d",  # let's not make it get in the way
            "gc_period": "0s",  # disable periodic gc to avoid noise
            "compaction_period": "0s",  # disable L0=>L1 compaction
            "checkpoint_timeout": "10years",  # rely solely on checkpoint_distance
            "checkpoint_distance": bytes_per_layer,  # 10M instead of 256M to create more smaller layers
            "image_creation_threshold": 100000,  # don't create image layers ever
        }
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as ep:
        ep.safe_psql("CREATE TABLE data (random_text text)")
        bytes_per_row = 512  # make big enough so WAL record size doesn't dominate
        desired_layers = 300
        desired_bytes = bytes_per_layer * desired_layers
        nrows = desired_bytes / bytes_per_row
        ep.safe_psql(
            f"INSERT INTO data SELECT lpad(i::text, {bytes_per_row}, '0') FROM generate_series(1, {int(nrows)})  as i",
            options="-c statement_timeout=0",
        )
        flush_ep_to_pageserver(env, ep, tenant_id, timeline_id)

    client.timeline_checkpoint(tenant_id, timeline_id, compact=False, wait_until_uploaded=True)

    return env


def run_benchmark(
    env: NeonEnv,
    pg_bin: PgBin,
    record,
    io_engine: str,
    concurrency_per_target: int,
    duration_secs: int,
):
    ps_http = env.pageserver.http_client()
    cmd = [
        str(env.neon_binpath / "pagebench"),
        "ondemand-download-churn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--runtime",
        f"{duration_secs}s",
        "--set-io-engine",
        f"{io_engine}",
        "--concurrency-per-target",
        f"{concurrency_per_target}",
        # don't specify the targets explicitly, let pagebench auto-discover them
    ]

    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    with open(results_path) as f:
        results = json.load(f)
    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    metric = "downloads_count"
    record(
        metric,
        metric_value=results[metric],
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )

    metric = "downloads_bytes"
    record(
        metric,
        metric_value=results[metric],
        unit="byte",
        report=MetricReport.HIGHER_IS_BETTER,
    )

    metric = "evictions_count"
    record(
        metric,
        metric_value=results[metric],
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )

    metric = "timeline_restarts"
    record(
        metric,
        metric_value=results[metric],
        unit="",
        report=MetricReport.LOWER_IS_BETTER,
    )

    metric = "runtime"
    record(
        metric,
        metric_value=humantime_to_ms(results[metric]) / 1000,
        unit="s",
        report=MetricReport.TEST_PARAM,
    )
