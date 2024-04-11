import json
import os
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.remote_storage import s3_storage


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

    params: Dict[str, Tuple[Any, Dict[str, Any]]] = {}

    # params from fixtures
    params.update(
        {
            "duration": (duration, {"unit": "s"}),
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

    run_benchmark(env, pg_bin, record, io_engine, concurrency_per_target, duration)


def setup_env(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",  # disable periodic gc
            "checkpoint_timeout": "10years",
            "compaction_period": "0s",  # disable periodic compaction
            "checkpoint_distance": 10485760,
            "image_creation_threshold": 1000,
            "compaction_threshold": 1,
            "pitr_interval": "1000d",
        }
    )

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    client = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as ep:
        if os.getenv("CI", "false") == "true":
            pg_bin.run_capture(["pgbench", "-i", "-s200", ep.connstr()])
        wait_for_last_flush_lsn(env, ep, tenant_id, timeline_id)
        client.timeline_checkpoint(tenant_id, timeline_id)
        client.timeline_compact(tenant_id, timeline_id)

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

    with open(results_path, "r") as f:
        results = json.load(f)
    log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")

    metric = "downloads"
    record(
        metric,
        metric_value=results[metric],
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )

    metric = "evictions"
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
