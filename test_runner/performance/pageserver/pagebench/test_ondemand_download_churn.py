import json
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.utils import humantime_to_ms

from performance.pageserver.util import (
    setup_pageserver_with_tenants,
)


@pytest.mark.parametrize("duration", [30])
@pytest.mark.parametrize("pgbench_scale", [200])
@pytest.mark.parametrize("io_engine", ["tokio-epoll-uring"])
@pytest.mark.parametrize("concurrency_per_target", [1])
@pytest.mark.parametrize("n_tenants", [1])
@pytest.mark.timeout(1000)
def test_download_churn(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    n_tenants: int,
    pgbench_scale: int,
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
            "n_tenants": (n_tenants, {"unit": ""}),
            "pgbench_scale": (pgbench_scale, {"unit": ""}),
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

    def setup_wrapper(env: NeonEnv):
        return setup_tenant_template(env, pg_bin, pgbench_scale)

    env = setup_pageserver_with_tenants(
        neon_env_builder,
        f"download_churn-{n_tenants}-{pgbench_scale}",
        n_tenants,
        setup_wrapper,
    )
    run_benchmark(env, pg_bin, record, io_engine, concurrency_per_target, duration)


def setup_tenant_template(env: NeonEnv, pg_bin: PgBin, scale: int):
    config = {
        "gc_period": "0s",  # disable periodic gc
        "checkpoint_timeout": "10 years",
        "compaction_period": "0s",  # disable periodic compaction
        "checkpoint_distance": 10485760,
        "image_creation_threshold": 1000,
        "compaction_threshold": 1,
        "pitr_interval": "1000d",
    }

    template_tenant, template_timeline = env.neon_cli.create_tenant(set_default=True)
    env.pageserver.tenant_detach(template_tenant)
    env.pageserver.allowed_errors.append(
        # tenant detach causes this because the underlying attach-hook removes the tenant from storage controller entirely
        ".*Dropped remote consistent LSN updates.*",
    )
    env.pageserver.tenant_attach(template_tenant, config)
    ps_http = env.pageserver.http_client()

    with env.endpoints.create_start("main", tenant_id=template_tenant) as ep:
        pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", "-I", "dtGvp", ep.connstr()])
        wait_for_last_flush_lsn(env, ep, template_tenant, template_timeline)
        ps_http.timeline_checkpoint(template_tenant, template_timeline)
        ps_http.timeline_compact(template_tenant, template_timeline)

    return (template_tenant, template_timeline, config)


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

    total = results["total"]

    metric = "request_count"
    record(
        metric,
        metric_value=total[metric],
        unit="",
        report=MetricReport.HIGHER_IS_BETTER,
    )

    metric = "latency_mean"
    record(
        metric,
        metric_value=humantime_to_ms(total[metric]),
        unit="ms",
        report=MetricReport.LOWER_IS_BETTER,
    )

    metric = "latency_percentiles"
    for k, v in total[metric].items():
        record(
            f"{metric}.{k}",
            metric_value=humantime_to_ms(v),
            unit="ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
