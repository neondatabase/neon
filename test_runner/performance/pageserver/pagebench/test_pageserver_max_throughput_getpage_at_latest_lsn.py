import json
from pathlib import Path
from typing import Any, Dict, Tuple

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.utils import get_scale_for_db, humantime_to_ms

from performance.pageserver.util import (
    setup_pageserver_with_tenants,
)


# For reference, the space usage of the snapshots:
# admin@ip-172-31-13-23:[~/neon-main]: sudo du -hs /instance_store/test_output/shared-snapshots
# 137G    /instance_store/test_output/shared-snapshots
# admin@ip-172-31-13-23:[~/neon-main]: sudo du -hs /instance_store/test_output/shared-snapshots/*
# 1.8G    /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-1-13
# 1.1G    /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-1-6
# 8.5G    /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-10-13
# 5.1G    /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-10-6
# 76G     /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-100-13
# 46G     /instance_store/test_output/shared-snapshots/max_throughput_latest_lsn-100-6
@pytest.mark.parametrize("duration", [30])
@pytest.mark.parametrize("pgbench_scale", [get_scale_for_db(s) for s in [100, 200]])
@pytest.mark.parametrize("n_tenants", [1, 10])
@pytest.mark.timeout(
    10000
)  # TODO: this value is just "a really high number"; have this per instance type
def test_pageserver_max_throughput_getpage_at_latest_lsn(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    n_tenants: int,
    pgbench_scale: int,
    duration: int,
):
    def record(metric, **kwargs):
        zenbenchmark.record(
            metric_name=f"pageserver_max_throughput_getpage_at_latest_lsn.{metric}", **kwargs
        )

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
        f"max_throughput_latest_lsn-{n_tenants}-{pgbench_scale}",
        n_tenants,
        setup_wrapper,
    )

    env.pageserver.allowed_errors.append(
        # https://github.com/neondatabase/neon/issues/6925
        # https://github.com/neondatabase/neon/issues/6390
        # https://github.com/neondatabase/neon/issues/6724
        r".*query handler for.*pagestream.*failed: unexpected message: CopyFail during COPY.*"
    )

    run_benchmark_max_throughput_latest_lsn(env, pg_bin, record, duration)


def setup_tenant_template(env: NeonEnv, pg_bin: PgBin, scale: int):
    """
    Set up a template tenant which will be replicated by the test infra.
    It's a pgbench tenant, initialized to a certain scale, and treated afterwards
    with a repeat application of (pgbench simple-update workload, checkpoint, compact).
    """
    # use a config that makes production of on-disk state timing-insensitive
    # as we ingest data into the tenant.
    config = {
        "gc_period": "0s",  # disable periodic gc
        "checkpoint_timeout": "10 years",
        "compaction_period": "0s",  # disable periodic compaction
        "compaction_threshold": 10,
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 3,
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
        for _ in range(
            0, 17
        ):  # some prime number to avoid potential resonances with the "_threshold" variables from the config
            # the L0s produced by this appear to have size ~5MiB
            num_txns = 10_000
            pg_bin.run_capture(
                ["pgbench", "-N", "-c1", "--transactions", f"{num_txns}", ep.connstr()]
            )
            wait_for_last_flush_lsn(env, ep, template_tenant, template_timeline)
            ps_http.timeline_checkpoint(template_tenant, template_timeline)
            ps_http.timeline_compact(template_tenant, template_timeline)
    # for reference, the output at scale=6 looked like so (306M total)
    # ls -sh test_output/shared-snapshots/max_throughput_latest_lsn-2-6/snapshot/pageserver_1/tenants/35c30b88ea16a7a09f82d9c6a115551b/timelines/da902b378eebe83dc8a4e81cd3dc1c59
    # total 306M
    # 188M 000000000000000000000000000000000000-030000000000000000000000000000000003__000000000149F060-0000000009E75829
    # 4.5M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000009E75829-000000000A21E919
    #  33M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000A21E919-000000000C20CB71
    #  36M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000C20CB71-000000000E470791
    #  16M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000E470791-000000000F34AEF1
    # 8.2M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000F34AEF1-000000000FABA8A9
    # 6.0M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000FABA8A9-000000000FFE0639
    # 6.1M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000000FFE0639-000000001051D799
    # 4.7M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__000000001051D799-0000000010908F19
    # 4.6M 000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000010908F19-0000000010CD3021

    return (template_tenant, template_timeline, config)


def run_benchmark_max_throughput_latest_lsn(
    env: NeonEnv, pg_bin: PgBin, record, duration_secs: int
):
    """
    Benchmark `env.pageserver` for max throughput @ latest LSN and record results in `zenbenchmark`.
    """

    ps_http = env.pageserver.http_client()
    cmd = [
        str(env.neon_binpath / "pagebench"),
        "get-page-latest-lsn",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--runtime",
        f"{duration_secs}s",
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

    env.storage_controller.allowed_errors.append(
        # The test setup swaps NeonEnv instances, hence different
        # pg instances are used for the storage controller db. This means
        # the storage controller doesn't know about the nodes mentioned
        # in attachments.json at start-up.
        ".* Scheduler missing node 1",
    )
