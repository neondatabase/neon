from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.utils import get_scale_for_db, humantime_to_ms

from performance.pageserver.util import (
    setup_pageserver_with_tenants,
)

if TYPE_CHECKING:
    from typing import Any


@pytest.mark.parametrize("duration", [30])
@pytest.mark.parametrize("pgbench_scale", [get_scale_for_db(200)])
@pytest.mark.parametrize("n_tenants", [10])
@pytest.mark.timeout(1000)
def test_basebackup_with_high_slru_count(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    n_tenants: int,
    pgbench_scale: int,
    duration: int,
):
    def record(metric, **kwargs):
        zenbenchmark.record(metric_name=f"pageserver_basebackup.{metric}", **kwargs)

    params: dict[str, tuple[Any, dict[str, Any]]] = {}

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

    n_txns = 500000

    def setup_wrapper(env: NeonEnv):
        return setup_tenant_template(env, n_txns)

    env = setup_pageserver_with_tenants(
        neon_env_builder, f"large_slru_count-{n_tenants}-{n_txns}", n_tenants, setup_wrapper
    )
    run_benchmark(env, pg_bin, record, duration)


def setup_tenant_template(env: NeonEnv, n_txns: int):
    config = {
        "gc_period": "0s",  # disable periodic gc
        "checkpoint_timeout": "10 years",
        "compaction_period": "0s",  # disable periodic compaction
        "compaction_threshold": 10,
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 3,
    }

    template_tenant, template_timeline = env.create_tenant(set_default=True)
    env.pageserver.tenant_detach(template_tenant)
    env.pageserver.tenant_attach(template_tenant, config)

    ps_http = env.pageserver.http_client()

    with env.endpoints.create_start(
        "main", tenant_id=template_tenant, config_lines=["shared_buffers=1MB"]
    ) as ep:
        rels = 10

        asyncio.run(run_updates(ep, n_txns, rels))

        wait_for_last_flush_lsn(env, ep, template_tenant, template_timeline)
        ps_http.timeline_checkpoint(template_tenant, template_timeline)
        ps_http.timeline_compact(template_tenant, template_timeline)

    return (template_tenant, template_timeline, config)


# Takes about 5 minutes and produces tenants with around 300 SLRU blocks
# of 8 KiB each.
async def run_updates(ep: Endpoint, n_txns: int, workers_count: int):
    workers = []
    for i in range(workers_count):
        workers.append(asyncio.create_task(run_update_loop_worker(ep, n_txns, i)))

    await asyncio.gather(*workers)


async def run_update_loop_worker(ep: Endpoint, n_txns: int, idx: int):
    table = f"t_{idx}"
    conn = await ep.connect_async()
    await conn.execute(f"CREATE TABLE {table} (pk integer PRIMARY KEY, x integer)")
    await conn.execute(f"ALTER TABLE {table} SET (autovacuum_enabled = false)")
    await conn.execute(f"INSERT INTO {table} VALUES (1, 0)")
    await conn.execute(
        f"""
        CREATE PROCEDURE updating{table}() as
        $$
            DECLARE
            i integer;
            BEGIN
            FOR i IN 1..{n_txns} LOOP
                UPDATE {table} SET x = x + 1 WHERE pk=1;
                COMMIT;
            END LOOP;
            END
        $$ LANGUAGE plpgsql
        """
    )
    await conn.execute("SET statement_timeout=0")
    await conn.execute(f"call updating{table}()")


def run_benchmark(env: NeonEnv, pg_bin: PgBin, record, duration_secs: int):
    ps_http = env.pageserver.http_client()
    cmd = [
        str(env.neon_binpath / "pagebench"),
        "basebackup",
        "--mgmt-api-endpoint",
        ps_http.base_url,
        "--page-service-connstring",
        env.pageserver.connstr(password=None),
        "--gzip-probability",
        "1",
        "--runtime",
        f"{duration_secs}s",
        # don't specify the targets explicitly, let pagebench auto-discover them
    ]

    log.info(f"command: {' '.join(cmd)}")
    basepath = pg_bin.run_capture(cmd, with_command_header=False)
    results_path = Path(basepath + ".stdout")
    log.info(f"Benchmark results at: {results_path}")

    with open(results_path) as f:
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
