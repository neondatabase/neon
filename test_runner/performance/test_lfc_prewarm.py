from __future__ import annotations

import os
import timeit
import traceback
from concurrent.futures import ThreadPoolExecutor as Exec
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Any, cast

import pytest
from fixtures.benchmark_fixture import NeonBenchmarker, PgBenchRunResult
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI, connection_parameters_to_env

if TYPE_CHECKING:
    from fixtures.compare_fixtures import NeonCompare
    from fixtures.neon_fixtures import Endpoint, PgBin
    from fixtures.pg_version import PgVersion

from performance.test_perf_pgbench import utc_now_timestamp

# These tests compare performance for a write-heavy and read-heavy workloads of an ordinary endpoint
# compared to the endpoint which saves its LFC and prewarms using it on startup.


def test_compare_prewarmed_pgbench_perf(neon_compare: NeonCompare):
    env = neon_compare.env
    env.create_branch("normal")
    env.create_branch("prewarmed")
    pg_bin = neon_compare.pg_bin
    ep_normal: Endpoint = env.endpoints.create_start("normal")
    ep_prewarmed: Endpoint = env.endpoints.create_start("prewarmed", autoprewarm=True)

    for ep in [ep_normal, ep_prewarmed]:
        connstr: str = ep.connstr()
        pg_bin.run(["pgbench", "-i", "-I", "dtGvp", connstr, "-s100"])
        ep.safe_psql("CREATE EXTENSION neon")
        client = ep.http_client()
        client.offload_lfc()
        ep.stop()
        ep.start()
        client.prewarm_lfc_wait()

        run_start_timestamp = utc_now_timestamp()
        t0 = timeit.default_timer()
        out = pg_bin.run_capture(["pgbench", "-c10", "-T10", connstr])
        run_duration = timeit.default_timer() - t0
        run_end_timestamp = utc_now_timestamp()

        stdout = Path(f"{out}.stdout").read_text()
        res = PgBenchRunResult.parse_from_stdout(
            stdout=stdout,
            run_duration=run_duration,
            run_start_timestamp=run_start_timestamp,
            run_end_timestamp=run_end_timestamp,
        )
        name: str = cast("str", ep.branch_name)
        neon_compare.zenbenchmark.record_pg_bench_result(name, res)


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_compare_prewarmed_pgbench_perf_benchmark(
    pg_bin: PgBin,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    name = f"Test prewarmed pgbench performance, GITHUB_RUN_ID={os.getenv('GITHUB_RUN_ID')}"
    project = neon_api.create_project(pg_version, name)
    project_id = project["project"]["id"]
    neon_api.wait_for_operation_to_finish(project_id)
    err = False
    try:
        benchmark_impl(pg_bin, neon_api, project, zenbenchmark)
    except Exception as e:
        err = True
        log.error(f"Caught exception: {e}")
        log.error(traceback.format_exc())
    finally:
        assert not err
        neon_api.delete_project(project_id)


def benchmark_impl(
    pg_bin: PgBin, neon_api: NeonAPI, project: dict[str, Any], zenbenchmark: NeonBenchmarker
):
    pgbench_size = int(os.getenv("PGBENCH_SIZE") or "3424")  # 50GB
    offload_secs = 20
    test_duration_min = 5
    pgbench_duration = f"-T{test_duration_min * 60}"
    # prewarm API is not publicly exposed. In order to test performance of a
    # fully prewarmed endpoint, wait after it restarts.
    # The number here is empirical, based on manual runs on staging
    prewarmed_sleep_secs = 180

    branch_id = project["branch"]["id"]
    project_id = project["project"]["id"]
    normal_env = connection_parameters_to_env(
        project["connection_uris"][0]["connection_parameters"]
    )
    normal_id = project["endpoints"][0]["id"]

    prewarmed_branch_id = neon_api.create_branch(
        project_id, "prewarmed", parent_id=branch_id, add_endpoint=False
    )["branch"]["id"]
    neon_api.wait_for_operation_to_finish(project_id)

    ep_prewarmed = neon_api.create_endpoint(
        project_id,
        prewarmed_branch_id,
        endpoint_type="read_write",
        settings={"autoprewarm": True, "offload_lfc_interval_seconds": offload_secs},
    )
    neon_api.wait_for_operation_to_finish(project_id)

    prewarmed_env = normal_env.copy()
    prewarmed_env["PGHOST"] = ep_prewarmed["endpoint"]["host"]
    prewarmed_id = ep_prewarmed["endpoint"]["id"]

    def bench(endpoint_name, endpoint_id, env):
        pg_bin.run(["pgbench", "-i", "-I", "dtGvp", f"-s{pgbench_size}"], env)

        if endpoint_name == "prewarmed":
            sleep(offload_secs * 2)  # ensure LFC is offloaded after pgbench finishes
            # omitting offload_lfc_interval_seconds makes endpoint not offload
            neon_api.update_endpoint(project_id, endpoint_id, {"autoprewarm": True})

        neon_api.restart_endpoint(project_id, endpoint_id)
        sleep(prewarmed_sleep_secs)

        run_start_timestamp = utc_now_timestamp()
        t0 = timeit.default_timer()
        out = pg_bin.run_capture(["pgbench", "-c10", pgbench_duration, "-Mprepared"], env)
        run_duration = timeit.default_timer() - t0
        run_end_timestamp = utc_now_timestamp()

        stdout = Path(f"{out}.stdout").read_text()
        res = PgBenchRunResult.parse_from_stdout(
            stdout=stdout,
            run_duration=run_duration,
            run_start_timestamp=run_start_timestamp,
            run_end_timestamp=run_end_timestamp,
        )
        zenbenchmark.record_pg_bench_result(endpoint_name, res)

    with Exec(max_workers=2) as exe:
        exe.submit(bench, "normal", normal_id, normal_env)
        exe.submit(bench, "prewarmed", prewarmed_id, prewarmed_env)


def test_compare_prewarmed_read_perf(neon_compare: NeonCompare):
    env = neon_compare.env
    env.create_branch("normal")
    env.create_branch("prewarmed")
    ep_normal: Endpoint = env.endpoints.create_start("normal")
    ep_prewarmed: Endpoint = env.endpoints.create_start("prewarmed", autoprewarm=True)

    sql = [
        "CREATE EXTENSION neon",
        "CREATE TABLE foo(key serial primary key, t text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')",
        "INSERT INTO foo SELECT FROM generate_series(1,1000000)",
    ]
    for ep in [ep_normal, ep_prewarmed]:
        ep.safe_psql_many(sql)
        client = ep.http_client()
        client.offload_lfc()
        ep.stop()
        ep.start()
        client.prewarm_lfc_wait()
        with neon_compare.record_duration(f"{ep.branch_name}_run_duration"):
            ep.safe_psql("SELECT count(*) from foo")
