from __future__ import annotations

import os
import timeit
from pathlib import Path
from threading import Thread
from time import sleep
from typing import TYPE_CHECKING, cast

import pytest
from fixtures.benchmark_fixture import NeonBenchmarker, PgBenchRunResult
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI, connstr_to_env

from performance.test_perf_pgbench import utc_now_timestamp

if TYPE_CHECKING:
    from fixtures.compare_fixtures import NeonCompare
    from fixtures.neon_fixtures import Endpoint, PgBin
    from fixtures.pg_version import PgVersion


# These tests compare performance for a write-heavy and read-heavy workloads of an ordinary endpoint
# compared to the endpoint which saves its LFC and prewarms using it on startup


def test_compare_prewarmed_pgbench_perf(neon_compare: NeonCompare):
    env = neon_compare.env
    env.create_branch("prewarmed")
    pg_bin = neon_compare.pg_bin
    ep_ordinary: Endpoint = neon_compare.endpoint
    ep_prewarmed: Endpoint = env.endpoints.create_start("prewarmed")

    for ep in [ep_ordinary, ep_prewarmed]:
        connstr: str = ep.connstr()
        pg_bin.run(["pgbench", "-i", "-I", "dtGvp", connstr, "-s100"])
        ep.safe_psql("CREATE SCHEMA neon; CREATE EXTENSION neon WITH SCHEMA neon")
        if ep == ep_prewarmed:
            client = ep.http_client()
            client.offload_lfc()
            ep.stop()
            ep.start(autoprewarm=True)
            client.prewarm_lfc_wait()
        else:
            ep.stop()
            ep.start()

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


def test_compare_prewarmed_read_perf(neon_compare: NeonCompare):
    env = neon_compare.env
    env.create_branch("prewarmed")
    ep_ordinary: Endpoint = neon_compare.endpoint
    ep_prewarmed: Endpoint = env.endpoints.create_start("prewarmed")

    sql = [
        "CREATE SCHEMA neon",
        "CREATE EXTENSION neon WITH SCHEMA neon",
        "CREATE TABLE foo(key serial primary key, t text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')",
        "INSERT INTO foo SELECT FROM generate_series(1,1000000)",
    ]
    sql_check = "SELECT count(*) from foo"

    ep_ordinary.safe_psql_many(sql)
    ep_ordinary.stop()
    ep_ordinary.start()
    with neon_compare.record_duration("ordinary_run_duration"):
        ep_ordinary.safe_psql(sql_check)

    ep_prewarmed.safe_psql_many(sql)
    client = ep_prewarmed.http_client()
    client.offload_lfc()
    ep_prewarmed.stop()
    ep_prewarmed.start(autoprewarm=True)
    client.prewarm_lfc_wait()
    with neon_compare.record_duration("prewarmed_run_duration"):
        ep_prewarmed.safe_psql(sql_check)


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_compare_prewarmed_pgbench_perf_benchmark(
    pg_bin: PgBin,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    """
    Prewarm API is not public, so this test relies on a pre-created project
    with pgbench size of 3424, pgbench -i -IdtGvp -s3424. Sleeping and
    offloading constants are hardcoded to this size as well
    """
    project_id = os.getenv("PROJECT_ID")
    assert project_id

    ordinary_branch_id = ""
    prewarmed_branch_id = ""
    for branch in neon_api.get_branches(project_id)["branches"]:
        if branch["name"] == "ordinary":
            ordinary_branch_id = branch["id"]
        if branch["name"] == "prewarmed":
            prewarmed_branch_id = branch["id"]
    assert len(ordinary_branch_id) > 0
    assert len(prewarmed_branch_id) > 0

    ep_ordinary = None
    ep_prewarmed = None
    for ep in neon_api.get_endpoints(project_id)["endpoints"]:
        if ep["branch_id"] == ordinary_branch_id:
            ep_ordinary = ep
        if ep["branch_id"] == prewarmed_branch_id:
            ep_prewarmed = ep
    assert ep_ordinary
    assert ep_prewarmed
    ordinary_id = ep_ordinary["id"]
    prewarmed_id = ep_prewarmed["id"]

    offload_secs = 20
    test_duration_min = 5
    pgbench_duration = f"-T{test_duration_min * 60}"
    pgbench_cmd = ["pgbench", "-P10", "-S", "-n", "-c10", pgbench_duration, "-Mprepared"]
    prewarmed_sleep_secs = 180

    ordinary_uri = neon_api.get_connection_uri(project_id, ordinary_branch_id, ordinary_id)["uri"]
    prewarmed_uri = neon_api.get_connection_uri(project_id, prewarmed_branch_id, prewarmed_id)[
        "uri"
    ]

    def bench(endpoint_name, endpoint_id, env):
        log.info(f"Running pgbench for {pgbench_duration}s to warm up the cache")
        pg_bin.run_capture(pgbench_cmd, env)  # capture useful for debugging

        log.info(f"Initialized {endpoint_name}")
        if endpoint_name == "prewarmed":
            log.info(f"sleeping {offload_secs * 2} to ensure LFC is offloaded")
            sleep(offload_secs * 2)
            neon_api.restart_endpoint(project_id, endpoint_id)
            log.info(f"sleeping {prewarmed_sleep_secs} to ensure LFC is prewarmed")
            sleep(prewarmed_sleep_secs)
        else:
            neon_api.restart_endpoint(project_id, endpoint_id)

        log.info(f"Starting benchmark for {endpoint_name}")
        run_start_timestamp = utc_now_timestamp()
        t0 = timeit.default_timer()
        out = pg_bin.run_capture(pgbench_cmd, env)
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

    prewarmed_args = ("prewarmed", prewarmed_id, connstr_to_env(prewarmed_uri))
    prewarmed_thread = Thread(target=bench, args=prewarmed_args)
    prewarmed_thread.start()

    bench("ordinary", ordinary_id, connstr_to_env(ordinary_uri))
    prewarmed_thread.join()
