from __future__ import annotations

import timeit
from pathlib import Path
from typing import TYPE_CHECKING, cast

from fixtures.benchmark_fixture import PgBenchRunResult

from performance.test_perf_pgbench import utc_now_timestamp

if TYPE_CHECKING:
    from fixtures.compare_fixtures import NeonCompare
    from fixtures.neon_fixtures import Endpoint, NeonApiEndpoint


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
        pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", connstr, "-s10"])
        ep.safe_psql("CREATE EXTENSION neon")
        ep.http_client().offload_lfc()
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

@pytest.mark.remote_cluster
def test_compare_prewarmed_pgbench_perf_benchmark(
    pg_bin: PgBin,
    benchmark_project_pub: NeonApiEndpoint,
    benchmark_project_sub: NeonApiEndpoint,
    zenbenchmark: NeonBenchmarker
):
    ep_normal: Endpoint = benchmark_project_pub.__slots__


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
        ep.http_client().offload_lfc()
        ep.stop()
        ep.start()
        with neon_compare.record_duration(f"{ep.branch_name}_run_duration"):
            ep.safe_psql("SELECT count(*) from foo")
