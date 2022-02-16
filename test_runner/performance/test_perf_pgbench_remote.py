import dataclasses
import os
import subprocess
from typing import List
from fixtures.benchmark_fixture import PgBenchRunResult, ZenithBenchmarker
import pytest
from datetime import datetime
import calendar
import timeit
import os


def utc_now_timestamp() -> int:
    return calendar.timegm(datetime.utcnow().utctimetuple())


@dataclasses.dataclass
class PgBenchRunner:
    connstr: str
    scale: int
    transactions: int
    pgbench_bin_path: str = "pgbench"

    def invoke(self, args: List[str]) -> 'subprocess.CompletedProcess[str]':
        res = subprocess.run([self.pgbench_bin_path, *args], text=True, capture_output=True)

        if res.returncode != 0:
            raise RuntimeError(f"pgbench failed. stdout: {res.stdout} stderr: {res.stderr}")
        return res

    def init(self, vacuum: bool = True) -> 'subprocess.CompletedProcess[str]':
        args = []
        if not vacuum:
            args.append("--no-vacuum")
        args.extend([f"--scale={self.scale}", "--initialize", self.connstr])
        return self.invoke(args)

    def run(self, jobs: int = 1, clients: int = 1):
        return self.invoke([
            f"--transactions={self.transactions}",
            f"--jobs={jobs}",
            f"--client={clients}",
            "--progress=2",  # print progress every two seconds
            self.connstr,
        ])


@pytest.fixture
def connstr():
    res = os.getenv("BENCHMARK_CONNSTR")
    if res is None:
        raise ValueError("no connstr provided, use BENCHMARK_CONNSTR environment variable")
    return res


def get_transactions_matrix():
    transactions = os.getenv("TEST_PG_BENCH_TRANSACTIONS_MATRIX")
    if transactions is None:
        return [10**4, 10**5]
    return list(map(int, transactions.split(",")))


def get_scales_matrix():
    scales = os.getenv("TEST_PG_BENCH_SCALES_MATRIX")
    if scales is None:
        return [10, 20]
    return list(map(int, scales.split(",")))


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("transactions", get_transactions_matrix())
@pytest.mark.remote_cluster
def test_pg_bench_remote_cluster(zenbenchmark: ZenithBenchmarker,
                                 connstr: str,
                                 scale: int,
                                 transactions: int):
    """
    The best way is to run same pack of tests both, for local zenith
    and against staging, but currently local tests heavily depend on
    things available only locally e.g. zenith binaries, pageserver api, etc.
    Also separate test allows to run pgbench workload against vanilla postgres
    or other systems that support postgres protocol.

    Also now this is more of a liveness test because it stresses pageserver internals,
    so we clearly see what goes wrong in more "real" environment.
    """
    pg_bin = os.getenv("PG_BIN")
    if pg_bin is not None:
        pgbench_bin_path = os.path.join(pg_bin, "pgbench")
    else:
        pgbench_bin_path = "pgbench"

    runner = PgBenchRunner(
        connstr=connstr,
        scale=scale,
        transactions=transactions,
        pgbench_bin_path=pgbench_bin_path,
    )
    # calculate timestamps and durations separately
    # timestamp is intended to be used for linking to grafana and logs
    # duration is actually a metric and uses float instead of int for timestamp
    init_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    runner.init()
    init_duration = timeit.default_timer() - t0
    init_end_timestamp = utc_now_timestamp()

    run_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    out = runner.run()  # TODO handle failures
    run_duration = timeit.default_timer() - t0
    run_end_timestamp = utc_now_timestamp()

    res = PgBenchRunResult.parse_from_output(
        out=out,
        init_duration=init_duration,
        init_start_timestamp=init_start_timestamp,
        init_end_timestamp=init_end_timestamp,
        run_duration=run_duration,
        run_start_timestamp=run_start_timestamp,
        run_end_timestamp=run_end_timestamp,
    )

    zenbenchmark.record_pg_bench_result(res)
