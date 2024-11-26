from __future__ import annotations

import statistics
import threading
import time
import timeit
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.compare_fixtures import NeonCompare, PgCompare, VanillaCompare
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, flush_ep_to_pageserver

from performance.test_perf_pgbench import get_durations_matrix, get_scales_matrix

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Any


@pytest.fixture(params=["vanilla", "neon_off", "neon_on"])
# This fixture constructs multiple `PgCompare` interfaces using a builder pattern.
# The builder parameters are encoded in the fixture's param.
# For example, to build a `NeonCompare` interface, the corresponding fixture's param should have
# a format of `neon_{safekeepers_enable_fsync}`.
# Note that, here "_" is used to separate builder parameters.
def pg_compare(request) -> Generator[PgCompare, None, None]:
    x = request.param.split("_")

    if x[0] == "vanilla":
        # `VanillaCompare` interface
        fixture = request.getfixturevalue("vanilla_compare")
        assert isinstance(fixture, VanillaCompare)

        yield fixture
    else:
        assert (
            len(x) == 2
        ), f"request param ({request.param}) should have a format of `neon_{{safekeepers_enable_fsync}}`"

        # `NeonCompare` interface
        neon_env_builder = request.getfixturevalue("neon_env_builder")
        assert isinstance(neon_env_builder, NeonEnvBuilder)

        zenbenchmark = request.getfixturevalue("zenbenchmark")
        assert isinstance(zenbenchmark, NeonBenchmarker)

        pg_bin = request.getfixturevalue("pg_bin")
        assert isinstance(pg_bin, PgBin)

        neon_env_builder.safekeepers_enable_fsync = x[1] == "on"

        env = neon_env_builder.init_start()

        cmp = NeonCompare(zenbenchmark, env, pg_bin)

        yield cmp

        flush_ep_to_pageserver(env, cmp._pg, cmp.tenant, cmp.timeline)
        env.pageserver.http_client().timeline_checkpoint(
            cmp.tenant, cmp.timeline, compact=False, wait_until_uploaded=True
        )


def start_heavy_write_workload(env: PgCompare, n_tables: int, scale: int, num_iters: int):
    """Start an intensive write workload across multiple tables.

    ## Single table workload:
    At each step, insert new `new_rows_each_update` rows.
    The variable `new_rows_each_update` is equal to `scale * 100_000`.
    The number of steps is determined by `num_iters` variable."""
    new_rows_each_update = scale * 100_000

    def start_single_table_workload(table_id: int):
        for _ in range(num_iters):
            with env.pg.connect(options="-cstatement_timeout=300s").cursor() as cur:
                cur.execute(
                    f"INSERT INTO t{table_id} SELECT FROM generate_series(1,{new_rows_each_update})"
                )

    with env.record_duration("run_duration"):
        threads = [
            threading.Thread(target=start_single_table_workload, args=(i,)) for i in range(n_tables)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


@pytest.mark.timeout(1000)
@pytest.mark.parametrize("n_tables", [5])
@pytest.mark.parametrize("scale", get_scales_matrix(5))
@pytest.mark.parametrize("num_iters", [10])
def test_heavy_write_workload(pg_compare: PgCompare, n_tables: int, scale: int, num_iters: int):
    env = pg_compare

    # Initializes test tables
    with env.pg.connect().cursor() as cur:
        for i in range(n_tables):
            cur.execute(
                f"CREATE TABLE t{i}(key serial primary key, t text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')"
            )
            cur.execute(f"INSERT INTO t{i} (key) VALUES (0)")

    workload_thread = threading.Thread(
        target=start_heavy_write_workload, args=(env, n_tables, scale, num_iters)
    )
    workload_thread.start()

    record_thread = threading.Thread(
        target=record_lsn_write_lag, args=(env, lambda: workload_thread.is_alive())
    )
    record_thread.start()

    record_read_latency(env, lambda: workload_thread.is_alive(), "SELECT * from t0 where key = 0")
    workload_thread.join()
    record_thread.join()


def start_pgbench_simple_update_workload(env: PgCompare, duration: int):
    with env.record_duration("run_duration"):
        env.pg_bin.run_capture(
            [
                "pgbench",
                "-j10",
                "-c10",
                "-N",
                f"-T{duration}",
                env.pg.connstr(options="-csynchronous_commit=off"),
            ]
        )
        env.flush()


@pytest.mark.timeout(1000)
@pytest.mark.parametrize("scale", get_scales_matrix(100))
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench_simple_update_workload(pg_compare: PgCompare, scale: int, duration: int):
    env = pg_compare

    # initialize pgbench tables
    env.pg_bin.run_capture(["pgbench", f"-s{scale}", "-i", env.pg.connstr()])
    env.flush()

    workload_thread = threading.Thread(
        target=start_pgbench_simple_update_workload, args=(env, duration)
    )
    workload_thread.start()

    record_thread = threading.Thread(
        target=record_lsn_write_lag, args=(env, lambda: workload_thread.is_alive())
    )
    record_thread.start()

    record_read_latency(
        env, lambda: workload_thread.is_alive(), "SELECT * from pgbench_accounts where aid = 1"
    )
    workload_thread.join()
    record_thread.join()


def start_pgbench_intensive_initialization(env: PgCompare, scale: int, done_event: threading.Event):
    with env.record_duration("run_duration"):
        # Disable statement timeout (default: 120s) because the
        # initialization step can be slow with a large scale.
        env.pg_bin.run_capture(
            [
                "pgbench",
                f"-s{scale}",
                "-i",
                "-Idtg",
                env.pg.connstr(options="-cstatement_timeout=0"),
            ]
        )

    done_event.set()


@pytest.mark.timeout(1000)
@pytest.mark.parametrize("scale", get_scales_matrix(1000))
def test_pgbench_intensive_init_workload(pg_compare: PgCompare, scale: int):
    env = pg_compare
    with env.pg.connect().cursor() as cur:
        cur.execute("CREATE TABLE foo as select generate_series(1,100000)")

    workload_done_event = threading.Event()

    workload_thread = threading.Thread(
        target=start_pgbench_intensive_initialization, args=(env, scale, workload_done_event)
    )
    workload_thread.start()

    record_thread = threading.Thread(
        target=record_lsn_write_lag, args=(env, lambda: not workload_done_event.is_set())
    )
    record_thread.start()

    record_read_latency(env, lambda: not workload_done_event.is_set(), "SELECT count(*) from foo")
    workload_thread.join()
    record_thread.join()


def record_lsn_write_lag(env: PgCompare, run_cond: Callable[[], bool], pool_interval: float = 1.0):
    if not isinstance(env, NeonCompare):
        return

    lsn_write_lags: list[Any] = []
    last_received_lsn = Lsn(0)
    last_pg_flush_lsn = Lsn(0)

    with env.pg.connect().cursor() as cur:
        cur.execute("CREATE EXTENSION neon")

        while run_cond():
            cur.execute(
                """
            select pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn),
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn)),
            pg_current_wal_flush_lsn(),
            received_lsn
            from backpressure_lsns();
            """
            )

            res = cur.fetchone()
            assert isinstance(res, tuple)
            lsn_write_lags.append(res[0])

            curr_received_lsn = Lsn(res[3])
            lsn_process_speed = (curr_received_lsn - last_received_lsn) / (1024**2)
            last_received_lsn = curr_received_lsn

            curr_pg_flush_lsn = Lsn(res[2])
            lsn_produce_speed = (curr_pg_flush_lsn - last_pg_flush_lsn) / (1024**2)
            last_pg_flush_lsn = curr_pg_flush_lsn

            log.info(
                f"received_lsn_lag={res[1]}, pg_flush_lsn={res[2]}, received_lsn={res[3]}, lsn_process_speed={lsn_process_speed:.2f}MB/s, lsn_produce_speed={lsn_produce_speed:.2f}MB/s"
            )

            time.sleep(pool_interval)

    env.zenbenchmark.record(
        "lsn_write_lag_max",
        float(max(lsn_write_lags) / (1024**2)),
        "MB",
        MetricReport.LOWER_IS_BETTER,
    )
    env.zenbenchmark.record(
        "lsn_write_lag_avg",
        float(statistics.mean(lsn_write_lags) / (1024**2)),
        "MB",
        MetricReport.LOWER_IS_BETTER,
    )
    env.zenbenchmark.record(
        "lsn_write_lag_stdev",
        float(statistics.stdev(lsn_write_lags) / (1024**2)),
        "MB",
        MetricReport.LOWER_IS_BETTER,
    )


def record_read_latency(
    env: PgCompare, run_cond: Callable[[], bool], read_query: str, read_interval: float = 1.0
):
    read_latencies = []

    with env.pg.connect().cursor() as cur:
        while run_cond():
            try:
                t1 = timeit.default_timer()
                cur.execute(read_query)
                t2 = timeit.default_timer()

                log.info(
                    f"Executed read query {read_query}, got {cur.fetchall()}, read time {t2-t1:.2f}s"
                )
                read_latencies.append(t2 - t1)
            except Exception as err:
                log.error(f"Got error when executing the read query: {err}")

            time.sleep(read_interval)

    env.zenbenchmark.record(
        "read_latency_max", max(read_latencies), "s", MetricReport.LOWER_IS_BETTER
    )
    env.zenbenchmark.record(
        "read_latency_avg", statistics.mean(read_latencies), "s", MetricReport.LOWER_IS_BETTER
    )
    env.zenbenchmark.record(
        "read_latency_stdev", statistics.stdev(read_latencies), "s", MetricReport.LOWER_IS_BETTER
    )
