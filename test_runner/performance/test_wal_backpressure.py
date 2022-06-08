import os
import time
import statistics
import threading
import time
import timeit
from random import randint
from typing import Callable

import pytest
from batch_others.test_backpressure import pg_cur
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import NeonCompare, PgCompare
from fixtures.log_helper import log
from fixtures.neon_fixtures import (DEFAULT_BRANCH_NAME, NeonEnvBuilder, Postgres)

from performance.test_perf_pgbench import get_scales_matrix


def get_num_iters_matrix(default: int = 10):
    scales = os.getenv("TEST_NUM_ITERS_MATRIX", default=str(default))
    return list(map(int, scales.split(",")))


def get_transactions_matrix(default: int = 1_000):
    scales = os.getenv("TEST_TRANSACTIONS_MATRIX", default=str(default))
    return list(map(int, scales.split(",")))


@pytest.fixture(scope='function')
def neon_compare_with_fsync(request, zenbenchmark, pg_bin,
                            neon_env_builder: NeonEnvBuilder) -> NeonCompare:
    neon_env_builder.safekeepers_enable_fsync = True

    env = neon_env_builder.init_start()
    env.neon_cli.create_branch('empty', ancestor_branch_name=DEFAULT_BRANCH_NAME)

    branch_name = request.node.name
    return NeonCompare(zenbenchmark, env, pg_bin, branch_name)


@pytest.fixture(params=["vanilla_compare", "neon_compare", "neon_compare_with_fsync"],
                ids=["vanilla", "neon_fsync_off", "neon_fsync_on"])
def compare(request) -> PgCompare:
    fixture = request.getfixturevalue(request.param)
    assert isinstance(fixture, PgCompare)
    return fixture


def start_heavy_write_workload(pg: Postgres, scale: int, num_iters: int):
    """Start an intensive write workload.
    At each step, we update a subset of rows in the table and insert new `new_rows_each_update` rows.
    The variable `new_rows_each_update` is equal to `scale * 100_000`.
    The number of steps is determined by `num_iters` variable."""
    new_rows_each_update = scale * 100_000

    n_rows = 0
    for _ in range(num_iters):
        # the subset of rows updated in this step is determined by the `tag` column
        tag = randint(0, 10)
        with pg_cur(pg) as cur:
            cur.execute(f"UPDATE t SET cnt=cnt+1 WHERE tag={tag}")
            cur.execute(
                f"INSERT INTO t SELECT s+{n_rows}, s % 10, 0 FROM generate_series(1,{new_rows_each_update}) s"
            )
        n_rows += new_rows_each_update


@pytest.mark.parametrize("scale", get_scales_matrix(10))
@pytest.mark.parametrize("num_iters", get_num_iters_matrix())
def test_measure_read_latency_heavy_write_workload(compare: PgCompare, scale: int, num_iters: int):
    env = compare
    pg = env.pg

    with pg_cur(pg) as cur:
        cur.execute("CREATE TABLE t(key int primary key, tag int, cnt int)")

    write_thread = threading.Thread(target=start_heavy_write_workload, args=(pg, scale, num_iters))
    write_thread.start()

    watch_thread = threading.Thread(target=watch_lsn_lags,
                                    args=(env.pg, lambda: write_thread.is_alive()))
    watch_thread.start()

    record_read_latency(env, lambda: write_thread.is_alive(), "SELECT count(*) from t")


def start_pgbench_simple_update_workload(env: PgCompare, scale: int, transactions: int):
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Igvp', env.pg.connstr()])
    env.flush()

    env.pg_bin.run_capture(['pgbench', '-N', f'-t{transactions}', '-Mprepared', env.pg.connstr()])
    env.flush()


@pytest.mark.parametrize("scale", get_scales_matrix(100))
@pytest.mark.parametrize("transactions", get_transactions_matrix())
def test_measure_read_latency_pgbench_simple_update_workload(compare: PgCompare,
                                                             scale: int,
                                                             transactions: int):
    env = compare

    # create pgbench tables
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Idt', env.pg.connstr()])
    env.flush()

    write_thread = threading.Thread(target=start_pgbench_simple_update_workload,
                                    args=(env, scale, transactions))
    write_thread.start()

    watch_thread = threading.Thread(target=watch_lsn_lags,
                                    args=(env.pg, lambda: write_thread.is_alive()))
    watch_thread.start()

    record_read_latency(env,
                        lambda: write_thread.is_alive(),
                        "SELECT sum(abalance) from pgbench_accounts")


def start_pgbench_intensive_initialization(env: PgCompare, scale: int):
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-n', env.pg.connstr()])
    env.flush()


@pytest.mark.parametrize("scale", get_scales_matrix(1000))
def test_measure_read_latency_other_table(compare: PgCompare, scale: int):
    # Measure the latency when reading from a table when doing a heavy write workload in another table
    # This test tries to simulate the scenario described in https://github.com/neondatabase/neon/issues/1763.

    env = compare
    with pg_cur(env.pg) as cur:
        cur.execute("CREATE TABLE foo as select generate_series(1,100000)")

    write_thread = threading.Thread(target=start_pgbench_intensive_initialization,
                                    args=(env, scale))
    write_thread.start()

    watch_thread = threading.Thread(target=watch_lsn_lags,
                                    args=(env.pg, lambda: write_thread.is_alive()))
    watch_thread.start()

    record_read_latency(env, lambda: write_thread.is_alive(), "SELECT count(*) from foo")


def watch_lsn_lags(pg: Postgres, run_cond: Callable[[], bool], pool_interval: float = 5.0):
    log.info("Start LSN lags watcher...")

    with pg_cur(pg) as cur:
        cur.execute("CREATE EXTENSION neon")

        while run_cond():
            cur.execute('''
            select pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn)),
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),disk_consistent_lsn)),
            pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_flush_lsn(),remote_consistent_lsn))
            from backpressure_lsns();
            ''')

            res = cur.fetchone()
            log.info(f"received_lsn_lag = {res[0]}, "
                     f"disk_consistent_lsn_lag = {res[1]}, "
                     f"remote_consistent_lsn_lag = {res[2]}")

            time.sleep(pool_interval)


def record_read_latency(env: PgCompare,
                        run_cond: Callable[[], bool],
                        read_query: str,
                        read_interval: float = 1.0):
    with env.record_duration("run_duration"):
        read_latencies = []
        while run_cond():
            with pg_cur(env.pg) as cur:
                t = timeit.default_timer()
                cur.execute(read_query)
                duration = timeit.default_timer() - t

                # log.info(
                #     f"Executed read query {read_query}, got {cur.fetchall()}, took {duration}")

                read_latencies.append(duration)

            time.sleep(read_interval)

    env.zenbenchmark.record("read_latency_max",
                            max(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("read_latency_avg",
                            statistics.mean(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("read_latency_stdev",
                            statistics.stdev(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
