import os
import statistics
import threading
import time
import timeit
from random import randint

import pytest
from batch_others.test_backpressure import pg_cur
from fixtures import log_helper
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.neon_fixtures import Postgres

from performance.test_perf_pgbench import get_scales_matrix


def start_heavy_write_workload(pg: Postgres, scale: int = 1, n_iters: int = 10):
    """Start an intensive write workload which first initializes a table `t` with `new_rows_each_update` rows.
    At each subsequent step, we update a subset of rows in the table and insert new `new_rows_each_update` rows.
    The variable `new_rows_each_update` is equal to `scale * 100_000`."""
    new_rows_each_update = scale * 100_000

    with pg_cur(pg) as cur:
        cur.execute(
            f"INSERT INTO t SELECT s, s % 10, 0 FROM generate_series(1,{new_rows_each_update}) s")
    n_rows = new_rows_each_update

    for _ in range(n_iters):
        # the subset of rows updated in this step is determined by the `tag` column
        tag = randint(0, 10)
        with pg_cur(pg) as cur:
            cur.execute(f"UPDATE t SET cnt=cnt+1 WHERE tag={tag}")
            cur.execute(
                f"INSERT INTO t SELECT s+{n_rows}, s % 10, 0 FROM generate_series(1,{new_rows_each_update}) s"
            )
        n_rows += new_rows_each_update


@pytest.mark.parametrize("scale", get_scales_matrix(1))
def test_measure_read_latency_heavy_write_workload(neon_with_baseline: PgCompare, scale: int):
    env = neon_with_baseline
    pg = env.pg

    with pg_cur(pg) as cur:
        cur.execute("CREATE TABLE t(key int primary key, tag int, cnt int)")

    write_thread = threading.Thread(target=start_heavy_write_workload, args=(pg, ))
    write_thread.start()

    record_read_latency(env, write_thread, "SELECT count(*) from t")


def start_pgbench_simple_update_workload(env: PgCompare, scale: int, transactions: int):
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Igvp', env.pg.connstr()])
    env.flush()

    env.pg_bin.run_capture(['pgbench', '-N', f'-t{transactions}', '-Mprepared', env.pg.connstr()])
    env.flush()


def get_transactions_matrix(default: int = 100_000):
    scales = os.getenv("TEST_PG_BENCH_TRANSACTIONS_MATRIX", default=str(default))
    return list(map(int, scales.split(",")))


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("transactions", get_transactions_matrix())
def test_measure_read_latency_pgbench_simple_update_workload(neon_with_baseline: PgCompare,
                                                             scale: int,
                                                             transactions: int):
    env = neon_with_baseline

    # create pgbench tables
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', '-Idt', env.pg.connstr()])
    env.flush()

    write_thread = threading.Thread(target=start_pgbench_simple_update_workload,
                                    args=(env, scale, transactions))
    write_thread.start()

    record_read_latency(env, write_thread, "SELECT sum(abalance) from pgbench_accounts")


def start_pgbench_intensive_initialization(env: PgCompare, scale: int):
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()


@pytest.mark.parametrize("scale", get_scales_matrix(1000))
def test_measure_read_latency_other_table(neon_with_baseline: PgCompare, scale: int):
    # Measure the latency when reading from a table when doing a heavy write workload in another table
    # This test tries to simulate the scenario described in https://github.com/neondatabase/neon/issues/1763.

    env = neon_with_baseline
    with pg_cur(env.pg) as cur:
        cur.execute("CREATE TABLE foo(key int primary key, i int)")
        cur.execute(f"INSERT INTO foo SELECT s, s FROM generate_series(1,100000) s")

    write_thread = threading.Thread(target=start_pgbench_intensive_initialization,
                                    args=(env, scale))
    write_thread.start()

    record_read_latency(env, write_thread, "SELECT count(*) from foo")


def record_read_latency(env: PgCompare, write_thread: threading.Thread, read_query: str):
    with env.record_duration("run_duration"):
        read_latencies = []
        while write_thread.is_alive():
            time.sleep(1.0)

            with pg_cur(env.pg) as cur:
                t = timeit.default_timer()
                cur.execute(read_query)
                duration = timeit.default_timer() - t

                log_helper.log.info(
                    f"Executed read query {read_query}, got {cur.fetchall()}, took {duration}")

                read_latencies.append(duration)

    env.zenbenchmark.record("read_latency_avg",
                            statistics.mean(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("read_latency_stdev",
                            statistics.stdev(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
