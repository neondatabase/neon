from random import randint
import statistics
import time
import threading
import timeit
from batch_others.test_backpressure import pg_cur
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.neon_fixtures import NeonEnv, Postgres
from fixtures.log_helper import log


def start_heavy_write_workload(pg: Postgres, scale: int = 1, n_iters: int = 10):
    """Start an intensive write workload which first initializes a table `t` with `new_rows_each_update` rows.
    At each subsequent step, we update a subset of rows in the table and insert new `new_rows_each_update` rows.
    The variable `new_rows_each_update` is equal to `scale * 100_000`."""
    new_rows_each_update = scale * 100_000

    with pg_cur(pg) as cur:
        cur.execute("CREATE TABLE t(key int primary key, tag int, cnt int)")

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


def test_measure_read_latency_heavy_write_workload(neon_with_baseline: PgCompare):
    env = neon_with_baseline
    pg = env.pg

    with env.record_duration("run_duration"):
        write_thread = threading.Thread(target=start_heavy_write_workload, args=(pg, ))
        write_thread.start()

        read_latencies = []
        while write_thread.is_alive():
            time.sleep(1.0)

            with pg_cur(pg) as cur:
                t = timeit.default_timer()
                cur.execute("SELECT count(*) from t")
                res = cur.fetchone()[0]
                duration = timeit.default_timer() - t

                log.info(f"Get the number of rows in the table `t`, got {res}, took {duration}s")

                read_latencies.append(duration)

    env.zenbenchmark.record("read_latency_avg",
                            statistics.mean(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("read_latency_stdev",
                            statistics.stdev(read_latencies),
                            's',
                            MetricReport.LOWER_IS_BETTER)
