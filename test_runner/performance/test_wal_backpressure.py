from random import randint, random
import time
import threading
import timeit
from batch_others.test_backpressure import pg_cur
from fixtures.neon_fixtures import NeonEnv, Postgres
from fixtures.log_helper import log


def measure_read_latency(pg: Postgres, read_query: str):
    for _ in range(20):
        time.sleep(1.0)

        with pg_cur(pg) as cur:
            t = timeit.default_timer()
            cur.execute(read_query)
            res = cur.fetchall()
            duration = timeit.default_timer() - t

            log.info(f"Executing read query '{read_query}', got {res}, took {duration}s")


def test_measure_read_latency_with_intensive_write_workload(neon_simple_env: NeonEnv):
    env = neon_simple_env

    pg = env.postgres.create_start("main")

    pg.safe_psql("CREATE TABLE t(key int primary key, tag int, cnt int)")

    read_thread = threading.Thread(target=measure_read_latency, args=(pg, "SELECT count(*) from t"))
    read_thread.start()

    # Start an intensive write workload which first initializes the table t with 1000000 rows.
    # At each subsequent step, we update a subset of rows and insert new 100000 rows.
    pg.safe_psql("INSERT INTO t SELECT s, s % 10, 0 FROM generate_series(1,1000000) s")
    n_rows = 1000000
    while read_thread.is_alive():
        # the subset of rows updated in this step is determined by the `tag` column
        tag = randint(0, 10)
        with pg_cur(pg) as cur:
            cur.execute(f"UPDATE t SET cnt=cnt+1 WHERE tag={tag}")
            cur.execute(
                f"INSERT INTO t SELECT s+{n_rows}, s % 10, 0 FROM generate_series(1,100000) s")
        n_rows += 100000

        time.sleep(1.0)
