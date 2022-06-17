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
from performance.test_backpressure import record_read_latency

def start_write_workload(pg: Postgres, scale: int = 10):
    with pg_cur(pg) as cur:
        cur.execute(
            f"create table big as select generate_series(1,{scale*100_000})")

@pytest.mark.parametrize("scale", get_scales_matrix(1))
def test_measure_read_latency_heavy_write_workload(neon_with_baseline: PgCompare, scale: int):
    env = neon_with_baseline
    pg = env.pg

    with pg_cur(pg) as cur:
        cur.execute(f"create table small as select generate_series(1,{scale*100_000})")

    write_thread = threading.Thread(target=start_write_workload, args=(pg, scale*100))
    write_thread.start()

    record_read_latency(env, write_thread, "SELECT count(*) from small")
