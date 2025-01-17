from __future__ import annotations

import threading

import pytest
from fixtures.compare_fixtures import PgCompare
from fixtures.neon_fixtures import PgProtocol

from performance.test_perf_pgbench import get_scales_matrix
from performance.test_wal_backpressure import record_read_latency


def start_write_workload(pg: PgProtocol, scale: int = 10):
    with pg.connect().cursor() as cur:
        cur.execute(f"create table big as select generate_series(1,{scale*100_000})")


# Measure latency of reads on one table, while lots of writes are happening on another table.
# The fine-grained tracking of last-written LSNs helps to keep the latency low. Without it, the reads would
# often need to wait for the WAL records of the unrelated writes to be processed by the pageserver.
@pytest.mark.parametrize("scale", get_scales_matrix(1))
def test_measure_read_latency_heavy_write_workload(neon_with_baseline: PgCompare, scale: int):
    env = neon_with_baseline
    pg = env.pg

    with pg.connect().cursor() as cur:
        cur.execute(f"create table small as select generate_series(1,{scale*100_000})")

    write_thread = threading.Thread(target=start_write_workload, args=(pg, scale * 100))
    write_thread.start()

    record_read_latency(env, lambda: write_thread.is_alive(), "SELECT count(*) from small")
