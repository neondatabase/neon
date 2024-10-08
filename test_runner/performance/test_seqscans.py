# Test sequential scan speed
#

from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.log_helper import log
from pytest_lazyfixture import lazy_fixture


@pytest.mark.parametrize(
    "rows,iters,workers",
    [
        # The test table is large enough (3-4 MB) that it doesn't fit in the compute node
        # cache, so the seqscans go to the page server. But small enough that it fits
        # into memory in the page server.
        pytest.param(100000, 100, 0),
        # Also test with a larger table, with and without parallelism
        pytest.param(10000000, 1, 0),
        pytest.param(10000000, 1, 4),
    ],
)
@pytest.mark.parametrize(
    "env,scale",
    [
        # Run on all envs. Use 200x larger table on remote cluster to make sure
        # it doesn't fit in shared buffers, which are larger on remote than local.
        pytest.param(lazy_fixture("neon_compare"), 1, id="neon"),
        pytest.param(lazy_fixture("vanilla_compare"), 1, id="vanilla"),
        # Reenable after switching per-test projects created via API
        # pytest.param(
        #     lazy_fixture("remote_compare"), 200, id="remote", marks=pytest.mark.remote_cluster
        # ),
    ],
)
def test_seqscans(env: PgCompare, scale: int, rows: int, iters: int, workers: int):
    rows = scale * rows

    with closing(env.pg.connect(options="-cstatement_timeout=0")) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists t;")
            cur.execute("create table t (i integer);")
            cur.execute(f"insert into t values (generate_series(1,{rows}));")

            # Verify that the table is larger than shared_buffers
            cur.execute(
                """
            select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('t') as tbl_size
            from pg_settings where name = 'shared_buffers'
            """
            )
            row = cur.fetchone()
            assert row is not None
            shared_buffers = row[0]
            table_size = row[1]
            log.info(f"shared_buffers is {shared_buffers}, table size {table_size}")
            assert int(shared_buffers) < int(table_size)
            env.zenbenchmark.record("table_size", table_size, "bytes", MetricReport.TEST_PARAM)

            cur.execute(f"set max_parallel_workers_per_gather = {workers}")

            with env.record_duration("run"):
                for _ in range(iters):
                    cur.execute("select count(*) from t;")
