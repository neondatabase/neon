from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.compare_fixtures import PgCompare
from pytest_lazyfixture import lazy_fixture


@pytest.mark.parametrize(
    "env",
    [
        # The test is too slow to run in CI, but fast enough to run with remote tests
        pytest.param(lazy_fixture("neon_compare"), id="neon", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("vanilla_compare"), id="vanilla", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("remote_compare"), id="remote", marks=pytest.mark.remote_cluster),
    ],
)
def test_hot_table(env: PgCompare):
    # Update a small table many times, then measure read performance
    num_rows = 100000  # initial table size only about 4 MB
    num_writes = 10000000  # write approximately 349 MB blocks > 128 MB shared_buffers
    num_reads = 10

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists t;")

            # Write many updates to a small table
            with env.record_duration("write"):
                cur.execute("create table t (i integer primary key);")
                cur.execute(f"insert into t values (generate_series(1,{num_rows}));")
                # PL/pgSQL block to perform updates (and avoid latency between client and server)
                # - however a single staement should not run into a timeout so we increase it
                cur.execute("SET statement_timeout = '4h';")
                cur.execute(
                    f"""
                DO $$
                DECLARE
                    r integer := {num_rows};
                BEGIN
                    FOR j IN 1..{num_writes} LOOP
                        UPDATE t SET i = j + r WHERE i = j;
                    END LOOP;
                END $$;
                """
                )

            # Read the table
            with env.record_duration("read"):
                for _ in range(num_reads):
                    cur.execute("select * from t;")
                    cur.fetchall()
