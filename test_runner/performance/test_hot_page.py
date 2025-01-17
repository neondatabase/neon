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
def test_hot_page(env: PgCompare):
    # Update the same page many times, then measure read performance

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists t, f;")
            num_writes = 1000000

            # Use a PL/pgSQL block to perform many updates to the same row
            # without depending on the latency between database client and postgres
            # server
            # - however a single staement should not run into a timeout so we increase it
            cur.execute("SET statement_timeout = '4h';")
            with env.record_duration("write"):
                cur.execute(
                    f"""
                DO $$
                BEGIN
                    create table t (i integer);
                    insert into t values (0);

                    FOR j IN 1..{num_writes} LOOP
                        update t set i = j;
                    END LOOP;
                END $$;
                """
                )

            # Write ca 350 MB to evict t from compute shared buffers (128 MB)
            # however it will still be in LFC, so I do not really understand the point of this test
            cur.execute("create table f (i integer);")
            cur.execute("insert into f values (generate_series(1,100000));")

            # Read
            with env.record_duration("read"):
                cur.execute("select * from t;")
                cur.fetchall()
