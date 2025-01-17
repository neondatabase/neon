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
def test_dup_key(env: PgCompare):
    # Update the same page many times, then measure read performance

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists t, f;")

            cur.execute("SET synchronous_commit=off")
            cur.execute("SET statement_timeout=0")

            # Write many updates to the same row
            with env.record_duration("write"):
                cur.execute("create table t (i integer, filler text);")
                cur.execute("insert into t values (0);")
                cur.execute(
                    """
do $$
begin
  for ivar in 1..5000000 loop
    update t set i = ivar, filler = repeat('a', 50);
    update t set i = ivar, filler = repeat('b', 50);
    update t set i = ivar, filler = repeat('c', 50);
    update t set i = ivar, filler = repeat('d', 50);
    rollback;
  end loop;
end;
$$;
"""
                )

            # Write 3-4 MB to evict t from compute cache
            cur.execute("create table f (i integer);")
            cur.execute("insert into f values (generate_series(1,100000));")

            # Read
            with env.record_duration("read"):
                cur.execute("select * from t;")
                cur.fetchall()
