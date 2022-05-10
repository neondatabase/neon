import pytest
from contextlib import closing
from fixtures.compare_fixtures import PgCompare
from pytest_lazyfixture import lazy_fixture  # type: ignore


@pytest.mark.parametrize(
    "env",
    [
        # The test is too slow to run in CI, but fast enough to run with remote tests
        pytest.param(lazy_fixture("zenith_compare"), id="zenith", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("vanilla_compare"), id="vanilla", marks=pytest.mark.slow),
        pytest.param(lazy_fixture("remote_compare"), id="remote", marks=pytest.mark.remote_cluster),
    ])
def test_hot_page(env: PgCompare):
    # Update the same page many times, then measure read performance
    num_writes = 1000000

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:

            # Write many updates to the same row
            with env.record_duration('write'):
                cur.execute('create table t (i integer);')
                cur.execute('insert into t values (0);')
                for i in range(num_writes):
                    cur.execute(f'update t set i = {i};')

            # Write 3-4 MB to evict t from compute cache
            cur.execute('create table f (i integer);')
            cur.execute(f'insert into f values (generate_series(1,100000));')

            # Read
            with env.record_duration('read'):
                cur.execute('select * from t;')
                cur.fetchall()
