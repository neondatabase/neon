import pytest
from contextlib import closing
from fixtures.compare_fixtures import PgCompare


@pytest.mark.slow
def test_hot_page(zenith_with_baseline: PgCompare):
    # Update the same page many times, then measure read performance
    env = zenith_with_baseline

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
