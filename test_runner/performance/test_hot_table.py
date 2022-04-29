import pytest
from contextlib import closing
from fixtures.compare_fixtures import PgCompare


# HACK: I wanna see this run in CI, if successful I'll clean it up
# @pytest.mark.slow
@pytest.mark.remote_cluster
def test_hot_table(remote_compare: PgCompare):
    # Update a small table many times, then measure read performance
    env = zenith_with_baseline

    num_rows = 100000  # Slightly larger than shared buffers size  TODO validate
    num_writes = 1000000
    num_reads = 10

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:

            # Write many updates to a small table
            with env.record_duration('write'):
                cur.execute('create table t (i integer primary key);')
                cur.execute(f'insert into t values (generate_series(1,{num_rows}));')
                for i in range(num_writes):
                    cur.execute(f'update t set i = {i + num_rows} WHERE i = {i};')

            # Read the table
            with env.record_duration('read'):
                for i in range(num_reads):
                    cur.execute('select * from t;')
                    cur.fetchall()
