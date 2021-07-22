from contextlib import closing
import psycopg2.extras
import time

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test insertion of larg number of records
#
# This test is pretty tightly coupled with the current implementation of page version storage
# and garbage collection in object_repository.rs.
#
def test_seq_scan(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_seq_scan", "empty"])
    pg = postgres.create_start('test_seq_scan')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
          cur.execute("create table t(c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint)")
          cur.execute("insert into t values (generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000))")
          cur.execute("set max_parallel_workers_per_gather=0");
          for i in range(100):
              start = time.time()
              cur.execute("select count(*) from t");
              stop = time.time()
              print(f'Elapsed time for iterating through 1000000 records is {stop - start}')
