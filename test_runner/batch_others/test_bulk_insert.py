from contextlib import closing
import psycopg2.extras

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test insertion of larg number of records
#
# This test is pretty tightly coupled with the current implementation of page version storage
# and garbage collection in object_repository.rs.
#
def test_bulk_insert(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_bulk_insert", "empty"])
    pg = postgres.create_start('test_bulk_insert')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
          cur.execute("create table t(c1 bigint, c2 bigint, c3 bigint, c4 bigint, c5 bigint)")
          cur.execute("create index on t(c1)")
          cur.execute("create index on t(c2)")
          cur.execute("create index on t(c3)")
          cur.execute("create index on t(c4)")
          cur.execute("create index on t(c5)")
          cur.execute("insert into t values (generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000),generate_series(1,1000000))")
          cur.execute("insert into t values (generate_series(1,1000000),random()*1000000,random()*1000000,random()*1000000,random()*1000000)")
