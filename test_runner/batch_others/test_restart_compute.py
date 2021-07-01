from contextlib import closing

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test restarting and recreating a postgres instance
#
def test_restart_compute(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_restart_compute", "empty"])

    pg = postgres.create_start('test_restart_compute')
    print("postgres is running on 'test_restart_compute' branch")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # Create table, and insert a row
            cur.execute('CREATE TABLE foo (t text)')
            cur.execute("INSERT INTO foo VALUES ('bar')")

    # Stop and restart the Postgres instance
    pg.stop_and_destroy().create_start('test_restart_compute')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the row
            cur.execute('SELECT count(*) FROM foo')
            assert cur.fetchone() == (1, )

            # Insert another row
            cur.execute("INSERT INTO foo VALUES ('bar2')")
            cur.execute('SELECT count(*) FROM foo')
            assert cur.fetchone() == (2, )

    # Stop, and destroy the Postgres instance. Then recreate and restart it.
    pg.stop_and_destroy().create_start('test_restart_compute')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # We can still see the rows
            cur.execute('SELECT count(*) FROM foo')
            assert cur.fetchone() == (2, )
