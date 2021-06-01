import psycopg2
import time

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test restarting and recreating a postgres instance
#
def test_restart_compute(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_restart_compute", "empty"])

    pg = postgres.create_start('test_restart_compute')
    print("postgres is running on 'test_restart_compute' branch")

    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    # Create table, and insert a row
    cur.execute('CREATE TABLE foo (t text)')
    cur.execute("INSERT INTO foo VALUES ('bar')")

    # Stop and restart the Postgres instance
    pg_conn.close()
    pg.stop()
    pg.start()
    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    # We can still see the row
    cur.execute('SELECT count(*) FROM foo')
    assert cur.fetchone() == (1, )

    # Insert another row
    cur.execute("INSERT INTO foo VALUES ('bar2')")
    cur.execute('SELECT count(*) FROM foo')
    assert cur.fetchone() == (2, )

    # FIXME: Currently, there is no guarantee that by the time the INSERT commits, the WAL
    # has been streamed safely to the WAL safekeeper or page server. It is merely stored
    # on the Postgres instance's local disk. Sleep a little, to give it time to be
    # streamed. This should be removed, when we have the ability to run the Postgres
    # instance -> safekeeper streaming in synchronous mode.
    time.sleep(5)

    # Stop, and destroy the Postgres instance. Then recreate and restart it.
    pg_conn.close()
    pg.stop_and_destroy()
    pg.create_start('test_restart_compute')
    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    # We can still see the rows
    cur.execute('SELECT count(*) FROM foo')
    assert cur.fetchone() == (2, )
