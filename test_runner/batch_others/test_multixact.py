import pytest
import os
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test multixact state after branching
# Now this test is very minimalistic -
# it only checks next_multixact_id field in restored pg_control,
# since we don't have functions to check multixact internals.
#
def test_multixact(pageserver, postgres, pg_bin, zenith_cli, base_dir):

    # Create a branch for us
    zenith_cli.run(["branch", "test_multixact", "empty"])
    pg = postgres.create_start('test_multixact')

    print("postgres is running on 'test_multixact' branch")
    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    cur.execute('CREATE TABLE t1(i int primary key);'
    'INSERT INTO t1 select * from generate_series(1,100);')

    cur.execute('SELECT next_multixact_id FROM pg_control_checkpoint();')
    next_multixact_id_old = cur.fetchone()[0]

    # Lock entries in parallel connections to set multixact
    nclients = 3
    connections = []
    for i in range(nclients):
        con = psycopg2.connect(pg.connstr())
        # Do not turn on autocommit. We want to hold the key-share locks.
        con.cursor().execute('select * from t1 for key share;')
        connections.append(con)

    # We should have a multixact now. We can close the connections.
    for c in connections:
        c.close()

    # force wal flush
    cur.execute('checkpoint')

    cur.execute('SELECT next_multixact_id, pg_current_wal_flush_lsn() FROM pg_control_checkpoint();')
    res = cur.fetchone()
    next_multixact_id = res[0]
    lsn = res[1]

    # Ensure that we did lock some tuples
    assert(int(next_multixact_id) > int(next_multixact_id_old))

    # Branch at this point
    zenith_cli.run(["branch", "test_multixact_new", "test_multixact@"+lsn]);
    pg_new = postgres.create_start('test_multixact_new')

    print("postgres is running on 'test_multixact_new' branch")
    pg_new_conn = psycopg2.connect(pg_new.connstr())
    pg_new_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur_new = pg_new_conn.cursor()

    cur_new.execute('SELECT next_multixact_id FROM pg_control_checkpoint();')
    next_multixact_id_new = cur_new.fetchone()[0]

    # Check that we restored pg_controlfile correctly
    assert(next_multixact_id_new == next_multixact_id)
