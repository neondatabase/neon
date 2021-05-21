#
# Test branching, when a transaction is in prepared state
#
import pytest
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

def test_twophase(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_twophase", "empty"]);

    pg = postgres.create_start('test_twophase', ['max_prepared_transactions=5'])
    print("postgres is running on 'test_twophase' branch")

    conn = psycopg2.connect(pg.connstr());
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute('CREATE TABLE foo (t text)');

    # Prepare a transaction that will insert a row
    cur.execute('BEGIN');
    cur.execute("INSERT INTO foo VALUES ('one')");
    cur.execute("PREPARE TRANSACTION 'insert_one'");

    # Prepare another transaction that will insert a row
    cur.execute('BEGIN');
    cur.execute("INSERT INTO foo VALUES ('two')");
    cur.execute("PREPARE TRANSACTION 'insert_two'");

    cur.execute('BEGIN');
    cur.execute("INSERT INTO foo VALUES ('three')");
    cur.execute("PREPARE TRANSACTION 'insert_three'");
    cur.execute("COMMIT PREPARED 'insert_three'");

    cur.execute('SELECT pg_current_wal_insert_lsn()');
    lsn = cur.fetchone()[0]

    # Create a branch with the transaction in prepared state
    zenith_cli.run(["branch", "test_twophase_prepared", "test_twophase@"+lsn]);

    pg2 = postgres.create_start('test_twophase_prepared', ['max_prepared_transactions=5'])
    conn2 = psycopg2.connect(pg2.connstr());
    conn2.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur2 = conn2.cursor()

    # On the new branch, commit one of the prepared transactions, abort the other one.
    cur2.execute("COMMIT PREPARED 'insert_one'");
    cur2.execute("ROLLBACK PREPARED 'insert_two'");

    cur2.execute('SELECT * FROM foo');
    assert(cur2.fetchall() == [('one',),('three',)]);

    # Neither insert is visible on the original branch, the transactions are still
    # in prepared state there.
    cur.execute('SELECT * FROM foo');
    assert(cur.fetchall() == [('three',)]);
