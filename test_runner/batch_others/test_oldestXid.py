import pytest
import getpass
import psycopg2
import time
import os

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test pg_control values after recreating a postgres instance
#
def test_oldestxid(zenith_cli, pageserver, postgres, pg_bin, repo_dir):
    zenith_cli.run(["branch", "test_oldestxid", "empty"]);

    pg = postgres.create_start('test_oldestxid')
    print("postgres is running on 'test_oldestxid' branch")

    pg_conn = psycopg2.connect(pg.connstr());
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    # Create table, and insert a row
    cur.execute('CREATE TABLE foo (t text)');
    cur.execute("INSERT INTO foo VALUES ('bar')");

    cur.execute('checkpoint')
    cur.execute('SELECT oldest_xid FROM pg_control_checkpoint();')
    oldest_xid = cur.fetchone()[0]

    # Stop, and destroy the Postgres instance. Then recreate and restart it.
    pg_conn.close();
    pg.stop();

    # capture old pg_controldata output for debugging purposes
    pgdatadir = os.path.join(repo_dir, 'pgdatadirs/test_oldestxid')
    pg_bin.run_capture(['pg_controldata', pgdatadir])

    pg.destroy();
    pg.create_start('test_oldestxid');
    pg_conn = psycopg2.connect(pg.connstr());
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    cur.execute('SELECT oldest_xid FROM pg_control_checkpoint();')
    oldest_xid_new = cur.fetchone()[0]

    assert(oldest_xid_new == oldest_xid)

    # capture new pg_controldata output for debugging purposes
    pgdatadir = os.path.join(repo_dir, 'pgdatadirs/test_oldestxid')
    pg_bin.run_capture(['pg_controldata', pgdatadir])