import pytest
import psycopg2
import getpass

pytest_plugins = ("fixtures.zenith_fixtures")

HOST = 'localhost'
PAGESERVER_PORT = 64000

def test_status(zen_simple):
    username = getpass.getuser()
    conn_str = 'host={} port={} dbname=postgres user={}'.format(
        HOST, PAGESERVER_PORT, username)
    pg_conn = psycopg2.connect(conn_str)
    pg_conn.autocommit = True
    cur = pg_conn.cursor()
    cur.execute('status;')
    assert cur.fetchone() == ('hello world',)
    pg_conn.close()
