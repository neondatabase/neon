import pytest
import psycopg2
import getpass
import json

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

def test_pg_list(zen_simple):
    username = getpass.getuser()
    page_server_conn_str = 'host={} port={} dbname=postgres user={}'.format(
        HOST, PAGESERVER_PORT, username)
    page_server_conn = psycopg2.connect(page_server_conn_str)
    page_server_conn.autocommit = True
    page_server_cur = page_server_conn.cursor()

    page_server_cur.execute('pg_list;')
    branches = json.loads(page_server_cur.fetchone()[0])
    assert len(branches) == 1
    assert branches[0]['name'] == 'main'
    assert 'timeline_id' in branches[0]
    assert 'latest_valid_lsn' in branches[0]

    zen_simple.zenith_cli.run(['branch', 'experimental', 'main'])
    zen_simple.zenith_cli.run(['pg', 'create', 'experimental'])

    page_server_cur.execute('pg_list;')
    new_branches = json.loads(page_server_cur.fetchone()[0])
    assert len(new_branches) == 2
    new_branches.sort(key=lambda k: k['name'])

    assert new_branches[0]['name'] == 'experimental'
    assert new_branches[0]['timeline_id'] != branches[0]['timeline_id']

    # TODO: do the LSNs have to match here?
    assert new_branches[1] == branches[0]

    page_server_conn.close()
