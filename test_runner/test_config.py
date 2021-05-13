import pytest
import os
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


def test_config(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run_init()
    pageserver.start()
    print('pageserver is running')

    # change config
    postgres.create_start(['log_min_messages=debug1'])

    print('postgres is running')

    username = getpass.getuser()
    conn_str = 'host={} port={} dbname=postgres user={}'.format(
        postgres.host, postgres.port, username)
    print('conn_str is', conn_str)
    pg_conn = psycopg2.connect(conn_str)
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    #check that config change was applied
    cur.execute('SELECT name, setting from pg_settings WHERE source!=%s and source!=%s', ("default","override",))
    for record in cur:
        if record[0] == 'log_min_messages':
            assert(record[1] == 'debug1')

    pg_conn.close()
