import pytest
import os
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test starting Postgres with custom options
#
def test_config(zenith_cli, pageserver, postgres, pg_bin):
    # Create a branch for us
    zenith_cli.run(["branch", "test_config", "empty"]);

    # change config
    pg = postgres.create_start('test_config', ['log_min_messages=debug1'])
    print('postgres is running on test_config branch')

    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()

    #check that config change was applied
    cur.execute('SELECT name, setting from pg_settings WHERE source!=%s and source!=%s', ("default","override",))
    for record in cur:
        if record[0] == 'log_min_messages':
            assert(record[1] == 'debug1')

    pg_conn.close()
