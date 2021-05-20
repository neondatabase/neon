import pytest
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test CREATE DATABASE when there have been relmapper changes
#
def test_createdb(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_createdb", "empty"]);

    pg = postgres.create_start('test_createdb')
    print("postgres is running on 'test_createdb' branch")

    conn = psycopg2.connect(pg.connstr());
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Cause a 'relmapper' change in the original branch
    cur.execute('VACUUM FULL pg_class');

    cur.execute('CREATE DATABASE foodb');

    conn.close();

    # Create a branch
    zenith_cli.run(["branch", "test_createdb2", "test_createdb"]);

    pg2 = postgres.create_start('test_createdb2')

    # Test that you can connect to the new database on both branches
    conn = psycopg2.connect(pg.connstr('foodb'));
    conn2 = psycopg2.connect(pg2.connstr('foodb'));
