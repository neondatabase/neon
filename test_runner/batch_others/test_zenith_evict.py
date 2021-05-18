import pytest
import os
import getpass
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test pgbench with zenith_test_evict setting
#
def test_evict(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run(["branch", "test_evict", "empty"]);
    pg = postgres.create_start('test_evict', ['zenith_test_evict=true'])
    print('postgres is running on test_evict branch')

    connstr = pg.connstr();

    print('run pgbench init')
    pg_bin.run_capture(['pgbench', '-i', connstr])
    print('run pgbench')
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 5 -P 1 -M prepared'.split() + [connstr])
