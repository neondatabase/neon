import pytest
from fixtures.utils import mkdir_if_needed
import getpass
import os
import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")

def test_zenith_regress(pageserver, postgres, pg_bin, zenith_cli, test_output_dir, pg_distrib_dir, base_dir, capsys):

    # Create a branch for us
    zenith_cli.run(["branch", "test_zenith_regress", "empty"]);

    # Connect to postgres and create a database called "regression".
    pg = postgres.create_start('test_zenith_regress')
    pg_conn = psycopg2.connect(pg.connstr())
    pg_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = pg_conn.cursor()
    cur.execute('CREATE DATABASE regression')
    pg_conn.close()

    # Create some local directories for pg_regress to run in.
    runpath = os.path.join(test_output_dir, 'regress')
    mkdir_if_needed(runpath)
    mkdir_if_needed(os.path.join(runpath, 'testtablespace'))

    # Compute all the file locations that pg_regress will need.
    # This test runs zenith specific tests
    build_path = os.path.join(
        pg_distrib_dir, 'build/src/test/regress')
    src_path = os.path.join(
        base_dir, 'test_runner/zenith_regress')
    bindir = os.path.join(pg_distrib_dir, 'bin')
    schedule = os.path.join(src_path, 'parallel_schedule')
    pg_regress = os.path.join(build_path, 'pg_regress')

    pg_regress_command = [
        pg_regress,
        '--use-existing',
        '--bindir={}'.format(bindir),
        '--dlpath={}'.format(build_path),
        '--schedule={}'.format(schedule),
        '--inputdir={}'.format(src_path),
    ]

    print(pg_regress_command)
    env = {
        'PGPORT': str(pg.port),
        'PGUSER': pg.username,
        'PGHOST': pg.host,
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env, cwd=runpath)
