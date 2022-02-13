import os

from fixtures.utils import mkdir_if_needed
from fixtures.zenith_fixtures import (ZenithEnv,
                                      check_restored_datadir_content,
                                      base_dir,
                                      pg_distrib_dir)
from fixtures.log_helper import log


def test_zenith_regress(zenith_simple_env: ZenithEnv, test_output_dir, pg_bin, capsys):
    env = zenith_simple_env

    new_timeline_id = env.zenith_cli.branch_timeline()
    # Connect to postgres and create a database called "regression".
    pg = env.postgres.create_start('test_zenith_regress', timeline_id=new_timeline_id)
    pg.safe_psql('CREATE DATABASE regression')

    # Create some local directories for pg_regress to run in.
    runpath = os.path.join(test_output_dir, 'regress')
    mkdir_if_needed(runpath)
    mkdir_if_needed(os.path.join(runpath, 'testtablespace'))

    # Compute all the file locations that pg_regress will need.
    # This test runs zenith specific tests
    build_path = os.path.join(pg_distrib_dir, 'build/src/test/regress')
    src_path = os.path.join(base_dir, 'test_runner/zenith_regress')
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

    log.info(pg_regress_command)
    env_vars = {
        'PGPORT': str(pg.port),
        'PGUSER': pg.username,
        'PGHOST': pg.host,
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

        # checkpoint one more time to ensure that the lsn we get is the latest one
        pg.safe_psql('CHECKPOINT')
        lsn = pg.safe_psql('select pg_current_wal_insert_lsn()')[0][0]

        # Check that we restore the content of the datadir correctly
        check_restored_datadir_content(test_output_dir, env, pg)
