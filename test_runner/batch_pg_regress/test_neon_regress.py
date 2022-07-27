import os
from pathlib import Path

from fixtures.neon_fixtures import (NeonEnv,
                                    check_restored_datadir_content,
                                    base_dir,
                                    pg_distrib_dir)
from fixtures.log_helper import log


def test_neon_regress(neon_simple_env: NeonEnv, test_output_dir: Path, pg_bin, capsys):
    env = neon_simple_env

    env.neon_cli.create_branch("test_neon_regress", "empty")
    # Connect to postgres and create a database called "regression".
    pg = env.postgres.create_start('test_neon_regress')
    pg.safe_psql('CREATE DATABASE regression')

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / 'regress'
    (runpath / 'testtablespace').mkdir(parents=True)

    # Find the pg_regress binary and --bindir option to pass to it.
    proc = pg_bin.run(['pg_config', '--libdir'], capture_output=True)
    libdir = proc.stdout.decode().strip()
    proc = pg_bin.run(['pg_config', '--bindir'], capture_output=True)
    bindir = proc.stdout.decode().strip()
    pg_regress = os.path.join(libdir, 'postgresql/pgxs/src/test/regress/pg_regress')

    # Compute all the file locations that pg_regress will need.
    # This test runs neon specific tests
    src_path = os.path.join(base_dir, 'test_runner/neon_regress')
    schedule = os.path.join(src_path, 'parallel_schedule')

    pg_regress_command = [
        pg_regress,
        '--use-existing',
        '--bindir={}'.format(bindir),
        '--schedule={}'.format(schedule),
        '--inputdir={}'.format(src_path),
    ]

    log.info(pg_regress_command)
    env_vars = {
        'PGPORT': str(pg.default_options['port']),
        'PGUSER': pg.default_options['user'],
        'PGHOST': pg.default_options['host'],
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
