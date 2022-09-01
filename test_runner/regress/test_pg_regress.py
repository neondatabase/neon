#
# This file runs pg_regress-based tests.
#
import os
from pathlib import Path

import pytest
from fixtures.neon_fixtures import NeonEnv, base_dir, check_restored_datadir_content, pg_distrib_dir


# Run the main PostgreSQL regression tests, in src/test/regress.
#
# This runs for a long time, especially in debug mode, so use a larger-than-default
# timeout.
@pytest.mark.timeout(1800)
def test_pg_regress(neon_simple_env: NeonEnv, test_output_dir: Path, pg_bin, capsys):
    env = neon_simple_env

    env.neon_cli.create_branch("test_pg_regress", "empty")
    # Connect to postgres and create a database called "regression".
    pg = env.postgres.create_start("test_pg_regress")
    pg.safe_psql("CREATE DATABASE regression")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    build_path = os.path.join(pg_distrib_dir, "build/src/test/regress")
    src_path = os.path.join(base_dir, "vendor/postgres/src/test/regress")
    bindir = os.path.join(pg_distrib_dir, "bin")
    schedule = os.path.join(src_path, "parallel_schedule")
    pg_regress = os.path.join(build_path, "pg_regress")

    pg_regress_command = [
        pg_regress,
        '--bindir=""',
        "--use-existing",
        "--bindir={}".format(bindir),
        "--dlpath={}".format(build_path),
        "--schedule={}".format(schedule),
        "--inputdir={}".format(src_path),
    ]

    env_vars = {
        "PGPORT": str(pg.default_options["port"]),
        "PGUSER": pg.default_options["user"],
        "PGHOST": pg.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

        # checkpoint one more time to ensure that the lsn we get is the latest one
        pg.safe_psql("CHECKPOINT")

        # Check that we restore the content of the datadir correctly
        check_restored_datadir_content(test_output_dir, env, pg)


# Run the PostgreSQL "isolation" tests, in src/test/isolation.
#
# This runs for a long time, especially in debug mode, so use a larger-than-default
# timeout.
@pytest.mark.timeout(1800)
def test_isolation(neon_simple_env: NeonEnv, test_output_dir: Path, pg_bin, capsys):
    env = neon_simple_env

    env.neon_cli.create_branch("test_isolation", "empty")
    # Connect to postgres and create a database called "regression".
    # isolation tests use prepared transactions, so enable them
    pg = env.postgres.create_start("test_isolation", config_lines=["max_prepared_transactions=100"])
    pg.safe_psql("CREATE DATABASE isolation_regression")

    # Create some local directories for pg_isolation_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_isolation_regress will need.
    build_path = os.path.join(pg_distrib_dir, "build/src/test/isolation")
    src_path = os.path.join(base_dir, "vendor/postgres/src/test/isolation")
    bindir = os.path.join(pg_distrib_dir, "bin")
    schedule = os.path.join(src_path, "isolation_schedule")
    pg_isolation_regress = os.path.join(build_path, "pg_isolation_regress")

    pg_isolation_regress_command = [
        pg_isolation_regress,
        "--use-existing",
        "--bindir={}".format(bindir),
        "--dlpath={}".format(build_path),
        "--inputdir={}".format(src_path),
        "--schedule={}".format(schedule),
    ]

    env_vars = {
        "PGPORT": str(pg.default_options["port"]),
        "PGUSER": pg.default_options["user"],
        "PGHOST": pg.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_isolation_regress_command, env=env_vars, cwd=runpath)


# Run extra Neon-specific pg_regress-based tests. The tests and their
# schedule file are in the sql_regress/ directory.
def test_sql_regress(neon_simple_env: NeonEnv, test_output_dir: Path, pg_bin, capsys):
    env = neon_simple_env

    env.neon_cli.create_branch("test_sql_regress", "empty")
    # Connect to postgres and create a database called "regression".
    pg = env.postgres.create_start("test_sql_regress")
    pg.safe_psql("CREATE DATABASE regression")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    # This test runs neon specific tests
    build_path = os.path.join(pg_distrib_dir, "build/src/test/regress")
    src_path = os.path.join(base_dir, "test_runner/sql_regress")
    bindir = os.path.join(pg_distrib_dir, "bin")
    schedule = os.path.join(src_path, "parallel_schedule")
    pg_regress = os.path.join(build_path, "pg_regress")

    pg_regress_command = [
        pg_regress,
        "--use-existing",
        "--bindir={}".format(bindir),
        "--dlpath={}".format(build_path),
        "--schedule={}".format(schedule),
        "--inputdir={}".format(src_path),
    ]

    env_vars = {
        "PGPORT": str(pg.default_options["port"]),
        "PGUSER": pg.default_options["user"],
        "PGHOST": pg.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

        # checkpoint one more time to ensure that the lsn we get is the latest one
        pg.safe_psql("CHECKPOINT")
        pg.safe_psql("select pg_current_wal_insert_lsn()")[0][0]

        # Check that we restore the content of the datadir correctly
        check_restored_datadir_content(test_output_dir, env, pg)
