#
# This file runs pg_regress-based tests.
#
from pathlib import Path
from typing import Optional

import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    check_restored_datadir_content,
)
from fixtures.remote_storage import s3_storage


# Run the main PostgreSQL regression tests, in src/test/regress.
#
@pytest.mark.parametrize("shard_count", [None, 4])
def test_pg_regress(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin,
    capsys,
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: Optional[int],
):
    """
    :param shard_count: if None, create an unsharded tenant.  Otherwise create a tenant with this
                        many shards.
    """
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Connect to postgres and create a database called "regression".
    endpoint = env.endpoints.create_start(
        "main", config_lines=["max_stack_depth = 4096kB"]
    )  # stack should be increased for tests to pass with asan
    endpoint.safe_psql("CREATE DATABASE regression")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    build_path = pg_distrib_dir / f"build/{env.pg_version.v_prefixed}/src/test/regress"
    src_path = base_dir / f"vendor/postgres-{env.pg_version.v_prefixed}/src/test/regress"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "parallel_schedule"
    pg_regress = build_path / "pg_regress"

    pg_regress_command = [
        str(pg_regress),
        '--bindir=""',
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--schedule={schedule}",
        f"--inputdir={src_path}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

        check_restored_datadir_content(test_output_dir, env, endpoint)


# Run the PostgreSQL "isolation" tests, in src/test/isolation.
#
@pytest.mark.parametrize("shard_count", [None, 4])
def test_isolation(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin,
    capsys,
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: Optional[int],
):
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Connect to postgres and create a database called "regression".
    # isolation tests use prepared transactions, so enable them
    endpoint = env.endpoints.create_start(
        "main", config_lines=["max_prepared_transactions=100", "max_stack_depth = 4096kB"]
    )
    endpoint.safe_psql("CREATE DATABASE isolation_regression")

    # Create some local directories for pg_isolation_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_isolation_regress will need.
    build_path = pg_distrib_dir / f"build/{env.pg_version.v_prefixed}/src/test/isolation"
    src_path = base_dir / f"vendor/postgres-{env.pg_version.v_prefixed}/src/test/isolation"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "isolation_schedule"
    pg_isolation_regress = build_path / "pg_isolation_regress"

    pg_isolation_regress_command = [
        str(pg_isolation_regress),
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--inputdir={src_path}",
        f"--schedule={schedule}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_isolation_regress_command, env=env_vars, cwd=runpath)


# Run extra Neon-specific pg_regress-based tests. The tests and their
# schedule file are in the sql_regress/ directory.
@pytest.mark.parametrize("shard_count", [None, 4])
def test_sql_regress(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin,
    capsys,
    base_dir: Path,
    pg_distrib_dir: Path,
    shard_count: Optional[int],
):
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()
    env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)

    # Connect to postgres and create a database called "regression".
    endpoint = env.endpoints.create_start("main")
    endpoint.safe_psql("CREATE DATABASE regression")

    # Create some local directories for pg_regress to run in.
    runpath = test_output_dir / "regress"
    (runpath / "testtablespace").mkdir(parents=True)

    # Compute all the file locations that pg_regress will need.
    # This test runs neon specific tests
    build_path = pg_distrib_dir / f"build/v{env.pg_version}/src/test/regress"
    src_path = base_dir / "test_runner/sql_regress"
    bindir = pg_distrib_dir / f"v{env.pg_version}/bin"
    schedule = src_path / "parallel_schedule"
    pg_regress = build_path / "pg_regress"

    pg_regress_command = [
        str(pg_regress),
        "--use-existing",
        f"--bindir={bindir}",
        f"--dlpath={build_path}",
        f"--schedule={schedule}",
        f"--inputdir={src_path}",
    ]

    env_vars = {
        "PGPORT": str(endpoint.default_options["port"]),
        "PGUSER": endpoint.default_options["user"],
        "PGHOST": endpoint.default_options["host"],
    }

    # Run the command.
    # We don't capture the output. It's not too chatty, and it always
    # logs the exact same data to `regression.out` anyway.
    with capsys.disabled():
        pg_bin.run(pg_regress_command, env=env_vars, cwd=runpath)

        check_restored_datadir_content(test_output_dir, env, endpoint)
