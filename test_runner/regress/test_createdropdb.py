from __future__ import annotations

import os
import pathlib

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, check_restored_datadir_content
from fixtures.pg_version import PgVersion
from fixtures.utils import query_scalar


#
# Test CREATE DATABASE when there have been relmapper changes
#
@pytest.mark.parametrize("strategy", ["file_copy", "wal_log"])
def test_createdb(neon_simple_env: NeonEnv, strategy: str):
    env = neon_simple_env
    if env.pg_version == PgVersion.V14 and strategy == "wal_log":
        pytest.skip("wal_log strategy not supported on PostgreSQL 14")

    endpoint = env.endpoints.create_start("main")

    with endpoint.cursor() as cur:
        # Cause a 'relmapper' change in the original branch
        cur.execute("VACUUM FULL pg_class")

        if env.pg_version == PgVersion.V14:
            cur.execute("CREATE DATABASE foodb")
        else:
            cur.execute(f"CREATE DATABASE foodb STRATEGY={strategy}")

        lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create a branch
    env.create_branch("test_createdb2", ancestor_branch_name="main", ancestor_start_lsn=lsn)
    endpoint2 = env.endpoints.create_start("test_createdb2")

    # Test that you can connect to the new database on both branches
    for db in (endpoint, endpoint2):
        with db.cursor(dbname="foodb") as cur:
            # Check database size in both branches
            cur.execute(
                """
                select pg_size_pretty(pg_database_size('foodb')),
                pg_size_pretty(
                sum(pg_relation_size(oid, 'main'))
                +sum(pg_relation_size(oid, 'vm'))
                +sum(pg_relation_size(oid, 'fsm'))
                ) FROM pg_class where relisshared is false
                """
            )
            res = cur.fetchone()
            assert res is not None
            # check that dbsize equals sum of all relation sizes, excluding shared ones
            # This is how we define dbsize in neon for now
            assert res[0] == res[1]


#
# Test DROP DATABASE
#
def test_dropdb(neon_simple_env: NeonEnv, test_output_dir):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    with endpoint.cursor() as cur:
        cur.execute("CREATE DATABASE foodb")

        lsn_before_drop = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

        dboid = query_scalar(cur, "SELECT oid FROM pg_database WHERE datname='foodb';")

    with endpoint.cursor() as cur:
        cur.execute("DROP DATABASE foodb")

        cur.execute("CHECKPOINT")

        lsn_after_drop = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create two branches before and after database drop.
    env.create_branch(
        "test_before_dropdb", ancestor_branch_name="main", ancestor_start_lsn=lsn_before_drop
    )
    endpoint_before = env.endpoints.create_start("test_before_dropdb")

    env.create_branch(
        "test_after_dropdb", ancestor_branch_name="main", ancestor_start_lsn=lsn_after_drop
    )
    endpoint_after = env.endpoints.create_start("test_after_dropdb")

    # Test that database exists on the branch before drop
    endpoint_before.connect(dbname="foodb").close()

    # Test that database subdir exists on the branch before drop
    assert endpoint_before.pgdata_dir
    dbpath = pathlib.Path(endpoint_before.pgdata_dir) / "base" / str(dboid)
    log.info(dbpath)

    assert os.path.isdir(dbpath) is True

    # Test that database subdir doesn't exist on the branch after drop
    assert endpoint_after.pgdata_dir
    dbpath = pathlib.Path(endpoint_after.pgdata_dir) / "base" / str(dboid)
    log.info(dbpath)

    assert os.path.isdir(dbpath) is False

    # Check that we restore the content of the datadir correctly
    check_restored_datadir_content(test_output_dir, env, endpoint)
