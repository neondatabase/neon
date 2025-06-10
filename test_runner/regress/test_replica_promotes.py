"""
File with secondary->primary promotion testing.

This far, only contains a test that we don't break and that the data is persisted.
"""

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_replica_caughtup
from fixtures.utils import USE_LFC
from fixtures.pg_version import PgVersion
from pytest import raises
from enum import Enum


class PromoteMethod(Enum):
    COMPUTE_CTL = False
    POSTGRES = True


QUERY_OPTIONS = PromoteMethod.POSTGRES, PromoteMethod.COMPUTE_CTL


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("query", QUERY_OPTIONS, ids=["postgres", "compute-ctl"])
def test_replica_promotes(neon_simple_env: NeonEnv, pg_version: PgVersion, query: PromoteMethod):
    """
    Test that a replica safely promotes, and can commit data updates which
    show up when the primary boots up after the promoted secondary endpoint
    shut down.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env: NeonEnv = neon_simple_env
    primary: Endpoint = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    secondary: Endpoint = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")

    with primary.connect() as primary_conn:

        primary_cur = primary_conn.cursor()

        # TODO remove? default neon version should be 1.6
        if query is PromoteMethod.COMPUTE_CTL:
            primary_cur.execute("create extension neon version '1.6'")

        primary_cur.execute(
            "create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)"
        )
        primary_cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")
        primary_cur.execute(
            """
            SELECT pg_current_wal_insert_lsn(),
                   pg_current_wal_lsn(),
                   pg_current_wal_flush_lsn()
            """
        )
        log.info(f"Primary: Current LSN after workload is {primary_cur.fetchone()}")
        primary_cur.execute("show neon.safekeepers")
        safekeepers = primary_cur.fetchall()[0][0]

    http_client = primary.http_client()
    if query is PromoteMethod.COMPUTE_CTL:
        safekeepers_lsn = http_client.safekeepers_lsn()
        http_client.offload_lfc()  # /promote depends on secondary being prewarmed
    else:
        wait_replica_caughtup(primary, secondary)

    with secondary.connect() as secondary_conn:
        secondary_cur = secondary_conn.cursor()

        secondary_cur.execute("select count(*) from t")

        assert secondary_cur.fetchone() == (100,)

        with raises(psycopg2.Error):
            secondary_cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200)")
            secondary_conn.commit()

        secondary_conn.rollback()
        secondary_cur.execute("select count(*) from t")
        assert secondary_cur.fetchone() == (100,)

    primary_endpoint_id = primary.endpoint_id
    primary.stop_and_destroy(mode="immediate")

    # Reconnect to the secondary to make sure we get a read-write connection
    http_client = secondary.http_client()
    if query is PromoteMethod.COMPUTE_CTL:
        # TODO Do we need to prewarm if LFC is full?
        # PG:2025-06-10 15:54:36.203 GMT [1079329] LOG:  LFC: skip prewarm because LFC is already filled
        http_client.prewarm_lfc(primary_endpoint_id)
        promote = http_client.promote(safekeepers_lsn)
        assert promote["status"] == "completed"
        assert "error" not in promote
    else:
        conn = secondary.connect()
        cur = conn.cursor()  # can't use with: ALTER SYSTEM cannot run inside a transaction block
        cur.execute(f"alter system set neon.safekeepers='{safekeepers}'")
        cur.execute("select pg_reload_conf()")
        cur.execute("SELECT * FROM pg_promote()")
        assert cur.fetchone() == (True,)
        cur.execute(
            """
                SELECT pg_current_wal_insert_lsn(),
                       pg_current_wal_lsn(),
                       pg_current_wal_flush_lsn()
                """
        )
        log.info(f"Secondary: LSN after promotion is {cur.fetchone()}")

    # Reconnect to the secondary to make sure we get a read-write connection
    with secondary.connect() as conn, conn.cursor() as new_primary_cur:
        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (100,)

        new_primary_cur.execute(
            "INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload"
        )
        assert new_primary_cur.fetchall() == [(it,) for it in range(101, 201)]

        new_primary_cur = conn.cursor()
        new_primary_cur.execute("select payload from t")
        assert new_primary_cur.fetchall() == [(it,) for it in range(1, 201)]

        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (200,)
        new_primary_cur.execute(
            """
            SELECT pg_current_wal_insert_lsn(),
                   pg_current_wal_lsn(),
                   pg_current_wal_flush_lsn()
            """
        )
        log.info(f"Secondary: LSN after workload is {new_primary_cur.fetchone()}")

    with secondary.connect() as conn, conn.cursor() as new_primary_cur:
        new_primary_cur.execute("select payload from t")
        assert new_primary_cur.fetchall() == [(it,) for it in range(1, 201)]

    secondary.stop_and_destroy()

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")

    with primary.connect() as new_primary:
        new_primary_cur = new_primary.cursor()
        new_primary_cur.execute(
            """
            SELECT pg_current_wal_insert_lsn(),
                   pg_current_wal_lsn(),
                   pg_current_wal_flush_lsn()
            """
        )
        log.info(f"New primary: Boot LSN is {new_primary_cur.fetchone()}")

        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (200,)
        new_primary_cur.execute("INSERT INTO t (payload) SELECT generate_series(201, 300)")
        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (300,)

    primary.stop(mode="immediate")
