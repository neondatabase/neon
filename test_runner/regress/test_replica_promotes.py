"""
File with secondary->primary promotion testing.

This far, only contains a test that we don't break and that the data is persisted.
"""

from typing import cast

import psycopg2
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_replica_caughtup
from fixtures.pg_version import PgVersion
from pytest import raises


def stop_and_check_lsn(ep: Endpoint, expected_lsn: Lsn | None):
    ep.stop(mode="immediate-terminate")
    lsn = ep.terminate_flush_lsn
    if expected_lsn is not None:
        assert lsn >= expected_lsn, f"{expected_lsn=} < {lsn=}"
    else:
        assert lsn == expected_lsn, f"{expected_lsn=} != {lsn=}"


def test_replica_promotes(neon_simple_env: NeonEnv, pg_version: PgVersion):
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
        lsn_triple = cast("tuple[str, str, str]", primary_cur.fetchone())
        log.info(f"Primary: Current LSN after workload is {lsn_triple}")
        expected_primary_lsn: Lsn = Lsn(lsn_triple[2])
        primary_cur.execute("show neon.safekeepers")
        safekeepers = primary_cur.fetchall()[0][0]

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

    stop_and_check_lsn(primary, expected_primary_lsn)

    # Reconnect to the secondary to make sure we get a read-write connection
    promo_conn = secondary.connect()
    promo_cur = promo_conn.cursor()
    promo_cur.execute(f"alter system set neon.safekeepers='{safekeepers}'")
    promo_cur.execute("select pg_reload_conf()")

    promo_cur.execute("SELECT * FROM pg_promote()")
    assert promo_cur.fetchone() == (True,)
    promo_cur.execute(
        """
            SELECT pg_current_wal_insert_lsn(),
                   pg_current_wal_lsn(),
                   pg_current_wal_flush_lsn()
            """
    )
    log.info(f"Secondary: LSN after promotion is {promo_cur.fetchone()}")

    # Reconnect to the secondary to make sure we get a read-write connection
    with secondary.connect() as new_primary_conn:
        new_primary_cur = new_primary_conn.cursor()
        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (100,)

        new_primary_cur.execute(
            "INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload"
        )
        assert new_primary_cur.fetchall() == [(it,) for it in range(101, 201)]

        new_primary_cur = new_primary_conn.cursor()
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

    with secondary.connect() as second_viewpoint_conn:
        new_primary_cur = second_viewpoint_conn.cursor()
        new_primary_cur.execute("select payload from t")
        assert new_primary_cur.fetchall() == [(it,) for it in range(1, 201)]

    # wait_for_last_flush_lsn(env, secondary, env.initial_tenant, env.initial_timeline)

    # secondaries don't sync safekeepers on finish so LSN will be None
    stop_and_check_lsn(secondary, None)

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary2")

    with primary.connect() as new_primary:
        new_primary_cur = new_primary.cursor()
        new_primary_cur.execute(
            """
            SELECT pg_current_wal_insert_lsn(),
                   pg_current_wal_lsn(),
                   pg_current_wal_flush_lsn()
            """
        )
        lsn_triple = cast("tuple[str, str, str]", new_primary_cur.fetchone())
        expected_primary_lsn = Lsn(lsn_triple[2])
        log.info(f"New primary: Boot LSN is {lsn_triple}")

        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (200,)
        new_primary_cur.execute("INSERT INTO t (payload) SELECT generate_series(201, 300)")
        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (300,)

    stop_and_check_lsn(primary, expected_primary_lsn)
