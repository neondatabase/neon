"""
Secondary -> primary promotion testing
"""

from enum import StrEnum
from typing import cast

import psycopg2
import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_replica_caughtup
from fixtures.utils import USE_LFC
from psycopg2.extensions import cursor as Cursor
from pytest import raises


def stop_and_check_lsn(ep: Endpoint, expected_lsn: Lsn | None):
    ep.stop(mode="immediate-terminate")
    lsn = ep.terminate_flush_lsn
    assert (lsn is not None) == (expected_lsn is not None), f"{lsn=}, {expected_lsn=}"
    if lsn is not None:
        assert lsn >= expected_lsn, f"{expected_lsn=} < {lsn=}"


def get_lsn_triple(cur: Cursor) -> tuple[str, str, str]:
    cur.execute(
        """
        SELECT pg_current_wal_insert_lsn(),
               pg_current_wal_lsn(),
               pg_current_wal_flush_lsn()
        """
    )
    return cast("tuple[str, str, str]", cur.fetchone())


class PromoteMethod(StrEnum):
    COMPUTE_CTL = "compute-ctl"
    POSTGRES = "postgres"


METHOD_OPTIONS = [e for e in PromoteMethod]
METHOD_IDS = [e.value for e in PromoteMethod]


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("method", METHOD_OPTIONS, ids=METHOD_IDS)
def test_replica_promote(neon_simple_env: NeonEnv, method: PromoteMethod):
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
        primary_cur.execute("create schema neon;create extension neon with schema neon")
        primary_cur.execute(
            "create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)"
        )
        primary_cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")

        lsn_triple = get_lsn_triple(primary_cur)
        log.info(f"Primary: Current LSN after workload is {lsn_triple}")
        expected_primary_lsn: Lsn = Lsn(lsn_triple[2])
        primary_cur.execute("show neon.safekeepers")
        safekeepers = primary_cur.fetchall()[0][0]

    if method == PromoteMethod.COMPUTE_CTL:
        primary.http_client().offload_lfc()
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

    primary_spec = primary.get_compute_spec()
    primary_endpoint_id = primary.endpoint_id
    stop_and_check_lsn(primary, expected_primary_lsn)

    # Reconnect to the secondary to make sure we get a read-write connection
    promo_conn = secondary.connect()
    promo_cur = promo_conn.cursor()
    if method == PromoteMethod.COMPUTE_CTL:
        client = secondary.http_client()
        client.prewarm_lfc(primary_endpoint_id)
        assert (lsn := primary.terminate_flush_lsn)
        promote_spec = {"spec": primary_spec, "wal_flush_lsn": str(lsn)}
        assert client.promote(promote_spec)["status"] == "completed"
    else:
        promo_cur.execute(f"alter system set neon.safekeepers='{safekeepers}'")
        promo_cur.execute("select pg_reload_conf()")
        promo_cur.execute("SELECT * FROM pg_promote()")
        assert promo_cur.fetchone() == (True,)

    lsn_triple = get_lsn_triple(promo_cur)
    log.info(f"Secondary: LSN after promotion is {lsn_triple}")

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

        lsn_triple = get_lsn_triple(new_primary_cur)
        log.info(f"Secondary: LSN after workload is {lsn_triple}")
        expected_lsn = Lsn(lsn_triple[2])

    with secondary.connect() as conn, conn.cursor() as new_primary_cur:
        new_primary_cur.execute("select payload from t")
        assert new_primary_cur.fetchall() == [(it,) for it in range(1, 201)]

    if method == PromoteMethod.COMPUTE_CTL:
        # compute_ctl's /promote switches replica type to Primary so it syncs safekeepers on finish
        stop_and_check_lsn(secondary, expected_lsn)
    else:
        # on testing postgres, we don't update replica type, secondaries don't sync so lsn should be None
        stop_and_check_lsn(secondary, None)

    if method == PromoteMethod.COMPUTE_CTL:
        secondary.stop()
        # In production, compute ultimately receives new compute spec from cplane.
        secondary.respec(mode="Primary")
        secondary.start()

        with secondary.connect() as conn, conn.cursor() as new_primary_cur:
            new_primary_cur.execute(
                "INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload"
            )
            assert new_primary_cur.fetchall() == [(it,) for it in range(101, 201)]
            lsn_triple = get_lsn_triple(new_primary_cur)
            log.info(f"Secondary: LSN after restart and workload is {lsn_triple}")
            expected_lsn = Lsn(lsn_triple[2])
        stop_and_check_lsn(secondary, expected_lsn)

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary2")

    with primary.connect() as new_primary, new_primary.cursor() as new_primary_cur:
        lsn_triple = get_lsn_triple(new_primary_cur)
        expected_primary_lsn = Lsn(lsn_triple[2])
        log.info(f"New primary: Boot LSN is {lsn_triple}")

        new_primary_cur.execute("select count(*) from t")
        compute_ctl_count = 100 * (method == PromoteMethod.COMPUTE_CTL)
        assert new_primary_cur.fetchone() == (200 + compute_ctl_count,)
        new_primary_cur.execute("INSERT INTO t (payload) SELECT generate_series(201, 300)")
        new_primary_cur.execute("select count(*) from t")
        assert new_primary_cur.fetchone() == (300 + compute_ctl_count,)
    stop_and_check_lsn(primary, expected_primary_lsn)


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_replica_promote_handler_disconnects(neon_simple_env: NeonEnv):
    """
    Test that if a handler disconnects from /promote route of compute_ctl, promotion still happens
    once, and no error is thrown
    """
    env: NeonEnv = neon_simple_env
    primary: Endpoint = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    secondary: Endpoint = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")

    with primary.connect() as conn, conn.cursor() as cur:
        cur.execute("create schema neon;create extension neon with schema neon")
        cur.execute("create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)")
        cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")

    primary.http_client().offload_lfc()
    primary_spec = primary.get_compute_spec()
    primary_endpoint_id = primary.endpoint_id
    primary.stop(mode="immediate-terminate")
    assert (lsn := primary.terminate_flush_lsn)

    client = secondary.http_client()
    client.prewarm_lfc(primary_endpoint_id)
    promote_spec = {"spec": primary_spec, "wal_flush_lsn": str(lsn)}
    assert client.promote(promote_spec, disconnect=True)["status"] == "completed"

    with secondary.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (100,)
        cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload")
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (200,)


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_replica_promote_fails(neon_simple_env: NeonEnv):
    """
    Test that if a /promote route fails, we can safely start primary back
    """
    env: NeonEnv = neon_simple_env
    primary: Endpoint = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    secondary: Endpoint = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")
    secondary.stop()
    secondary.start(env={"FAILPOINTS": "compute-promotion=return(0)"})

    with primary.connect() as conn, conn.cursor() as cur:
        cur.execute("create schema neon;create extension neon with schema neon")
        cur.execute("create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)")
        cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")

    primary.http_client().offload_lfc()
    primary_spec = primary.get_compute_spec()
    primary_endpoint_id = primary.endpoint_id
    primary.stop(mode="immediate-terminate")
    assert (lsn := primary.terminate_flush_lsn)

    client = secondary.http_client()
    client.prewarm_lfc(primary_endpoint_id)
    promote_spec = {"spec": primary_spec, "wal_flush_lsn": str(lsn)}
    assert client.promote(promote_spec)["status"] == "failed"
    secondary.stop()

    primary.start()
    with primary.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (100,)
        cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload")
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (200,)


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_replica_promote_prewarm_fails(neon_simple_env: NeonEnv):
    """
    Test that if /lfc/prewarm route fails, we are able to promote
    """
    env: NeonEnv = neon_simple_env
    primary: Endpoint = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    secondary: Endpoint = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")
    secondary.stop()
    secondary.start(env={"FAILPOINTS": "compute-prewarm=return(0)"})

    with primary.connect() as conn, conn.cursor() as cur:
        cur.execute("create schema neon;create extension neon with schema neon")
        cur.execute("create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)")
        cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")

    primary.http_client().offload_lfc()
    primary_spec = primary.get_compute_spec()
    primary_endpoint_id = primary.endpoint_id
    primary.stop(mode="immediate-terminate")
    assert (lsn := primary.terminate_flush_lsn)

    client = secondary.http_client()
    with pytest.raises(AssertionError):
        client.prewarm_lfc(primary_endpoint_id)
    assert client.prewarm_lfc_status()["status"] == "failed"
    promote_spec = {"spec": primary_spec, "wal_flush_lsn": str(lsn)}
    assert client.promote(promote_spec)["status"] == "completed"

    with secondary.connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (100,)
        cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200) RETURNING payload")
        cur.execute("select count(*) from t")
        assert cur.fetchone() == (200,)
