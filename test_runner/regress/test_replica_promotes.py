"""
File with secondary->primary promotion testing.

This far, only contains a test that we don't break and that the data is persisted.
"""

from typing import cast
import psycopg2
from fixtures.common_types import Lsn
from enum import Enum
import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_replica_caughtup
from fixtures.utils import USE_LFC
from pytest import raises


def stop_and_check_lsn(ep: Endpoint, expected_lsn: Lsn | None):
    ep.stop(mode="immediate")
    lsn = ep.terminate_flush_lsn
    if expected_lsn is not None:
        assert lsn >= expected_lsn, f"{expected_lsn=} < {lsn=}"
    else:
        assert lsn == expected_lsn, f"{expected_lsn=} != {lsn=}"


class PromoteMethod(Enum):
    COMPUTE_CTL = False
    POSTGRES = True


QUERY_OPTIONS = PromoteMethod.POSTGRES, PromoteMethod.COMPUTE_CTL

# test two primaries
# test promote failed
# test promote handler disconnects
# test prewarm fails before promotion -> promotion should not be impacted


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
@pytest.mark.parametrize("query", QUERY_OPTIONS, ids=["postgres", "compute-ctl"])
def test_replica_promotes(neon_simple_env: NeonEnv, query: PromoteMethod):
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
        if query is PromoteMethod.COMPUTE_CTL:
            # TODO(myrrc): remove version
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
        lsn_triple = cast("tuple[str, str, str]", primary_cur.fetchone())
        log.info(f"Primary: Current LSN after workload is {lsn_triple}")
        expected_primary_lsn: Lsn = Lsn(lsn_triple[2])
        primary_cur.execute("show neon.safekeepers")
        safekeepers = primary_cur.fetchall()[0][0]

    if query is PromoteMethod.COMPUTE_CTL:
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

    primary_endpoint_id: str = primary.endpoint_id
    stop_and_check_lsn(primary, expected_primary_lsn)

    # Reconnect to the secondary to make sure we get a read-write connection
    promo_conn = secondary.connect()
    promo_cur = promo_conn.cursor()
    http_client = secondary.http_client()
    if query is PromoteMethod.COMPUTE_CTL:
        # TODO Do we need to prewarm if LFC is full?
        # PG:2025-06-10 15:54:36.203 GMT [1079329] LOG:  LFC: skip prewarm because LFC is already filled
        http_client.prewarm_lfc(primary_endpoint_id)

        # control plane knows safekeepers, here we simulate it by querying primary
        lsn = primary.terminate_flush_lsn
        assert lsn
        safekeepers_lsn = {"safekeepers": safekeepers, "wal_flush_lsn": lsn}
        promote = http_client.promote(safekeepers_lsn)
        assert promote["status"] == "completed"
    else:
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
        lsn_triple = cast("tuple[str, str, str]", new_primary_cur.fetchone())
        log.info(f"Secondary: LSN after workload is {lsn_triple}")
        expected_promoted_lsn = Lsn(lsn_triple[2])

    with secondary.connect() as conn, conn.cursor() as new_primary_cur:
        new_primary_cur.execute("select payload from t")
        assert new_primary_cur.fetchall() == [(it,) for it in range(1, 201)]

    secondary.stop()
    stop_and_check_lsn(secondary, expected_promoted_lsn)

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
