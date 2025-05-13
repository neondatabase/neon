"""
File with secondary->primary promotion testing.

This far, only contains a test that we don't break and that the data is persisted.
"""

import psycopg2
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_replica_caughtup
from fixtures.pg_version import PgVersion
from pytest import raises


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
        primary_cur.execute("select pg_switch_wal()")
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

    wait_replica_caughtup(primary, secondary)
    primary.stop()

    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()

    if env.pg_version is PgVersion.V14:
        signalfile = secondary.pgdata_dir / "standby.signal"
        assert signalfile.exists()
        signalfile.unlink()

        promoted = False
        while not promoted:
            with secondary.connect() as try_it:
                try_cursor = try_it.cursor()
                try_cursor.execute(
                    "SELECT setting FROM pg_settings WHERE name = 'transaction_read_only'"
                )
                promoted = ("off",) == try_cursor.fetchone()
    else:
        secondary_cur.execute("SELECT * FROM pg_promote()")
        assert secondary_cur.fetchone() == (True,)

    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()

    secondary_cur.execute(f"alter system set neon.safekeepers='{safekeepers}'")
    secondary_cur.execute("select pg_reload_conf()");

    new_primary_conn = secondary.connect()
    new_primary_cur = new_primary_conn.cursor()
    new_primary_cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200)")

    new_primary_cur.execute("select count(*) from t")
    assert new_primary_cur.fetchone() == (200,)

    secondary.stop(mode="immediate")

    primary.start()

    with primary.connect() as primary_conn:
        primary_cur = primary_conn.cursor()
        primary_cur.execute("select count(*) from t")
        assert primary_cur.fetchone() == (200,)
