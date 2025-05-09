"""
File with secondary->primary promotion testing.

This far, only contains a test that we don't break and that the data is persisted.
"""

from __future__ import annotations

import threading
from contextlib import closing

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint, NeonEnv, wait_for_last_flush_lsn, wait_replica_caughtup
from fixtures.pg_version import PgVersion
from fixtures.utils import query_scalar, skip_on_postgres, wait_until

def test_replica_promotes(neon_simple_env: NeonEnv):
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
        primary_cur.execute("create table t(pk bigint GENERATED ALWAYS AS IDENTITY, payload integer)")
        primary_cur.execute("INSERT INTO t(payload) SELECT generate_series(1, 100)")
        primary_cur.execute("select pg_switch_wal()")

    wait_replica_caughtup(primary, secondary)

    with secondary.connect() as secondary_conn:
        secondary_cur = secondary_conn.cursor()
        secondary_cur.execute("select count(*) from t")

        assert secondary_cur.fetchone() == (100,)
        
        with pytest.raises():
            secondary_cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200)")
            secondary_cur.commit()

        secondary_cur.execute("select count(*) from t")
        assert secondary_cur.fetchone() == (100,)

        wait_replica_caughtup(primary, secondary)
        primary.stop(mode="immediate")

        secondary_cur = secondary_conn.cursor()

        secondary_cur.execute("SELECT * FROM pg_promote()")
        assert secondary_cur.fetchone() == (True,)

        secondary_cur.execute("INSERT INTO t (payload) SELECT generate_series(101, 200)")
        secondary_cur.fetchall()

        secondary_cur.execute("select count(*) from t")
        assert secondary_cur.fetchone() == (200,)

    secondary.stop(mode="immediate")
    
    primary.start()

    with primary.connect() as primary_conn:
        primary_cur = primary_conn.cursor()
        primary_cur.execute("select count(*) from t")
        assert primary_cur.fetchone() == (200,)
