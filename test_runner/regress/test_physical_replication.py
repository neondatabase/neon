from __future__ import annotations

import random
import threading
import time
from typing import TYPE_CHECKING

from fixtures.log_helper import log

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_physical_replication(neon_simple_env: NeonEnv):
    env = neon_simple_env
    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute(
                    "CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))"
                )
        time.sleep(1)
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    with secondary.connect() as s_con:
                        with s_con.cursor() as s_cur:
                            runtime_secs = 30
                            started_at = time.time()
                            pk = 0
                            while True:
                                pk += 1
                                now = time.time()
                                if now - started_at > runtime_secs:
                                    break
                                p_cur.execute("insert into t (pk) values (%s)", (pk,))
                                # an earlier version of this test was based on a fixed number of loop iterations
                                # and selected for pk=(random.randrange(1, fixed number of loop iterations)).
                                # => the probability of selection for a value that was never inserted changed from 99.9999% to 0% over the course of the test.
                                #
                                # We changed the test to where=(random.randrange(1, 2*pk)), which means the probability is now fixed to 50%.
                                s_cur.execute(
                                    "select * from t where pk=%s", (random.randrange(1, 2 * pk),)
                                )


def test_physical_replication_config_mismatch_max_connections(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_connections).
    It should normally work because such small difference in maximal number of backends can not cause
    problems during recovery.
    """
    env = neon_simple_env
    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute(
                    "CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))"
                )
        time.sleep(1)

        with env.endpoints.new_replica_start(
            origin=primary,
            endpoint_id="secondary",
            config_lines=["max_connections=5"],
        ) as secondary:
            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    cursors = []
                    for i in range(10):
                        p_con = primary.connect()
                        p_cur = p_con.cursor()
                        p_cur.execute("begin")
                        p_cur.execute("insert into t (pk) values (%s)", (i,))
                        cursors.append(p_cur)

                    for p_cur in cursors:
                        p_cur.execute("commit")

                    time.sleep(5)
                    s_cur.execute("select count(*) from t")
                    assert s_cur.fetchall()[0][0] == 10


def test_physical_replication_config_mismatch_max_prepared(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_prepared_transactions).
    If number of transaction at primary exceeds it's limit at replica then walreceiver is terminated.
    """
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=["max_prepared_transactions=10"],
    )
    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))")
    time.sleep(1)

    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=["max_prepared_transactions=5"],
    )
    s_con = secondary.connect()
    s_cur = s_con.cursor()
    cursors = []
    for i in range(10):
        p_con = primary.connect()
        p_cur = p_con.cursor()
        p_cur.execute("begin")
        p_cur.execute("insert into t (pk) values (%s)", (i,))
        p_cur.execute(f"prepare transaction 't{i}'")
        cursors.append(p_cur)

    for i in range(10):
        cursors[i].execute(f"commit prepared 't{i}'")

    time.sleep(5)
    try:
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchall()[0][0] == 10
        secondary.stop()
    except Exception as e:
        log.info(f"Replica crashed with {e}")
        assert secondary.log_contains("maximum number of prepared transactions reached")


def do_txn(ep, i):
    max_reconnect_attempts = 10
    for _ in range(max_reconnect_attempts):
        try:
            con = ep.connect()
            break
        except Exception as e:
            log.info(f"Failed to connect with primary: {e}")
            time.sleep(1)
    cur = con.cursor()
    cur.execute("begin")
    cur.execute(f"insert into t values ({i})")
    time.sleep(10)
    cur.execute("commit")


def test_physical_replication_config_mismatch_too_many_known_xids(neon_simple_env: NeonEnv):
    """
    Test for primary and replica with different configuration settings (max_connections).
    In this case large difference in this setting and larger number of concurrent trsanctions at primary
    # cause too many known xids error  at replica.
    """
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", config_lines=["max_connections=1000"]
    )
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=[
            "max_connections=2",
            "autovacuum_max_workers=1",
            "max_worker_processes=5",
            "max_wal_senders=1",
            "superuser_reserved_connections=0",
        ],
    )

    p_con = primary.connect()
    p_cur = p_con.cursor()
    p_cur.execute("CREATE TABLE t(x integer)")

    threads = []
    n_threads = 990
    for i in range(n_threads):
        t = threading.Thread(
            target=do_txn,
            args=(
                primary,
                i,
            ),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    time.sleep(5)
    try:
        s_con = secondary.connect()
        s_cur = s_con.cursor()
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchall()[0][0] == n_threads
        secondary.stop()
    except Exception as e:
        log.info(f"Replica crashed with {e}")
        assert secondary.log_contains("too many KnownAssignedXids")
