"""
In PostgreSQL, a standby always has to wait for a running-xacts WAL record to
arrive before it can start accepting queries. Furthermore, if there are
transactions with too many subxids (> 64) open to fit in the in-memory subxids
cache, the running-xacts record will be marked as "suboverflowed", and the
standby will need to also wait for the currently in-progress transactions to
finish.

In Neon, we have an additional mechanism that scans the CLOG at server startup
to determine the list of running transactions, so that the standby can start up
immediately without waiting for the running-xacts record, but that mechanism
only works if the # of active (sub-)transactions is reasonably small. Otherwise
it falls back to waiting. Furthermore, it's somewhat optimistic in using up the
known-assigned XIDs array: if too many transactions with subxids are started in
the primary later, the replay in the replica will crash with "too many
KnownAssignedXids" error.

This module contains tests for those various cases at standby startup: starting
from shutdown checkpoint, using the CLOG scanning mechanism, waiting for
running-xacts record and for in-progress transactions to finish etc.
"""

from __future__ import annotations

import threading
from contextlib import closing

import psycopg2
import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, PgBin, wait_for_last_flush_lsn, wait_replica_caughtup
from fixtures.pg_version import PgVersion
from fixtures.utils import query_scalar, skip_on_postgres, wait_until

CREATE_SUBXACTS_FUNC = """
create or replace function create_subxacts(n integer) returns void as $$
declare
   i integer;
begin
   for i in 1..n loop
      begin
         insert into t (payload) values (0);
      exception
         when others then
            raise exception 'caught something: %', sqlerrm;
      end;
   end loop;
end; $$ language plpgsql
"""


def test_replica_start_scan_clog(neon_simple_env: NeonEnv):
    """
    Test the CLOG-scanning mechanism at hot standby startup. There is one
    transaction active in the primary when the standby is started. The primary
    is killed before it has a chance to write a running-xacts record. The
    CLOG-scanning at neon startup allows the standby to start up anyway.

    See the module docstring for background.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("CREATE EXTENSION neon_test_utils")
    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)
    primary_cur.execute("select pg_switch_wal()")

    # Start a transaction in the primary. Leave the transaction open.
    #
    # The transaction has some subtransactions, but not too many to cause the
    # CLOG-scanning mechanism to give up.
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(50)")

    # Wait for the WAL to be flushed, but then immediately kill the primary,
    # before it has a chance to generate a running-xacts record.
    primary_cur.execute("select neon_xlogflush()")
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    primary.stop(mode="immediate")

    # Create a replica. It should start up normally, thanks to the CLOG-scanning
    # mechanism.
    secondary = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")

    # The transaction did not commit, so it should not be visible in the secondary
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (0,)


def test_replica_start_scan_clog_crashed_xids(neon_simple_env: NeonEnv):
    """
    Test the CLOG-scanning mechanism at hot standby startup, after
    leaving behind crashed transactions.

    See the module docstring for background.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    timeline_id = env.initial_timeline
    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)
    primary_cur.execute("select pg_switch_wal()")

    # Consume a lot of XIDs, then kill Postgres without giving it a
    # chance to write abort records for them.
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(100000)")
    primary.stop(mode="immediate", sks_wait_walreceiver_gone=(env.safekeepers, timeline_id))

    # Restart the primary. Do some light work, and shut it down cleanly
    primary.start()
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("insert into t (payload) values (0)")
    primary.stop(mode="fast")

    # Create a replica. It should start up normally, thanks to the CLOG-scanning
    # mechanism. (Restarting the primary writes a checkpoint and/or running-xacts
    # record, which allows the standby to know that the crashed XIDs are aborted)
    secondary = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")

    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (1,)


@skip_on_postgres(
    PgVersion.V14, reason="pg_log_standby_snapshot() function is available since Postgres 16"
)
@skip_on_postgres(
    PgVersion.V15, reason="pg_log_standby_snapshot() function is available since Postgres 16"
)
def test_replica_start_at_running_xacts(neon_simple_env: NeonEnv, pg_version):
    """
    Test that starting a replica works right after the primary has
    created a running-xacts record. This may seem like a trivial case,
    but during development, we had a bug that was triggered by having
    oldestActiveXid == nextXid. Starting right after a running-xacts
    record is one way to test that case.

    See the module docstring for background.
    """
    env = neon_simple_env

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()

    primary_cur.execute("CREATE EXTENSION neon_test_utils")
    primary_cur.execute("select pg_log_standby_snapshot()")
    primary_cur.execute("select neon_xlogflush()")
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)

    secondary = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")

    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select 123")
    assert secondary_cur.fetchone() == (123,)


def test_replica_start_wait_subxids_finish(neon_simple_env: NeonEnv):
    """
    Test replica startup when there are a lot of (sub)transactions active in the
    primary. That's too many for the CLOG-scanning mechanism to handle, so the
    replica has to wait for the large transaction to finish before it starts to
    accept queries.

    After replica startup, test MVCC with transactions that were in-progress
    when the replica was started.

    See the module docstring for background.
    """

    # Initialize the primary, a test table, and a helper function to create
    # lots of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)

    # Start a transaction with 100000 subtransactions, and leave it open. That's
    # too many to fit in the "known-assigned XIDs array" in the replica, and
    # also too many to fit in the subxid caches so the running-xacts record will
    # also overflow.
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(100000)")

    # Start another, smaller transaction in the primary. We'll come back to this
    # later.
    primary_conn2 = primary.connect()
    primary_cur2 = primary_conn2.cursor()
    primary_cur2.execute("begin")
    primary_cur2.execute("insert into t (payload) values (0)")

    # Create a replica. but before that, wait for the wal to be flushed to
    # safekeepers, so that the replica is started at a point where the large
    # transaction is already active. (The whole transaction might not be flushed
    # yet, but that's OK.)
    #
    # Start it in a separate thread, so that we can do other stuff while it's
    # blocked waiting for the startup to finish.
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    secondary = env.endpoints.new_replica(
        origin=primary,
        endpoint_id="secondary",
        config_lines=["neon.running_xacts_overflow_policy='wait'"],
    )
    start_secondary_thread = threading.Thread(target=secondary.start)
    start_secondary_thread.start()

    # Verify that the replica has otherwise started up, but cannot start
    # accepting queries yet.
    log.info("Waiting 5 s to verify that the secondary does not start")
    start_secondary_thread.join(5)
    assert secondary.log_contains("consistent recovery state reached")
    assert secondary.log_contains("started streaming WAL from primary")
    # The "redo starts" message is printed when the first WAL record is
    # received. It might or might not be present in the log depending on how
    # far exactly the WAL was flushed when the replica was started, and whether
    # background activity caused any more WAL records to be flushed on the
    # primary afterwards.
    #
    # assert secondary.log_contains("redo # starts")

    # should not be open for connections yet
    assert start_secondary_thread.is_alive()
    assert not secondary.is_running()
    assert not secondary.log_contains("database system is ready to accept read-only connections")

    # Commit the large transaction in the primary.
    #
    # Within the next 15 s, the primary should write a new running-xacts record
    # to the WAL which shows the transaction as completed. Once the replica
    # replays that record, it will start accepting queries.
    primary_cur.execute("commit")
    start_secondary_thread.join()

    # Verify that the large transaction is correctly visible in the secondary
    # (but not the second, small transaction, which is still in-progress!)
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (100000,)

    # Perform some more MVCC testing using the second transaction that was
    # started in the primary before the replica was created
    primary_cur2.execute("select create_subxacts(10000)")

    # The second transaction still hasn't committed
    wait_replica_caughtup(primary, secondary)
    secondary_cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (100000,)

    # Commit the second transaction in the primary
    primary_cur2.execute("commit")

    # Should still be invisible to the old snapshot
    wait_replica_caughtup(primary, secondary)
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (100000,)

    # Commit the REPEATABLE READ transaction in the replica. Both
    # primary transactions should now be visible to a new snapshot.
    secondary_cur.execute("commit")
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (110001,)


def test_replica_too_many_known_assigned_xids(neon_simple_env: NeonEnv):
    """
    The CLOG-scanning mechanism fills the known-assigned XIDs array
    optimistically at standby startup, betting that it can still fit
    upcoming transactions replayed later from the WAL in the
    array. This test tests what happens when that bet fails and the
    known-assigned XID array fills up after the standby has already
    been started. The WAL redo will fail with an error:

    FATAL:  too many KnownAssignedXids
    CONTEXT:  WAL redo at 0/1895CB0 for neon/INSERT: off: 25, flags: 0x08; blkref #0: rel 1663/5/16385, blk 64

    which causes the standby to shut down.

    See the module docstring for background.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("CREATE EXTENSION neon_test_utils")
    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)

    # Determine how many connections we can use
    primary_cur.execute("show max_connections")
    max_connections = int(primary_cur.fetchall()[0][0])
    primary_cur.execute("show superuser_reserved_connections")
    superuser_reserved_connections = int(primary_cur.fetchall()[0][0])
    n_connections = max_connections - superuser_reserved_connections
    n_subxids = 200

    # Start one top transaction in primary, with lots of subtransactions. This
    # uses up much of the known-assigned XIDs space in the standby, but doesn't
    # cause it to overflow.
    large_p_conn = primary.connect()
    large_p_cur = large_p_conn.cursor()
    large_p_cur.execute("begin")
    large_p_cur.execute(f"select create_subxacts({max_connections} * 30)")

    with closing(primary.connect()) as small_p_conn:
        with small_p_conn.cursor() as small_p_cur:
            small_p_cur.execute("select create_subxacts(1)")

    # Create a replica at this LSN
    primary_cur.execute("select neon_xlogflush()")
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    secondary = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()

    # The transaction in primary has not committed yet.
    wait_replica_caughtup(primary, secondary)
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (1,)

    # Start max number of top transactions in primary, with a lot of
    # subtransactions each. We add the subtransactions to each top transaction
    # in a round-robin fashion, instead of adding a lot of subtransactions to
    # one top transaction at a time. This way, we will have the max number of
    # subtransactions in the in-memory subxid cache of each top transaction,
    # until they all overflow.
    #
    # Currently, PGPROC_MAX_CACHED_SUBXIDS == 64, so this will overflow the all
    # the subxid caches after creating 64 subxids in each top transaction. The
    # point just before the caches have overflowed is the most interesting point
    # in time, but we'll keep going beyond that, to ensure that this test is
    # robust even if PGPROC_MAX_CACHED_SUBXIDS changes.
    p_curs = []
    for _ in range(0, n_connections):
        p_cur = primary.connect().cursor()
        p_cur.execute("begin")
        p_curs.append(p_cur)

    for _subxid in range(0, n_subxids):
        for i in range(0, n_connections):
            p_curs[i].execute("select create_subxacts(1)")

    # Commit all the transactions in the primary
    for i in range(0, n_connections):
        p_curs[i].execute("commit")
    large_p_cur.execute("commit")

    # Wait until the replica crashes with "too many KnownAssignedXids" error.
    def check_replica_crashed():
        try:
            secondary.connect()
        except psycopg2.Error:
            # Once the connection fails, return success
            return None
        raise RuntimeError("connection succeeded")

    wait_until(check_replica_crashed)
    assert secondary.log_contains("too many KnownAssignedXids")

    # Replica is crashed, so ignore stop result
    secondary.check_stop_result = False


def test_replica_start_repro_visibility_bug(neon_simple_env: NeonEnv):
    """
    Before PR #7288, a hot standby in neon incorrectly started up
    immediately, before it had received a running-xacts record. That
    led to visibility bugs if there were active transactions in the
    primary. This test reproduces the incorrect query results and
    incorrectly set hint bits, before that was fixed.
    """
    env = neon_simple_env

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    p_cur = primary.connect().cursor()

    p_cur.execute("begin")
    p_cur.execute("create table t(pk integer primary key, payload integer)")
    p_cur.execute("insert into t values (generate_series(1,100000), 0)")

    secondary = env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary")
    wait_replica_caughtup(primary, secondary)
    s_cur = secondary.connect().cursor()

    # Set hint bits for pg_class tuples. If primary's transaction is
    # not marked as in-progress in MVCC snapshot, then XMIN_INVALID
    # hint bit will be set for table's 't' tuple, making it invisible
    # even after the commit record is replayed later.
    s_cur.execute("select * from pg_class")

    p_cur.execute("commit")
    wait_replica_caughtup(primary, secondary)
    s_cur.execute("select * from t where pk = 1")
    assert s_cur.fetchone() == (1, 0)


@pytest.mark.parametrize("shutdown", [True, False])
def test_replica_start_with_prepared_xacts(neon_simple_env: NeonEnv, shutdown: bool):
    """
    Test the CLOG-scanning mechanism at hot standby startup in the presence of
    prepared transactions.

    This test is run in two variants: one where the primary server is shut down
    before starting the secondary, or not.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", config_lines=["max_prepared_transactions=5"]
    )
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("CREATE EXTENSION neon_test_utils")
    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute("create table t1(pk integer primary key)")
    primary_cur.execute("create table t2(pk integer primary key)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)

    # Prepare a transaction for two-phase commit
    primary_cur.execute("begin")
    primary_cur.execute("insert into t1 values (1)")
    primary_cur.execute("prepare transaction 't1'")

    # Prepare another transaction for two-phase commit, with a subtransaction
    primary_cur.execute("begin")
    primary_cur.execute("insert into t2 values (2)")
    primary_cur.execute("savepoint sp")
    primary_cur.execute("insert into t2 values (3)")
    primary_cur.execute("prepare transaction 't2'")

    # Start a transaction in the primary. Leave the transaction open.
    #
    # The transaction has some subtransactions, but not too many to cause the
    # CLOG-scanning mechanism to give up.
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(50)")

    # Wait for the WAL to be flushed
    primary_cur.execute("select neon_xlogflush()")
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)

    if shutdown:
        primary.stop(mode="fast")

    # Create a replica. It should start up normally, thanks to the CLOG-scanning
    # mechanism.
    secondary = env.endpoints.new_replica_start(
        origin=primary, endpoint_id="secondary", config_lines=["max_prepared_transactions=5"]
    )

    # The transaction did not commit, so it should not be visible in the secondary
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (0,)
    secondary_cur.execute("select count(*) from t1")
    assert secondary_cur.fetchone() == (0,)
    secondary_cur.execute("select count(*) from t2")
    assert secondary_cur.fetchone() == (0,)

    if shutdown:
        primary.start()
        primary_conn = primary.connect()
        primary_cur = primary_conn.cursor()
    else:
        primary_cur.execute("commit")
    primary_cur.execute("commit prepared 't1'")
    primary_cur.execute("commit prepared 't2'")

    wait_replica_caughtup(primary, secondary)

    secondary_cur.execute("select count(*) from t")
    if shutdown:
        assert secondary_cur.fetchone() == (0,)
    else:
        assert secondary_cur.fetchone() == (50,)
    secondary_cur.execute("select * from t1")
    assert secondary_cur.fetchall() == [(1,)]
    secondary_cur.execute("select * from t2")
    assert secondary_cur.fetchall() == [(2,), (3,)]


def test_replica_start_with_prepared_xacts_with_subxacts(neon_simple_env: NeonEnv):
    """
    Test the CLOG-scanning mechanism at hot standby startup in the presence of
    prepared transactions, with subtransactions.
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", config_lines=["max_prepared_transactions=5"]
    )
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()

    # Install extension containing function needed for test
    primary_cur.execute("CREATE EXTENSION neon_test_utils")

    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)

    # Advance nextXid close to the beginning of the next pg_subtrans segment (2^16 XIDs)
    #
    # This is interesting, because it tests that pg_subtrans is initialized correctly
    # at standby startup. (We had a bug where it didn't at one point during development.)
    while True:
        xid = int(query_scalar(primary_cur, "SELECT txid_current()"))
        log.info(f"xid now {xid}")
        # Consume 500 transactions at a time until we get close
        if xid < 65535 - 600:
            primary_cur.execute("select test_consume_xids(500);")
        else:
            break
    primary_cur.execute("checkpoint")

    # Prepare a transaction for two-phase commit
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(1000)")
    primary_cur.execute("prepare transaction 't1'")

    # Wait for the WAL to be flushed, and stop the primary
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    primary.stop(mode="fast")

    # Create a replica. It should start up normally, thanks to the CLOG-scanning
    # mechanism.
    secondary = env.endpoints.new_replica_start(
        origin=primary, endpoint_id="secondary", config_lines=["max_prepared_transactions=5"]
    )

    # The transaction did not commit, so it should not be visible in the secondary
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (0,)

    primary.start()

    # Open a lot of subtransactions in the primary, causing the subxids cache to overflow
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("select create_subxacts(100000)")

    wait_replica_caughtup(primary, secondary)

    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (100000,)

    primary_cur.execute("commit prepared 't1'")

    wait_replica_caughtup(primary, secondary)
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (101000,)


def test_replica_start_with_prepared_xacts_with_many_subxacts(neon_simple_env: NeonEnv):
    """
    Test the CLOG-scanning mechanism at hot standby startup in the presence of
    prepared transactions, with lots of subtransactions.

    Like test_replica_start_with_prepared_xacts_with_subxacts, but with more
    subxacts, to test that the prepared transaction's subxids don't consume
    space in the known-assigned XIDs array. (They are set in pg_subtrans
    instead)
    """

    # Initialize the primary, a test table, and a helper function to create lots
    # of subtransactions.
    env = neon_simple_env
    primary = env.endpoints.create_start(
        branch_name="main", endpoint_id="primary", config_lines=["max_prepared_transactions=5"]
    )
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()

    # Install extension containing function needed for test
    primary_cur.execute("CREATE EXTENSION neon_test_utils")

    primary_cur.execute("create table t(pk serial primary key, payload integer)")
    primary_cur.execute(CREATE_SUBXACTS_FUNC)

    # Prepare a transaction for two-phase commit, with lots of subxids
    primary_cur.execute("begin")
    primary_cur.execute("select create_subxacts(50000)")

    # to make things a bit more varied, intersperse a few other XIDs in between
    # the prepared transaction's sub-XIDs
    with primary.connect().cursor() as primary_cur2:
        primary_cur2.execute("insert into t (payload) values (123)")
        primary_cur2.execute("begin; insert into t (payload) values (-1); rollback")

    primary_cur.execute("select create_subxacts(50000)")
    primary_cur.execute("prepare transaction 't1'")

    # Wait for the WAL to be flushed
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)

    primary.stop(mode="fast")

    # Create a replica. It should start up normally, thanks to the CLOG-scanning
    # mechanism.
    secondary = env.endpoints.new_replica_start(
        origin=primary, endpoint_id="secondary", config_lines=["max_prepared_transactions=5"]
    )

    # The transaction did not commit, so it should not be visible in the secondary
    secondary_conn = secondary.connect()
    secondary_cur = secondary_conn.cursor()
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (1,)

    primary.start()

    # Open a lot of subtransactions in the primary, causing the subxids cache to overflow
    primary_conn = primary.connect()
    primary_cur = primary_conn.cursor()
    primary_cur.execute("select create_subxacts(100000)")

    wait_replica_caughtup(primary, secondary)

    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (100001,)

    primary_cur.execute("commit prepared 't1'")

    wait_replica_caughtup(primary, secondary)
    secondary_cur.execute("select count(*) from t")
    assert secondary_cur.fetchone() == (200001,)


def test_replica_start_with_too_many_unused_xids(neon_simple_env: NeonEnv):
    """
    Test the CLOG-scanning mechanism at hot standby startup in the presence of
    large number of unsued XIDs, caused by  XID alignment and frequent primary restarts
    """
    n_restarts = 50

    # Initialize the primary and a test table
    env = neon_simple_env
    timeline_id = env.initial_timeline
    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")
    with primary.cursor() as primary_cur:
        primary_cur.execute("create table t(pk serial primary key, payload integer)")

    for _ in range(n_restarts):
        with primary.cursor() as primary_cur:
            primary_cur.execute("insert into t (payload) values (0)")
        # restart primary
        primary.stop("immediate", sks_wait_walreceiver_gone=(env.safekeepers, timeline_id))
        primary.start()

    # Wait for the WAL to be flushed
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)

    # stop primary to check that we can start replica without it
    primary.stop(mode="immediate")

    # Create a replica. It should start up normally, because of ignore policy
    # mechanism.
    secondary = env.endpoints.new_replica_start(
        origin=primary,
        endpoint_id="secondary",
        config_lines=["neon.running_xacts_overflow_policy='ignore'"],
    )

    # Check that replica see all changes
    with secondary.cursor() as secondary_cur:
        secondary_cur.execute("select count(*) from t")
        assert secondary_cur.fetchone() == (n_restarts,)


def test_ephemeral_endpoints_vacuum(neon_simple_env: NeonEnv, pg_bin: PgBin):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    sql = """
CREATE TABLE CHAR_TBL(f1 char(4));
CREATE TABLE FLOAT8_TBL(f1 float8);
CREATE TABLE INT2_TBL(f1 int2);
CREATE TABLE INT4_TBL(f1 int4);
CREATE TABLE INT8_TBL(q1 int8, q2 int8);
CREATE TABLE POINT_TBL(f1 point);
CREATE TABLE TEXT_TBL (f1 text);
CREATE TABLE VARCHAR_TBL(f1 varchar(4));
CREATE TABLE onek (unique1		int4);
CREATE TABLE onek2 AS SELECT * FROM onek;
CREATE TABLE tenk1 (unique1		int4);
CREATE TABLE tenk2 AS SELECT * FROM tenk1;
CREATE TABLE person (name text, age int4,location point);
CREATE TABLE emp (salary int4, manager name) INHERITS (person);
CREATE TABLE student (gpa float8) INHERITS (person);
CREATE TABLE stud_emp (	percent 	int4) INHERITS (emp, student);
CREATE TABLE road (name		text,thepath 	path);
CREATE TABLE ihighway () INHERITS (road);
CREATE TABLE shighway(surface		text) INHERITS (road);
CREATE TABLE BOOLTBL3 (d text, b bool, o int);
CREATE TABLE booltbl4(isfalse bool, istrue bool, isnul bool);
DROP TABLE BOOLTBL3;
DROP TABLE BOOLTBL4;
CREATE TABLE ceil_floor_round (a numeric);
DROP TABLE ceil_floor_round;
CREATE TABLE width_bucket_test (operand_num numeric, operand_f8 float8);
DROP TABLE width_bucket_test;
CREATE TABLE num_input_test (n1 numeric);
CREATE TABLE num_variance (a numeric);
INSERT INTO num_variance VALUES (0);
CREATE TABLE snapshot_test (nr	integer, snap	txid_snapshot);
CREATE TABLE guid1(guid_field UUID, text_field TEXT DEFAULT(now()));
CREATE TABLE guid2(guid_field UUID, text_field TEXT DEFAULT(now()));
CREATE INDEX guid1_btree ON guid1 USING BTREE (guid_field);
CREATE INDEX guid1_hash  ON guid1 USING HASH  (guid_field);
TRUNCATE guid1;
DROP TABLE guid1;
DROP TABLE guid2 CASCADE;
CREATE TABLE numrange_test (nr NUMRANGE);
CREATE INDEX numrange_test_btree on numrange_test(nr);
CREATE TABLE numrange_test2(nr numrange);
CREATE INDEX numrange_test2_hash_idx on numrange_test2 using hash (nr);
INSERT INTO numrange_test2 VALUES('[, 5)');
CREATE TABLE textrange_test (tr text);
CREATE INDEX textrange_test_btree on textrange_test(tr);
CREATE TABLE test_range_gist(ir int4range);
CREATE INDEX test_range_gist_idx on test_range_gist using gist (ir);
DROP INDEX test_range_gist_idx;
CREATE INDEX test_range_gist_idx on test_range_gist using gist (ir);
CREATE TABLE test_range_spgist(ir int4range);
CREATE INDEX test_range_spgist_idx on test_range_spgist using spgist (ir);
DROP INDEX test_range_spgist_idx;
CREATE INDEX test_range_spgist_idx on test_range_spgist using spgist (ir);
CREATE TABLE test_range_elem(i int4);
CREATE INDEX test_range_elem_idx on test_range_elem (i);
CREATE INDEX ON test_range_elem using spgist(int4range(i,i+10));
DROP TABLE test_range_elem;
CREATE TABLE test_range_excl(room int4range, speaker int4range, during tsrange, exclude using gist (room with =, during with &&), exclude using gist (speaker with =, during with &&));
CREATE TABLE f_test(f text, i int);
CREATE TABLE i8r_array (f1 int, f2 text);
CREATE TYPE arrayrange as range (subtype=int4[]);
CREATE TYPE two_ints as (a int, b int);
DROP TYPE two_ints cascade;
CREATE TABLE text_support_test (t text);
CREATE TABLE TEMP_FLOAT (f1 FLOAT8);
CREATE TABLE TEMP_INT4 (f1 INT4);
CREATE TABLE TEMP_INT2 (f1 INT2);
CREATE TABLE TEMP_GROUP (f1 INT4, f2 INT4, f3 FLOAT8);
CREATE TABLE POLYGON_TBL(f1 polygon);
CREATE TABLE quad_poly_tbl (id int, p polygon);
INSERT INTO quad_poly_tbl SELECT (x - 1) * 100 + y, polygon(circle(point(x * 10, y * 10), 1 + (x + y) % 10)) FROM generate_series(1, 200) x, generate_series(1, 100) y;
CREATE TABLE quad_poly_tbl_ord_seq2 AS SELECT 1 FROM quad_poly_tbl;
CREATE TABLE quad_poly_tbl_ord_idx2 AS SELECT 1 FROM quad_poly_tbl;
"""

    with endpoint.cursor() as cur:
        lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        env.endpoints.create_start(branch_name="main", lsn=lsn)
        log.info(f"lsn: {lsn}")

        for line in sql.split("\n"):
            if len(line.strip()) == 0 or line.startswith("--"):
                continue
            cur.execute(line)

        lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        env.endpoints.create_start(branch_name="main", lsn=lsn)
        log.info(f"lsn: {lsn}")

        cur.execute("VACUUM FULL pg_class;")

    for ep in env.endpoints.endpoints:
        log.info(f"{ep.endpoint_id} / {ep.pg_port}")
        pg_dump_command = ["pg_dumpall", "-f", f"/tmp/dump-{ep.endpoint_id}.sql"]
        env_vars = {
            "PGPORT": str(ep.pg_port),
            "PGUSER": endpoint.default_options["user"],
            "PGHOST": endpoint.default_options["host"],
        }
        pg_bin.run_capture(pg_dump_command, env=env_vars)
