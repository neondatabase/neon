from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, wait_for_last_flush_lsn, wait_replica_caughtup


def test_replication_start_subxid_overflow(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.endpoints.create_start(branch_name="main", endpoint_id="primary") as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute("begin")
                p_cur.execute("create table t(pk serial primary key, payload integer)")
                p_cur.execute(
                    """create or replace function create_subxacts(n integer) returns void as $$
                    declare
                        i integer;
                    begin
                       for i in 1..n loop
                          begin
                              insert into t (payload) values (0);
                          exception
                              when others then
                                  raise exception 'caught something';
                          end;
                       end loop;
                   end; $$ language plpgsql"""
                )
                p_cur.execute("select create_subxacts(100000)")
                xid = p_cur.fetchall()[0][0]
                log.info(f"Master transaction {xid}")
                with env.endpoints.new_replica_start(
                    origin=primary, endpoint_id="secondary"
                ) as secondary:
                    wait_replica_caughtup(primary, secondary)
                    with secondary.connect() as s_con:
                        with s_con.cursor() as s_cur:
                            # Enforce setting hint bits for pg_class tuples.
                            # If master's transaction is not marked as in-progress in MVCC snapshot,
                            # then XMIN_INVALID hint bit will be set for table's 't' tuple makeing it invisible.
                            s_cur.execute("select * from pg_class")
                            p_cur.execute("commit")
                            wait_replica_caughtup(primary, secondary)
                            s_cur.execute("select * from t where pk = 1")
                            assert s_cur.fetchone() == (1, 0)


# Test starting a replica at a point in time where there is a transaction
# running in primary with lots of subtransactions. Test MVCC in standby before
# and after the commit of the transaction.
#
# XXX: This currently fails, the query in standby incorrectly sees some sub-xids
# as committed:
#
# test_runner/regress/test_replication_start_subxid_overflow.py:105: AssertionError
#
# FAILED test_runner/regress/test_replication_start_subxid_overflow.py::test_replication_start_subxid_overflow2[debug-pg16] - assert (93229,) == (0,)

def test_replication_start_subxid_overflow2(neon_simple_env: NeonEnv):
    env = neon_simple_env

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")

    p_con = primary.connect()
    p_cur = p_con.cursor()

    p_cur.execute("create table t(pk serial primary key, payload integer)")
    p_cur.execute(
        """create or replace function create_subxacts(n integer) returns void as $$
        declare
            i integer;
        begin
           for i in 1..n loop
              begin
                  insert into t (payload) values (0);
              exception
                  when others then
                      raise exception 'caught something';
              end;
           end loop;
       end; $$ language plpgsql"""
    )

    # Start a new transaction in primary, with a lot of subtransactions
    p_cur.execute("begin")
    p_cur.execute("select create_subxacts(100000)")

    # Create a replica at this LSN
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
        s_con = secondary.connect()
        s_cur = s_con.cursor()

        # The transaction in primary has not committed yet.
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Add more sub-xids to the same transaction in primary
        p_cur.execute("select create_subxacts(10000)")

        # The transaction still hasn't committed
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Commit the transaction in primary
        p_cur.execute("commit")

        # Should still be invisible to the old snapshot
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Commit the REPEATABLE READ transaction in standby
        s_cur.execute("commit")

        # The transaction should now visible to new snapshot in standby
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (200000,)

# Test starting a standby from situation that there is one transaction running
# in primary, with lots of subtransactions. Then, after the standby is running,
# start a large number of new transcations, with lots of subtransactions each, in
# the primary.
#
# XXX: This currently crashes the startup process in secondary with:
#
# FATAL:  too many KnownAssignedXids
# CONTEXT:  WAL redo at 0/1895CB0 for neon/INSERT: off: 25, flags: 0x08; blkref #0: rel 1663/5/16385, blk 64
def test_replication_start_subxid_overflow3(neon_simple_env: NeonEnv):
    env = neon_simple_env

    primary = env.endpoints.create_start(branch_name="main", endpoint_id="primary")

    with primary.connect() as p_con:
        p_cur = p_con.cursor()

        p_cur.execute("create table t(pk serial primary key, payload integer)")
        p_cur.execute(
            """create or replace function create_subxacts(n integer) returns void as $$
            declare
                i integer;
            begin
                for i in 1..n loop
                    begin
                        insert into t (payload) values (0);
                    exception
                        when others then
                            raise exception 'caught something';
                    end;
                end loop;
           end; $$ language plpgsql"""
        )

        p_cur.execute(f"show max_connections")
        max_connections = int(p_cur.fetchone()[0])

    n_connections = max_connections - 2
    n_subxids = 100

    # Start one top tranaction in primary, with lots of subtransactions. This fills up the
    # known-assigned XIDs space in the standby.
    large_p_conn = primary.connect()
    large_p_cur = large_p_conn.cursor()
    large_p_cur.execute("begin")
    large_p_cur.execute("select create_subxacts(20000)")

    # Create a replica at this LSN
    wait_for_last_flush_lsn(env, primary, env.initial_tenant, env.initial_timeline)
    with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
        s_con = secondary.connect()
        s_cur = s_con.cursor()

        # The transaction in primary has not committed yet.
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Start max number of top transactions in primary, with a lot of
        # subtransactions each We add the subtransactions to each top
        # transaction in a round-robin fashion, instead of adding a lot of
        # subtransactions ot one top transaction at a time. This way, we will
        # have the max number of subtransactions in the in-memory subxid cache
        # of each top transaction, until they all overflow.
        #
        # Currently, PGPROC_MAX_CACHED_SUBXIDS == 64, so this will overflow the
        # all the subxid caches after creating 64 subxids in each top
        # transaction. The point just before the caches have overflowed is the
        # most interesting point in time, but we'll keep going beyond that, to
        # ensure that this test is robust even if PGPROC_MAX_CACHED_SUBXIDS
        # changes.
        p_cons = []
        p_curs = []
        for i in range(0, n_connections):
            p_con = primary.connect()
            p_cur = p_con.cursor()

            p_cur.execute("begin")
            p_cons.append(p_con)
            p_curs.append(p_cur)

        for subxid in range(0, n_subxids):
            for i in range(0, n_connections):
                p_curs[i].execute("select create_subxacts(1)")

            # None of the transactions have committed yet, so they should be
            # invisible in standby.
            s_cur.execute("select count(*) from t")
            assert s_cur.fetchone() == (0,)

        # The transactions still haven't committed. Start a new snapshot, we
        # will use it later, after committing in the primary.
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")

        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Commit all the transactions in the primary
        large_p_cur.execute("commit")
        for i in range(0, n_connections):
            p_curs[i].execute("commit")

        # All the XIDs still be invisible to the old snapshot
        wait_replica_caughtup(primary, secondary)
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (0,)

        # Commit the REPEATABLE READ transaction in standby
        s_cur.execute("commit")

        # The transaction should now visible to new snapshot in standby
        s_cur.execute("select count(*) from t")
        assert s_cur.fetchone() == (n_connections * n_subxids,)
