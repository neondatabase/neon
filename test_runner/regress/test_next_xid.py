from __future__ import annotations

import os
import time
from pathlib import Path

from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
    import_timeline_from_vanilla_postgres,
    wait_for_wal_insert_lsn,
)
from fixtures.remote_storage import RemoteStorageKind
from fixtures.utils import query_scalar


def test_next_xid(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("CREATE TABLE t(x integer)")

    iterations = 32
    for i in range(1, iterations + 1):
        print(f"iteration {i} / {iterations}")

        # Kill and restart the pageserver.
        endpoint.stop()
        env.pageserver.stop(immediate=True)
        env.pageserver.start()
        endpoint.start()

        retry_sleep = 0.5
        max_retries = 200
        retries = 0
        while True:
            try:
                conn = endpoint.connect()
                cur = conn.cursor()
                cur.execute(f"INSERT INTO t values({i})")
                conn.close()

            except Exception as error:
                # It's normal that it takes some time for the pageserver to
                # restart, and for the connection to fail until it does. It
                # should eventually recover, so retry until it succeeds.
                print(f"failed: {error}")
                if retries < max_retries:
                    retries += 1
                    print(f"retry {retries} / {max_retries}")
                    time.sleep(retry_sleep)
                    continue
                else:
                    raise
            break

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM t")
    assert cur.fetchone() == (iterations,)


# Test for a bug we had, where nextXid was incorrectly updated when the
# XID counter reached 2 billion. The nextXid tracking logic incorrectly
# treated 0 (InvalidTransactionId) as a regular XID, and after reaching
# 2 billion, it started to look like a very new XID, which caused nextXid
# to be immediately advanced to the next epoch.
#
def test_import_at_2bil(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin: PgBin,
    vanilla_pg,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    # Reset the vanilla Postgres instance to somewhat before 2 billion transactions.
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, "pg_resetwal")
    cmd = [pg_resetwal_path, "--next-transaction-id=2129920000", "-D", str(vanilla_pg.pgdatadir)]
    pg_bin.run_capture(cmd)

    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql(
        """create table tt as select 'long string to consume some space' || g
     from generate_series(1,300000) g"""
    )
    assert vanilla_pg.safe_psql("select count(*) from tt") == [(300000,)]
    vanilla_pg.safe_psql("CREATE TABLE t (t text);")
    vanilla_pg.safe_psql("INSERT INTO t VALUES ('inserted in vanilla')")
    vanilla_pg.safe_psql("CHECKPOINT")

    tenant_id = TenantId.generate()
    env.pageserver.tenant_create(tenant_id)
    timeline_id = TimelineId.generate()

    # Import the cluster to Neon
    import_timeline_from_vanilla_postgres(
        test_output_dir,
        env,
        pg_bin,
        tenant_id,
        timeline_id,
        "imported_2bil_xids",
        vanilla_pg.connstr(),
    )
    vanilla_pg.stop()  # don't need the original server anymore

    # Check that it works
    endpoint = env.endpoints.create_start(
        "imported_2bil_xids",
        tenant_id=tenant_id,
        config_lines=[
            "log_autovacuum_min_duration = 0",
            "autovacuum_naptime='5 s'",
        ],
    )
    assert endpoint.safe_psql("select count(*) from t") == [(1,)]

    conn = endpoint.connect()
    cur = conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Advance nextXid close to 2 billion XIDs
    while True:
        xid = int(query_scalar(cur, "SELECT txid_current()"))
        log.info(f"xid now {xid}")
        # Consume 10k transactons at a time until we get to 2^31 - 200k
        if xid < 2 * 1024 * 1024 * 1024 - 100000:
            cur.execute("select test_consume_xids(50000);")
        elif xid < 2 * 1024 * 1024 * 1024 - 10000:
            cur.execute("select test_consume_xids(5000);")
        else:
            break

    # Run a bunch of real INSERTs to cross over the 2 billion mark
    # Use a begin-exception block to have a separate sub-XID for each insert.
    cur.execute(
        """
        do $$
        begin
          for i in 1..10000 loop
            -- Use a begin-exception block to generate a new subtransaction on each iteration
            begin
              insert into t values (i);
            exception when others then
              raise 'not expected %', sqlerrm;
            end;
          end loop;
        end;
        $$;
        """
    )

    # Also create a multi-XID with members past the 2 billion mark
    conn2 = endpoint.connect()
    cur2 = conn2.cursor()
    cur.execute("INSERT INTO t VALUES ('x')")
    cur.execute("BEGIN; select * from t WHERE t = 'x' FOR SHARE;")
    cur2.execute("BEGIN; select * from t WHERE t = 'x' FOR SHARE;")
    cur.execute("COMMIT")
    cur2.execute("COMMIT")

    # A checkpoint writes a WAL record with xl_xid=0. Many other WAL
    # records would have the same effect.
    cur.execute("checkpoint")

    # wait until pageserver receives that data
    wait_for_wal_insert_lsn(env, endpoint, tenant_id, timeline_id)

    # Restart endpoint
    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) from t")
    assert cur.fetchone() == (10000 + 1 + 1,)


# Constants and macros copied from PostgreSQL multixact.c and headers. These are needed to
# calculate the SLRU segments that a particular multixid or multixid-offsets falls into.
BLCKSZ = 8192
MULTIXACT_OFFSETS_PER_PAGE = int(BLCKSZ / 4)
SLRU_PAGES_PER_SEGMENT = 32
MXACT_MEMBER_BITS_PER_XACT = 8
MXACT_MEMBER_FLAGS_PER_BYTE = 1
MULTIXACT_FLAGBYTES_PER_GROUP = 4
MULTIXACT_MEMBERS_PER_MEMBERGROUP = MULTIXACT_FLAGBYTES_PER_GROUP * MXACT_MEMBER_FLAGS_PER_BYTE
MULTIXACT_MEMBERGROUP_SIZE = 4 * MULTIXACT_MEMBERS_PER_MEMBERGROUP + MULTIXACT_FLAGBYTES_PER_GROUP
MULTIXACT_MEMBERGROUPS_PER_PAGE = int(BLCKSZ / MULTIXACT_MEMBERGROUP_SIZE)
MULTIXACT_MEMBERS_PER_PAGE = MULTIXACT_MEMBERGROUPS_PER_PAGE * MULTIXACT_MEMBERS_PER_MEMBERGROUP


def MultiXactIdToOffsetSegment(xid: int):
    return int(xid / (SLRU_PAGES_PER_SEGMENT * MULTIXACT_OFFSETS_PER_PAGE))


def MXOffsetToMemberSegment(off: int):
    return int(off / (SLRU_PAGES_PER_SEGMENT * MULTIXACT_MEMBERS_PER_PAGE))


def advance_multixid_to(
    pg_bin: PgBin, vanilla_pg: VanillaPostgres, next_multi_xid: int, next_multi_offset: int
):
    """
    Use pg_resetwal to advance the nextMulti and nextMultiOffset values in a stand-alone
    Postgres cluster. This is useful to get close to wraparound or some other interesting
    value, without having to burn a lot of time consuming the (multi-)XIDs one by one.

    The new values should be higher than the old ones, in a wraparound-aware sense.

    On entry, the server should be running. It will be shut down and restarted.
    """

    # Read old values from the last checkpoint. We will pass the old oldestMultiXid value
    # back to pg_resetwal, there's no option to leave it alone.
    with vanilla_pg.connect() as conn:
        with conn.cursor() as cur:
            # Make sure the oldest-multi-xid value in the control file is up-to-date
            cur.execute("checkpoint")
            cur.execute("select oldest_multi_xid, next_multixact_id from pg_control_checkpoint()")
            rec = cur.fetchone()
            assert rec is not None
            (ckpt_oldest_multi_xid, ckpt_next_multi_xid) = rec
    log.info(f"oldestMultiXid was {ckpt_oldest_multi_xid}, nextMultiXid was {ckpt_next_multi_xid}")
    log.info(f"Resetting to {next_multi_xid}")

    # Use pg_resetwal to reset the next multiXid and multiOffset to given values.
    vanilla_pg.stop()
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, "pg_resetwal")
    cmd = [
        pg_resetwal_path,
        f"--multixact-ids={next_multi_xid},{ckpt_oldest_multi_xid}",
        f"--multixact-offset={next_multi_offset}",
        "-D",
        str(vanilla_pg.pgdatadir),
    ]
    pg_bin.run_capture(cmd)

    # Because we skip over a lot of values, Postgres hasn't created the SLRU segments for
    # the new values yet. Create them manually, to allow Postgres to start up.
    #
    # This leaves "gaps" in the SLRU where segments between old value and new value are
    # missing. That's OK for our purposes. Autovacuum will print some warnings about the
    # missing segments, but will clean it up by truncating the SLRUs up to the new value,
    # closing the gap.
    segname = f"{MultiXactIdToOffsetSegment(next_multi_xid):04X}"
    log.info(f"Creating dummy segment pg_multixact/offsets/{segname}")
    with open(vanilla_pg.pgdatadir / "pg_multixact" / "offsets" / segname, "w") as of:
        of.write("\0" * SLRU_PAGES_PER_SEGMENT * BLCKSZ)
        of.flush()

    segname = f"{MXOffsetToMemberSegment(next_multi_offset):04X}"
    log.info(f"Creating dummy segment pg_multixact/members/{segname}")
    with open(vanilla_pg.pgdatadir / "pg_multixact" / "members" / segname, "w") as of:
        of.write("\0" * SLRU_PAGES_PER_SEGMENT * BLCKSZ)
        of.flush()

    # Start Postgres again and wait until autovacuum has processed all the databases
    #
    # This allows truncating the SLRUs, fixing the gaps with missing segments.
    vanilla_pg.start()
    with vanilla_pg.connect().cursor() as cur:
        for _ in range(1000):
            datminmxid = int(
                query_scalar(cur, "select min(datminmxid::text::int8) from pg_database")
            )
            log.info(f"datminmxid {datminmxid}")
            if next_multi_xid - datminmxid < 1_000_000:  # not wraparound-aware!
                break
            time.sleep(0.5)


def test_multixid_wraparound_import(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_bin: PgBin,
    vanilla_pg,
):
    """
    Test that the wraparound of the "next-multi-xid" counter is handled correctly in
    pageserver, And multi-offsets as well
    """
    env = neon_env_builder.init_start()

    # In order to to test multixid wraparound, we need to first advance the counter to
    # within spitting distance of the wraparound, that is 2^32 multi-XIDs. We could simply
    # run a workload that consumes a lot of multi-XIDs until we approach that, but that
    # takes a very long time. So we cheat.
    #
    # Our strategy is to create a vanilla Postgres cluster, and use pg_resetwal to
    # directly set the multi-xid counter a higher value. However, we cannot directly set
    # it to just before 2^32 (~ 4 billion), because that would make the exisitng
    # 'relminmxid' values to look like they're in the future. It's not clear how the
    # system would behave in that situation. So instead, we bump it up ~ 1 billion
    # multi-XIDs at a time, and let autovacuum to process all the relations and update
    # 'relminmxid' between each run.
    #
    # XXX: For the multi-offsets, most of the bump is done in the last call.  This is
    # because advancing it ~ 1 billion at a time hit a pathological case in the
    # MultiXactMemberFreezeThreshold() function, causing autovacuum not trigger multixid
    # freezing. See
    # https://www.postgresql.org/message-id/85fb354c-f89f-4d47-b3a2-3cbd461c90a3%40iki.fi
    # Multi-offsets don't have the same wraparound problems at 2 billion mark as
    # multi-xids do, so one big jump is fine.
    vanilla_pg.configure(
        [
            "log_autovacuum_min_duration = 0",
            # Perform anti-wraparound vacuuming aggressively
            "autovacuum_naptime='1 s'",
            "autovacuum_freeze_max_age = 1000000",
            "autovacuum_multixact_freeze_max_age = 1000000",
        ],
    )
    vanilla_pg.start()
    advance_multixid_to(pg_bin, vanilla_pg, 0x40000000, 0x10000000)
    advance_multixid_to(pg_bin, vanilla_pg, 0x80000000, 0x20000000)
    advance_multixid_to(pg_bin, vanilla_pg, 0xC0000000, 0x30000000)
    advance_multixid_to(pg_bin, vanilla_pg, 0xFFFFFF00, 0xFFFFFF00)

    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql("create table tt as select g as id from generate_series(1, 10) g")
    vanilla_pg.safe_psql("CHECKPOINT")

    # Import the cluster to the pageserver
    tenant_id = TenantId.generate()
    env.pageserver.tenant_create(tenant_id)
    timeline_id = TimelineId.generate()
    import_timeline_from_vanilla_postgres(
        test_output_dir,
        env,
        pg_bin,
        tenant_id,
        timeline_id,
        "imported_multixid_wraparound_test",
        vanilla_pg.connstr(),
    )
    vanilla_pg.stop()

    endpoint = env.endpoints.create_start(
        "imported_multixid_wraparound_test",
        tenant_id=tenant_id,
        config_lines=[
            "log_autovacuum_min_duration = 0",
            "autovacuum_naptime='5 s'",
            "autovacuum=off",
        ],
    )
    conn = endpoint.connect()
    cur = conn.cursor()
    assert query_scalar(cur, "select count(*) from tt") == 10  # sanity check

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Consume a lot of XIDs, just to advance the XIDs to different range than the
    # multi-xids. That avoids confusion while debugging
    cur.execute("select test_consume_xids(100000)")
    cur.execute("select pg_switch_wal()")
    cur.execute("checkpoint")

    # Use subtransactions so that each row in 'tt' is stamped with different XID. Leave
    # the transaction open.
    cur.execute("BEGIN")
    cur.execute(
        """
do $$
declare
  idvar int;
begin
  for idvar in select id from tt loop
    begin
      update tt set id = idvar where id = idvar;
    exception when others then
      raise 'didn''t expect an error: %', sqlerrm;
    end;
  end loop;
end;
$$;
"""
    )

    # In a different transaction, acquire a FOR KEY SHARE lock on each row. This generates
    # a new multixid for each row, with the previous xmax and this transaction's XID as the
    # members.
    #
    # Repeat this until the multi-xid counter wraps around.
    conn3 = endpoint.connect()
    cur3 = conn3.cursor()
    next_multixact_id_before_restart = 0
    observed_before_wraparound = False
    while True:
        cur3.execute("BEGIN")
        cur3.execute("SELECT * FROM tt FOR KEY SHARE")

        # Get the xmax of one of the rows we locked. It should be a multi-xid. It might
        # not be the latest one, but close enough.
        row_xmax = int(query_scalar(cur3, "SELECT xmax FROM tt LIMIT 1"))
        cur3.execute("COMMIT")
        log.info(f"observed a row with xmax {row_xmax}")

        # High value means not wrapped around yet
        if row_xmax >= 0xFFFFFF00:
            observed_before_wraparound = True
            continue

        # xmax should not be a regular XID. (We bumped up the regular XID range earlier
        # to around 100000 and above.)
        assert row_xmax < 100

        # xmax values < FirstNormalTransactionId (== 3) could be special XID values, or
        # multixid values after wraparound. We don't know for sure which, so keep going to
        # be sure we see value that's unambiguously a wrapped-around multixid
        if row_xmax < 3:
            continue

        next_multixact_id_before_restart = row_xmax
        log.info(
            f"next_multixact_id is now at {next_multixact_id_before_restart} or a little higher"
        )
        break

    # We should have observed the state before wraparound
    assert observed_before_wraparound

    cur.execute("COMMIT")

    # Wait until pageserver has received all the data, and restart the endpoint
    wait_for_wal_insert_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop(
        mode="immediate", sks_wait_walreceiver_gone=(env.safekeepers, timeline_id)
    )  # 'immediate' to avoid writing shutdown checkpoint
    endpoint.start()

    # Check that the next-multixid value wrapped around correctly
    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("select next_multixact_id from pg_control_checkpoint()")
    next_multixact_id_after_restart = int(
        query_scalar(cur, "select next_multixact_id from pg_control_checkpoint()")
    )
    log.info(f"next_multixact_id after restart: {next_multixact_id_after_restart}")
    assert next_multixact_id_after_restart >= next_multixact_id_before_restart

    # The multi-offset should wrap around as well
    cur.execute("select next_multi_offset from pg_control_checkpoint()")
    next_multi_offset_after_restart = int(
        query_scalar(cur, "select next_multi_offset from pg_control_checkpoint()")
    )
    log.info(f"next_multi_offset after restart: {next_multi_offset_after_restart}")
    assert next_multi_offset_after_restart < 100000
