import json
import os
import time
from pathlib import Path

from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_wal_insert_lsn
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
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
    ps_http = env.pageserver.http_client()

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

    endpoint_id = "ep-import_from_vanilla"
    tenant = TenantId.generate()
    timeline = TimelineId.generate()

    env.pageserver.tenant_create(tenant)

    # Take basebackup
    basebackup_dir = os.path.join(test_output_dir, "basebackup")
    base_tar = os.path.join(basebackup_dir, "base.tar")
    wal_tar = os.path.join(basebackup_dir, "pg_wal.tar")
    os.mkdir(basebackup_dir)
    vanilla_pg.safe_psql("CHECKPOINT")
    pg_bin.run(
        [
            "pg_basebackup",
            "-F",
            "tar",
            "-d",
            vanilla_pg.connstr(),
            "-D",
            basebackup_dir,
        ]
    )

    # Get start_lsn and end_lsn
    with open(os.path.join(basebackup_dir, "backup_manifest")) as f:
        manifest = json.load(f)
        start_lsn = manifest["WAL-Ranges"][0]["Start-LSN"]
        end_lsn = manifest["WAL-Ranges"][0]["End-LSN"]

    def import_tar(base, wal):
        env.neon_cli.raw_cli(
            [
                "timeline",
                "import",
                "--tenant-id",
                str(tenant),
                "--timeline-id",
                str(timeline),
                "--node-name",
                endpoint_id,
                "--base-lsn",
                start_lsn,
                "--base-tarfile",
                base,
                "--end-lsn",
                end_lsn,
                "--wal-tarfile",
                wal,
                "--pg-version",
                env.pg_version,
            ]
        )

    # Importing correct backup works
    import_tar(base_tar, wal_tar)
    wait_for_last_record_lsn(ps_http, tenant, timeline, Lsn(end_lsn))

    endpoint = env.endpoints.create_start(
        endpoint_id,
        tenant_id=tenant,
        config_lines=[
            "log_autovacuum_min_duration = 0",
            "autovacuum_naptime='5 s'",
        ],
    )
    assert endpoint.safe_psql("select count(*) from t") == [(1,)]

    # Ok, consume
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
    wait_for_wal_insert_lsn(env, endpoint, tenant, timeline)

    # Restart endpoint
    endpoint.stop()
    endpoint.start()

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) from t")
    assert cur.fetchone() == (10000 + 1 + 1,)
