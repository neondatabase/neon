from __future__ import annotations

import pytest
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, tenant_get_shards
from fixtures.utils import query_scalar


#
# Test on-demand download of the pg_xact SLRUs
#
@pytest.mark.parametrize("shard_count", [None, 4])
def test_ondemand_download_pg_xact(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count

    tenant_conf = {
        "lazy_slru_download": "true",
        # set PITR interval to be small, so we can do GC
        "pitr_interval": "0 s",
    }
    env = neon_env_builder.init_start(
        initial_tenant_conf=tenant_conf, initial_tenant_shard_count=shard_count
    )

    timeline_id = env.initial_timeline
    tenant_id = env.initial_tenant
    endpoint = env.endpoints.create_start("main")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table
    cur.execute("CREATE TABLE clogtest (id integer)")
    cur.execute("INSERT INTO clogtest VALUES (1)")

    # Consume a lot of XIDs, to create more pg_xact segments
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")
    cur.execute("INSERT INTO clogtest VALUES (2)")
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")
    cur.execute("INSERT INTO clogtest VALUES (2)")
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")
    cur.execute("INSERT INTO clogtest VALUES (3)")

    # Restart postgres. After restart, the new instance will download the
    # pg_xact segments lazily.
    endpoint.stop()
    endpoint.start()
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Consume more WAL, so that the pageserver can compact and GC older data,
    # including the LSN that we started the new endpoint at,
    cur.execute("CREATE TABLE anothertable (i int, t text)")
    cur.execute(
        "INSERT INTO anothertable SELECT g, 'long string to consume some space' || g FROM generate_series(1, 10000) g"
    )

    # Run GC
    shards = tenant_get_shards(env, tenant_id, None)
    for tenant_shard_id, pageserver in shards:
        client = pageserver.http_client()
        client.timeline_checkpoint(tenant_shard_id, timeline_id)
        client.timeline_compact(tenant_shard_id, timeline_id)
        client.timeline_gc(tenant_shard_id, timeline_id, 0)

    # Test that this can still on-demand download the old pg_xact segments
    cur.execute("select xmin, xmax, * from clogtest")
    tup = cur.fetchall()
    log.info(f"tuples = {tup}")


@pytest.mark.parametrize("shard_count", [None, 4])
def test_ondemand_download_replica(neon_env_builder: NeonEnvBuilder, shard_count: int | None):
    if shard_count is not None:
        neon_env_builder.num_pageservers = shard_count

    tenant_conf = {
        "lazy_slru_download": "true",
    }
    env = neon_env_builder.init_start(
        initial_tenant_conf=tenant_conf, initial_tenant_shard_count=shard_count
    )

    endpoint = env.endpoints.create_start("main")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table
    cur.execute("CREATE TABLE clogtest (id integer)")
    cur.execute("INSERT INTO clogtest VALUES (1)")

    # Consume a lot of XIDs, to create more pg_xact segments
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")

    # Open a new connection and insert another row, but leave
    # the transaction open
    pg_conn2 = endpoint.connect()
    cur2 = pg_conn2.cursor()
    cur2.execute("BEGIN")
    cur2.execute("INSERT INTO clogtest VALUES (2)")

    # Another insert on the first connection, which is committed.
    for _ in range(1000):
        cur.execute("select test_consume_xids(10000);")
    cur.execute("INSERT INTO clogtest VALUES (3)")

    # Start standby at this point in time
    lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_insert_lsn()"))
    endpoint_at_lsn = env.endpoints.create_start(
        branch_name="main", endpoint_id="ep-at-lsn", lsn=lsn
    )

    # Commit transaction 2, after the standby was launched.
    cur2.execute("COMMIT")

    # The replica should not see transaction 2 as committed.
    conn_replica = endpoint_at_lsn.connect()
    cur_replica = conn_replica.cursor()
    cur_replica.execute("SELECT * FROM clogtest")
    assert cur_replica.fetchall() == [(1,), (3,)]


def test_ondemand_download_after_wal_switch(neon_env_builder: NeonEnvBuilder):
    """
    Test on-demand SLRU download on standby, when starting right after
    WAL segment switch.

    This is a repro for a bug in how the LSN at WAL page/segment
    boundary was handled (https://github.com/neondatabase/neon/issues/8030)
    """

    tenant_conf = {
        "lazy_slru_download": "true",
    }
    env = neon_env_builder.init_start(initial_tenant_conf=tenant_conf)

    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create a test table
    cur.execute("CREATE TABLE clogtest (id integer)")
    cur.execute("INSERT INTO clogtest VALUES (1)")

    # Start standby at WAL segment boundary
    cur.execute("SELECT pg_switch_wal()")
    lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_insert_lsn()"))
    _endpoint_at_lsn = env.endpoints.create_start(
        branch_name="main", endpoint_id="ep-at-lsn", lsn=lsn
    )
