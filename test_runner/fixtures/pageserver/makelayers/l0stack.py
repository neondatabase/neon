from dataclasses import dataclass

from psycopg2.extensions import connection as PgConnection

from fixtures.common_types import Lsn, TenantShardId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import Endpoint
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_last_record_lsn


@dataclass
class L0StackShape:
    logical_table_size_mib: int = 50
    delta_stack_height: int = 20


def make_l0_stack(endpoint: Endpoint, shape: L0StackShape):
    """
    Creates stack of L0 deltas each of which should have 1 Value::Delta per page in table `data`.
    """
    env = endpoint.env

    # TDOO: wait for storcon to finish any reonciles before jumping to action here?
    description = env.storage_controller.tenant_describe(endpoint.tenant_id)
    shards = description["shards"]
    assert len(shards) == 1, "does not support sharding"
    tenant_shard_id = TenantShardId.parse(shards[0]["tenant_shard_id"])

    endpoint.config(["full_page_writes=off"])
    endpoint.reconfigure()

    ps = env.get_pageserver(shards[0]["node_attached"])

    timeline_id = endpoint.show_timeline_id()

    vps_http = env.storage_controller.pageserver_api()
    ps_http = ps.http_client()
    endpoint_conn = endpoint.connect()
    make_l0_stack_standalone(vps_http, ps_http, tenant_shard_id, timeline_id, endpoint_conn, shape)


def make_l0_stack_standalone(
    vps_http: PageserverHttpClient,
    ps_http: PageserverHttpClient,
    tenant_shard_id: TenantShardId,
    timeline_id: TimelineId,
    endpoint_conn: PgConnection,
    shape: L0StackShape,
):
    """
    See make_l0_stack for details.

    This function is a standalone version of make_l0_stack, usable from not-test code.
    """

    assert not tenant_shard_id.shard_index.is_sharded, (
        "the current implementation only supports unsharded tenants"
    )

    tenant_id = tenant_shard_id.tenant_id
    conn = endpoint_conn
    desired_size = shape.logical_table_size_mib * 1024 * 1024

    config = {
        "gc_period": "0s",  # disable periodic gc
        "checkpoint_timeout": "10 years",
        "compaction_period": "1h",  # doesn't matter, but 0 value will kill walredo every 10s
        "compaction_threshold": 100000,  # we just want L0s
        "compaction_target_size": 134217728,
        "checkpoint_distance": 268435456,
        "image_creation_threshold": 100000,  # we just want L0s
    }

    vps_http.set_tenant_config(tenant_id, config)

    conn.autocommit = True
    cur = conn.cursor()

    # Ensure full_page_writes are disabled so that all Value::Delta in
    # pageserver are !will_init, and therefore a getpage needs to read
    # the entire delta stack.
    cur.execute("SHOW full_page_writes")
    assert cur.fetchall()[0][0] == "off", "full_page_writes should be off"

    # each tuple is 23 (header) + 100 bytes = 123 bytes
    # page header si 24 bytes
    # 8k page size
    # (8k-24bytes) / 123 bytes = 63 tuples per page
    # set fillfactor to 10 to have 6 tuples per page
    cur.execute("DROP TABLE IF EXISTS data")
    cur.execute("CREATE TABLE data(id bigint, row char(92)) with (fillfactor=10)")
    need_pages = desired_size // 8192
    need_rows = need_pages * 6
    log.info(f"Need {need_pages} pages, {need_rows} rows")
    cur.execute(f"INSERT INTO data SELECT i,'row'||i FROM generate_series(1, {need_rows}) as i")
    # Raise fillfactor to 100% so that all updates are HOT updates.
    # We assert they're hot updates by checking fetch_id_to_page_mapping remains the same.
    cur.execute("ALTER TABLE data SET (fillfactor=100)")

    def settle_and_flush():
        cur.execute("SELECT pg_current_wal_flush_lsn()")
        flush_lsn = Lsn(cur.fetchall()[0][0])
        wait_for_last_record_lsn(ps_http, tenant_shard_id, timeline_id, flush_lsn)
        ps_http.timeline_checkpoint(tenant_id, timeline_id)

    # create an L0 for the initial data we just inserted
    settle_and_flush()

    # assert we wrote what we think we wrote
    cur.execute("""
        with ntuples_per_page as (
            select (ctid::text::point)[0]::bigint pageno,count(*) ntuples from data group by pageno
        )
        select ntuples, count(*) npages from ntuples_per_page group by ntuples order by ntuples;
    """)
    rows = cur.fetchall()
    log.info(f"initial table layout: {rows}")
    assert len(rows) == 1
    assert rows[0][0] == 6, f"expected 6 tuples per page, got {rows[0][0]}"
    assert rows[0][1] == need_pages, f"expected {need_pages} pages, got {rows[0][1]}"

    def fetch_id_to_page_mapping():
        cur.execute("""
            SELECT id,(ctid::text::point)[0]::bigint pageno FROM data ORDER BY id
        """)
        return cur.fetchall()

    initial_mapping = fetch_id_to_page_mapping()

    # every iteration updates one tuple in each page
    delta_stack_height = shape.delta_stack_height
    for i in range(0, delta_stack_height):
        log.info(i)
        cur.execute(f"UPDATE data set row = row||',u' where id % 6 = {i % 6}")
        log.info(f"modified rows: {cur.rowcount}")
        assert cur.rowcount == need_pages
        settle_and_flush()
        post_update_mapping = fetch_id_to_page_mapping()
        assert initial_mapping == post_update_mapping, "Postgres should be doing HOT updates"

    # Assert the layer count is what we expect it is
    layer_map = vps_http.layer_map_info(tenant_id, timeline_id)
    assert (
        len(layer_map.delta_l0_layers()) == delta_stack_height + 1 + 1
    )  # +1 for the initdb layer + 1 for the table creation & fill
    assert len(layer_map.delta_l0_layers()) == len(layer_map.delta_layers())  # it's all L0s
    assert len(layer_map.image_layers()) == 0  # no images
