from contextlib import closing

import pytest
from fixtures.compare_fixtures import NeonCompare
from fixtures.neon_fixtures import wait_for_last_flush_lsn


#
# Test compaction and image layer creation performance.
#
# This creates a few tables and runs some simple INSERTs and UPDATEs on them to generate
# some delta layers. Then it runs manual compaction, measuring how long it takes.
#
@pytest.mark.timeout(1000)
def test_compaction(neon_compare: NeonCompare):
    env = neon_compare.env
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant(
        conf={
            # Disable background GC and compaction, we'll run compaction manually.
            "gc_period": "0s",
            "compaction_period": "0s",
            # Make checkpoint distance somewhat smaller than default, to create
            # more delta layers quicker, to trigger compaction.
            "checkpoint_distance": "25000000",  # 25 MB
            # Force image layer creation when we run compaction.
            "image_creation_threshold": "1",
        }
    )
    neon_compare.tenant = tenant_id
    neon_compare.timeline = timeline_id

    # Create some tables, and run a bunch of INSERTs and UPDATes on them,
    # to generate WAL and layers
    pg = env.postgres.create_start(
        "main", tenant_id=tenant_id, config_lines=["shared_buffers=512MB"]
    )

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            for i in range(100):
                cur.execute(f"create table tbl{i} (i int, j int);")
                cur.execute(f"insert into tbl{i} values (generate_series(1, 1000), 0);")
                for j in range(100):
                    cur.execute(f"update tbl{i} set j = {j};")

    wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)

    # First compaction generates L1 layers
    with neon_compare.zenbenchmark.record_duration("compaction"):
        pageserver_http.timeline_compact(tenant_id, timeline_id)

    # And second compaction triggers image layer creation
    with neon_compare.zenbenchmark.record_duration("image_creation"):
        pageserver_http.timeline_compact(tenant_id, timeline_id)

    neon_compare.report_size()
