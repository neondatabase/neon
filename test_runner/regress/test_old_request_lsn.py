from __future__ import annotations

from fixtures.common_types import TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import print_gc_result, query_scalar


#
# Test where Postgres generates a lot of WAL, and it's garbage collected away, but
# no pages are evicted so that Postgres uses an old LSN in a GetPage request.
# We had a bug where the page server failed to find the page version because it
# thought it was already garbage collected away, because the LSN in the GetPage
# request was very old and the WAL from that time was indeed already removed.
# In reality, the LSN on a GetPage request coming from a primary server is
# just a hint that the page hasn't been modified since that LSN, and the page
# server should return the latest page version regardless of the LSN.
#
def test_old_request_lsn(neon_env_builder: NeonEnvBuilder):
    # Disable pitr, because here we want to test branch creation after GC
    env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "0 sec"})
    env.create_branch("test_old_request_lsn", ancestor_branch_name="main")
    endpoint = env.endpoints.create_start("test_old_request_lsn")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    timeline = TimelineId(query_scalar(cur, "SHOW neon.timeline_id"))

    pageserver_http = env.pageserver.http_client()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers.
    cur.execute("CREATE TABLE foo (id int4 PRIMARY KEY, val int, t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT g, 1, 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    # Verify that the table is larger than shared_buffers, so that the SELECT below
    # will cause GetPage requests.
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
    """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    cur.execute("VACUUM foo")

    # Make a lot of updates on a single row, generating a lot of WAL. Trigger
    # garbage collections so that the page server will remove old page versions.
    for _ in range(10):
        pageserver_http.timeline_checkpoint(env.initial_tenant, timeline)
        gc_result = pageserver_http.timeline_gc(env.initial_tenant, timeline, 0)
        print_gc_result(gc_result)

        for _ in range(100):
            cur.execute("UPDATE foo SET val = val + 1 WHERE id = 1;")

    # All (or at least most of) the updates should've been on the same page, so
    # that we haven't had to evict any dirty pages for a long time. Now run
    # a query that sends GetPage@LSN requests with the old LSN.
    cur.execute("SELECT COUNT(*), SUM(val) FROM foo")
    assert cur.fetchone() == (100000, 101000)
