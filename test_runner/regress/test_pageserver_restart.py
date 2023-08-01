from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_pageserver_restart")
    endpoint = env.endpoints.create_start("test_pageserver_restart")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    # Verify that the table is larger than shared_buffers
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

    # Stop the pageserver gracefully and restart it.
    env.pageserver.stop()
    env.pageserver.start()

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    # Restart the server again, but delay the loading of tenants, and test what the
    # pageserver does if a compute node connects and sends a request for the tenant
    # while it's still in Loading state. (It waits for the loading to finish, and then
    # processes the request.)
    env.pageserver.stop()
    env.pageserver.start(extra_env_vars={"FAILPOINTS": "before-loading-tenant=return(5000)"})

    # Check that it's in Loading state
    client = env.pageserver.http_client()
    tenant_status = client.tenant_status(env.initial_tenant)
    log.info("Tenant status : %s", tenant_status)
    assert tenant_status["state"]["slug"] == "Loading"

    # Try to read. This waits until the loading finishes, and then return normally.
    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)


# Test that repeatedly kills and restarts the page server, while the
# safekeeper and compute node keep running.
@pytest.mark.timeout(540)
def test_pageserver_chaos(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    # Use a tiny checkpoint distance, to create a lot of layers quickly.
    # That allows us to stress the compaction and layer flushing logic more.
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "checkpoint_distance": "5000000",
        }
    )
    env.neon_cli.create_timeline("test_pageserver_chaos", tenant_id=tenant)
    endpoint = env.endpoints.create_start("test_pageserver_chaos", tenant_id=tenant)

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (id int, t text, updates int)")
            cur.execute("CREATE INDEX ON foo (id)")
            cur.execute(
                """
            INSERT INTO foo
            SELECT g, 'long string to consume some space' || g, 0
            FROM generate_series(1, 100000) g
            """
            )

            # Verify that the table is larger than shared_buffers
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

    # Update the whole table, then immediately kill and restart the pageserver
    for i in range(1, 15):
        endpoint.safe_psql("UPDATE foo set updates = updates + 1")

        # This kills the pageserver immediately, to simulate a crash
        env.pageserver.stop(immediate=True)
        env.pageserver.start()

        # Check that all the updates are visible
        num_updates = endpoint.safe_psql("SELECT sum(updates) FROM foo")[0][0]
        assert num_updates == i * 100000
