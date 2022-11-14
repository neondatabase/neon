from contextlib import closing

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_pageserver_restart(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_pageserver_restart")
    pg = env.postgres.create_start("test_pageserver_restart")

    pg_conn = pg.connect()
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
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_ize
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

    # Stopping the pageserver breaks the connection from the postgres backend to
    # the page server, and causes the next query on the connection to fail. Start a new
    # postgres connection too, to avoid that error. (Ideally, the compute node would
    # handle that and retry internally, without propagating the error to the user, but
    # currently it doesn't...)
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    # Stop the page server by force, and restart it
    env.pageserver.stop()
    env.pageserver.start()


# Test that repeatedly kills and restarts the page server, while the
# safekeeper and compute node keep running.
@pytest.mark.timeout(540)
def test_pageserver_chaos(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    # These warnings are expected, when the pageserver is restarted abruptly
    env.pageserver.allowed_errors.append(".*found future image layer.*")
    env.pageserver.allowed_errors.append(".*found future delta layer.*")

    # Use a tiny checkpoint distance, to create a lot of layers quickly.
    # That allows us to stress the compaction and layer flushing logic more.
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            "checkpoint_distance": "5000000",
        }
    )
    env.neon_cli.create_timeline("test_pageserver_chaos", tenant_id=tenant)
    pg = env.postgres.create_start("test_pageserver_chaos", tenant_id=tenant)

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with closing(pg.connect()) as conn:
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
            select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_ize
            from pg_settings where name = 'shared_buffers'
            """
            )
            row = cur.fetchone()
            assert row is not None
            log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
            assert int(row[0]) < int(row[1])

    # Update the whole table, then immediately kill and restart the pageserver
    for i in range(1, 15):
        pg.safe_psql("UPDATE foo set updates = updates + 1")

        # This kills the pageserver immediately, to simulate a crash
        env.pageserver.stop(immediate=True)
        env.pageserver.start()

        # Stopping the pageserver breaks the connection from the postgres backend to
        # the page server, and causes the next query on the connection to fail. Start a new
        # postgres connection too, to avoid that error. (Ideally, the compute node would
        # handle that and retry internally, without propagating the error to the user, but
        # currently it doesn't...)
        pg_conn = pg.connect()
        cur = pg_conn.cursor()

        # Check that all the updates are visible
        num_updates = pg.safe_psql("SELECT sum(updates) FROM foo")[0][0]
        assert num_updates == i * 100000
