from __future__ import annotations

from fixtures.neon_fixtures import NeonEnvBuilder


# Test safekeeper sync and pageserver catch up
# while initial compute node is down and pageserver is lagging behind safekeepers.
# Ensure that basebackup after restart of all components is correct
# and new compute node contains all data.
def test_pageserver_catchup_while_compute_down(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_pageserver_catchup_while_compute_down")
    # Make shared_buffers large to ensure we won't query pageserver while it is down.
    endpoint = env.endpoints.create_start(
        "test_pageserver_catchup_while_compute_down", config_lines=["shared_buffers=512MB"]
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 10000) g
    """
    )

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (10000,)

    # Stop and restart pageserver. This is a more or less graceful shutdown, although
    # the page server doesn't currently have a shutdown routine so there's no difference
    # between stopping and crashing.
    env.pageserver.stop()

    # insert some more rows
    # since pageserver is shut down, these will be only on safekeepers
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 10000) g
    """
    )

    # stop safekeepers gracefully
    env.safekeepers[0].stop()
    env.safekeepers[1].stop()
    env.safekeepers[2].stop()

    # start everything again
    # safekeepers must synchronize and pageserver must catch up
    env.pageserver.start()
    env.safekeepers[0].start()
    env.safekeepers[1].start()
    env.safekeepers[2].start()

    # restart compute node
    endpoint.stop_and_destroy().create_start("test_pageserver_catchup_while_compute_down")

    # Ensure that basebackup went correct and pageserver returned all data
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (20000,)
