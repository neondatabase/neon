import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, wait_for_last_record_lsn
from fixtures.types import Lsn
from fixtures.utils import query_scalar


#
# Create read-only compute nodes, anchored at historical points in time.
#
# This is very similar to the 'test_branch_behind' test, but instead of
# creating branches, creates read-only nodes.
#
def test_readonly_node(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_readonly_node", "empty")
    pgmain = env.postgres.create_start("test_readonly_node")
    log.info("postgres is running on 'test_readonly_node' branch")

    env.pageserver.allowed_errors.append(".*basebackup .* failed: invalid basebackup lsn.*")

    main_pg_conn = pgmain.connect()
    main_cur = main_pg_conn.cursor()

    # Create table, and insert the first 100 rows
    main_cur.execute("CREATE TABLE foo (t text)")

    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100) g
    """
    )
    main_cur.execute("SELECT pg_current_wal_insert_lsn()")
    lsn_a = query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info("LSN after 100 rows: " + lsn_a)

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    """
    )
    lsn_b = query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info("LSN after 200100 rows: " + lsn_b)

    # Insert many more rows. This generates enough WAL to fill a few segments.
    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    """
    )

    lsn_c = query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info("LSN after 400100 rows: " + lsn_c)

    # Create first read-only node at the point where only 100 rows were inserted
    pg_hundred = env.postgres.create_start(
        branch_name="test_readonly_node", node_name="test_readonly_node_hundred", lsn=lsn_a
    )

    # And another at the point where 200100 rows were inserted
    pg_more = env.postgres.create_start(
        branch_name="test_readonly_node", node_name="test_readonly_node_more", lsn=lsn_b
    )

    # On the 'hundred' node, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute("SELECT count(*) FROM foo")
    assert hundred_cur.fetchone() == (100,)

    # On the 'more' node, we should see 100200 rows
    more_pg_conn = pg_more.connect()
    more_cur = more_pg_conn.cursor()
    more_cur.execute("SELECT count(*) FROM foo")
    assert more_cur.fetchone() == (200100,)

    # All the rows are visible on the main branch
    main_cur.execute("SELECT count(*) FROM foo")
    assert main_cur.fetchone() == (400100,)

    # Check creating a node at segment boundary
    pg = env.postgres.create_start(
        branch_name="test_readonly_node",
        node_name="test_branch_segment_boundary",
        lsn=Lsn("0/3000000"),
    )
    cur = pg.connect().cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)

    # Create node at pre-initdb lsn
    with pytest.raises(Exception, match="invalid basebackup lsn"):
        # compute node startup with invalid LSN should fail
        env.postgres.create_start(
            branch_name="test_readonly_node",
            node_name="test_readonly_node_preinitdb",
            lsn=Lsn("0/42"),
        )


# Similar test, but with more data, and we force checkpoints
def test_timetravel(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http_client = env.pageserver.http_client()
    env.neon_cli.create_branch("test_timetravel", "empty")
    pg = env.postgres.create_start("test_timetravel")

    client = env.pageserver.http_client()

    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]

    lsns = []

    with pg.cursor() as cur:
        cur.execute(
            """
        CREATE TABLE testtab(id serial primary key, iteration int, data text);
        INSERT INTO testtab (iteration, data) SELECT 0, 'data' FROM generate_series(1, 100000);
        """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
    lsns.append((0, current_lsn))

    for i in range(1, 5):
        with pg.cursor() as cur:
            cur.execute(f"UPDATE testtab SET iteration = {i}")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))
        lsns.append((i, current_lsn))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

        # run checkpoint manually to force a new layer file
        pageserver_http_client.timeline_checkpoint(tenant_id, timeline_id)

    ##### Restart pageserver
    env.postgres.stop_all()
    env.pageserver.stop()
    env.pageserver.start()

    for (i, lsn) in lsns:
        pg_old = env.postgres.create_start(
            branch_name="test_timetravel", node_name=f"test_old_lsn_{i}", lsn=lsn
        )
        with pg_old.cursor() as cur:
            assert query_scalar(cur, f"select count(*) from testtab where iteration={i}") == 100000
            assert query_scalar(cur, f"select count(*) from testtab where iteration<>{i}") == 0
