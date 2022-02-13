import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnv


#
# Create read-only compute nodes, anchored at historical points in time.
#
# This is very similar to the 'test_branch_behind' test, but instead of
# creating branches, creates read-only nodes.
#
def test_readonly_node(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    test_readonly_node_timeline_id = env.zenith_cli.branch_timeline()
    pgmain = env.postgres.create_start('test_readonly_node',
                                       timeline_id=test_readonly_node_timeline_id)
    log.info("postgres is running on 'test_readonly_node' branch")

    main_pg_conn = pgmain.connect()
    main_cur = main_pg_conn.cursor()

    # Create table, and insert the first 100 rows
    main_cur.execute('CREATE TABLE foo (t text)')

    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_a = main_cur.fetchone()[0]
    log.info('LSN after 100 rows: ' + lsn_a)

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_b = main_cur.fetchone()[0]
    log.info('LSN after 200100 rows: ' + lsn_b)

    # Insert many more rows. This generates enough WAL to fill a few segments.
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    ''')

    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_c = main_cur.fetchone()[0]
    log.info('LSN after 400100 rows: ' + lsn_c)

    # Create first read-only node at the point where only 100 rows were inserted
    pg_hundred = env.postgres.create_start("test_readonly_node_hundred",
                                           timeline_id=test_readonly_node_timeline_id,
                                           lsn=lsn_a)

    # And another at the point where 200100 rows were inserted
    pg_more = env.postgres.create_start("test_readonly_node_more",
                                        timeline_id=test_readonly_node_timeline_id,
                                        lsn=lsn_b)

    # On the 'hundred' node, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute('SELECT count(*) FROM foo')
    assert hundred_cur.fetchone() == (100, )

    # On the 'more' node, we should see 100200 rows
    more_pg_conn = pg_more.connect()
    more_cur = more_pg_conn.cursor()
    more_cur.execute('SELECT count(*) FROM foo')
    assert more_cur.fetchone() == (200100, )

    # All the rows are visible on the main branch
    main_cur.execute('SELECT count(*) FROM foo')
    assert main_cur.fetchone() == (400100, )

    # Check creating a node at segment boundary
    pg = env.postgres.create_start("test_branch_segment_boundary",
                                   timeline_id=test_readonly_node_timeline_id,
                                   lsn='0/3000000')
    cur = pg.connect().cursor()
    cur.execute('SELECT 1')
    assert cur.fetchone() == (1, )

    # Create node at pre-initdb lsn
    with pytest.raises(Exception, match="invalid basebackup lsn"):
        # compute node startup with invalid LSN should fail
        env.zenith_cli.pg_start("test_readonly_node_preinitdb",
                                timeline_id=test_readonly_node_timeline_id,
                                lsn="0/42")
