import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnvBuilder

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Create a couple of branches off the main branch, at a historical point in time.
#
def test_branch_behind(zenith_env_builder: ZenithEnvBuilder):

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    # Branch at the point where only 100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind", "main"])

    pgmain = env.postgres.create_start('test_branch_behind')
    log.info("postgres is running on 'test_branch_behind' branch")

    main_pg_conn = pgmain.connect()
    main_cur = main_pg_conn.cursor()

    main_cur.execute("SHOW zenith.zenith_timeline")
    timeline = main_cur.fetchone()[0]

    # Create table, and insert the first 100 rows
    main_cur.execute('CREATE TABLE foo (t text)')

    # keep some early lsn to test branch creation on out of date lsn
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    gced_lsn = main_cur.fetchone()[0]

    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_a = main_cur.fetchone()[0]
    log.info(f'LSN after 100 rows: {lsn_a}')

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_b = main_cur.fetchone()[0]
    log.info(f'LSN after 200100 rows: {lsn_b}')

    # Branch at the point where only 100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind_hundred", "test_branch_behind@" + lsn_a])

    # Insert many more rows. This generates enough WAL to fill a few segments.
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')

    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_c = main_cur.fetchone()[0]
    log.info(f'LSN after 400100 rows: {lsn_c}')

    # Branch at the point where only 200100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind_more", "test_branch_behind@" + lsn_b])

    pg_hundred = env.postgres.create_start("test_branch_behind_hundred")
    pg_more = env.postgres.create_start("test_branch_behind_more")

    # On the 'hundred' branch, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute('SELECT count(*) FROM foo')
    assert hundred_cur.fetchone() == (100, )

    # On the 'more' branch, we should see 100200 rows
    more_pg_conn = pg_more.connect()
    more_cur = more_pg_conn.cursor()
    more_cur.execute('SELECT count(*) FROM foo')
    assert more_cur.fetchone() == (200100, )

    # All the rows are visible on the main branch
    main_cur.execute('SELECT count(*) FROM foo')
    assert main_cur.fetchone() == (400100, )

    # Check bad lsn's for branching

    # branch at segment boundary
    env.zenith_cli(["branch", "test_branch_segment_boundary", "test_branch_behind@0/3000000"])
    pg = env.postgres.create_start("test_branch_segment_boundary")
    cur = pg.connect().cursor()
    cur.execute('SELECT 1')
    assert cur.fetchone() == (1, )

    # branch at pre-initdb lsn
    with pytest.raises(Exception, match="invalid branch start lsn"):
        env.zenith_cli(["branch", "test_branch_preinitdb", "test_branch_behind@0/42"])

    # check that we cannot create branch based on garbage collected data
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:
            # call gc to advace latest_gc_cutoff_lsn
            pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
            row = pscur.fetchone()
            print_gc_result(row)

    with pytest.raises(Exception, match="invalid branch start lsn"):
        # this gced_lsn is pretty random, so if gc is disabled this woudln't fail
        env.zenith_cli(["branch", "test_branch_create_fail", f"test_branch_behind@{gced_lsn}"])

    # check that after gc everything is still there
    hundred_cur.execute('SELECT count(*) FROM foo')
    assert hundred_cur.fetchone() == (100, )

    more_cur.execute('SELECT count(*) FROM foo')
    assert more_cur.fetchone() == (200100, )

    main_cur.execute('SELECT count(*) FROM foo')
    assert main_cur.fetchone() == (400100, )
