import subprocess
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Create a couple of branches off the main branch, at a historical point in time.
#
def test_branch_behind(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    # Branch at the point where only 100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind", "empty"])

    pgmain = env.postgres.create_start('test_branch_behind')
    log.info("postgres is running on 'test_branch_behind' branch")

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
    log.info(f'LSN after 100 rows: {lsn_a}')

    # Branch at the point where only 100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind_hundred", "test_branch_behind@" + lsn_a])

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute('''
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_b = main_cur.fetchone()[0]
    log.info(f'LSN after 200100 rows: {lsn_b}')
    # Branch at the point where only 200100 rows were inserted
    env.zenith_cli(["branch", "test_branch_behind_more", "test_branch_behind@" + lsn_b])

    # # Branch at the point where only 100 rows were inserted
    # env.zenith_cli(["branch", "test_branch_behind_hundred", "test_branch_behind@" + lsn_a])

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
    main_cur.execute("SELECT pg_relation_filepath('foo');")
    log.warn("pg_relation_filepath('foo') %s", main_cur.fetchone())

    # # Branch at the point where only 200100 rows were inserted
    # env.zenith_cli(["branch", "test_branch_behind_more", "test_branch_behind@" + lsn_b])

    pg_hundred = env.postgres.create_start("test_branch_behind_hundred")
    pg_more = env.postgres.create_start("test_branch_behind_more")

    # On the 'hundred' branch, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    # All the rows are visible on the main branch
    main_cur.execute('SELECT count(*) FROM foo')
    assert main_cur.fetchone() == (400100, )

    # psycopg2.errors.UndefinedTable: relation "foo" does not exist
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

    # # branch at segment boundary
    # env.zenith_cli(["branch", "test_branch_segment_boundary", "test_branch_behind@0/3000000"])
    # pg = env.postgres.create_start("test_branch_segment_boundary")
    # cur = pg.connect().cursor()
    # cur.execute('SELECT 1')
    # assert cur.fetchone() == (1, )

    # # branch at pre-initdb lsn
    # #
    # # FIXME: This works currently, but probably shouldn't be allowed
    # try:
    #     env.zenith_cli(["branch", "test_branch_preinitdb", "test_branch_behind@0/42"])
    #     # FIXME: assert false, "branch with invalid LSN should have failed"
    # except subprocess.CalledProcessError:
    #     log.info("Branch creation with pre-initdb LSN failed (as expected)")
