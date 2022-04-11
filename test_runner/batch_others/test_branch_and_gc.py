import subprocess
import asyncio
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnvBuilder


#
# Create a branches off the main branch and run gc.
#
def test_branch_and_gc(zenith_env_builder: ZenithEnvBuilder):

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    # Override defaults, 1M gc_horizon and 4M checkpoint_distance.
    # Extend compaction_period and gc_period to disable background compaction and gc.
    env.pageserver.start(overrides=[
        '--pageserver-config-override="gc_period"="10 m"',
        '--pageserver-config-override="gc_horizon"=1048576',
        '--pageserver-config-override="checkpoint_distance"=4194304',
        '--pageserver-config-override="compaction_period"="10 m"',
        '--pageserver-config-override="compaction_threshold"=2'
    ])
    env.safekeepers[0].start()

    env.zenith_cli.create_branch('test_main')
    pgmain = env.postgres.create_start('test_main')
    log.info("postgres is running on 'test_main' branch")

    main_cur = pgmain.connect().cursor()
    main_cur.execute("SHOW zenith.zenith_timeline")
    timeline = main_cur.fetchone()[0]

    # Create table, and insert 100 rows.
    main_cur.execute('CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    init_lsn = main_cur.fetchone()[0]
    main_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':main:' || g
            FROM generate_series(1, 100) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_100 = main_cur.fetchone()[0]
    log.info(f'LSN after 100 rows: {lsn_100}')

    # Create branch.
    env.zenith_cli.create_branch('test_branch', 'test_main', ancestor_start_lsn=lsn_100)
    pg_branch = env.postgres.create_start('test_branch')

    # Append more rows to generate multiple layers in ancestor's branch.
    main_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':main:' || g
            FROM generate_series(1, 200000) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_200100 = main_cur.fetchone()[0]
    log.info(f'LSN after 400100 rows: {lsn_200100}')

    # Run gc on main branch at current lsn.
    psconn = env.pageserver.connect()
    psconn.cursor().execute(f'''do_gc {env.initial_tenant.hex} {timeline} {lsn_200100}''')

    # Insert some rows on descendent's branch to diverge.
    branch_cur = pg_branch.connect().cursor()
    branch_cur.execute("SHOW zenith.zenith_timeline")
    branch_timeline = branch_cur.fetchone()[0]
    branch_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch:' || g
            FROM generate_series(1, 100) g
    ''')
    branch_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_branch_200 = branch_cur.fetchone()[0]
    log.info(f'LSN after 200 rows on branch: {lsn_branch_200}')

    branch_cur.execute('SELECT count(*) FROM foo')
    assert branch_cur.fetchone() == (200, )
