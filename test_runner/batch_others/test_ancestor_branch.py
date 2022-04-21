import subprocess
import asyncio
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnvBuilder


#
# Create ancestor branches off the main branch.
#
def test_ancestor_branch(zenith_env_builder: ZenithEnvBuilder):

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

    pg_branch0 = env.postgres.create_start('main')
    branch0_cur = pg_branch0.connect().cursor()
    branch0_cur.execute("SHOW zenith.zenith_timeline")
    branch0_timeline = branch0_cur.fetchone()[0]
    log.info(f"b0 timeline {branch0_timeline}")

    # Create table, and insert 100k rows.
    branch0_cur.execute('SELECT pg_current_wal_insert_lsn()')
    branch0_lsn = branch0_cur.fetchone()[0]
    log.info(f"b0 at lsn {branch0_lsn}")

    branch0_cur.execute('CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)')
    branch0_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch0:' || g
            FROM generate_series(1, 100000) g
    ''')
    branch0_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_100 = branch0_cur.fetchone()[0]
    log.info(f'LSN after 100 rows: {lsn_100}')

    # Create branch1.
    env.zenith_cli.create_branch('branch1', 'main', ancestor_start_lsn=lsn_100)
    pg_branch1 = env.postgres.create_start('branch1')
    log.info("postgres is running on 'branch1' branch")

    branch1_cur = pg_branch1.connect().cursor()
    branch1_cur.execute("SHOW zenith.zenith_timeline")
    branch1_timeline = branch1_cur.fetchone()[0]
    log.info(f"b1 timeline {branch1_timeline}")

    branch1_cur.execute('SELECT pg_current_wal_insert_lsn()')
    branch1_lsn = branch1_cur.fetchone()[0]
    log.info(f"b1 at lsn {branch1_lsn}")

    # Insert 100k rows.
    branch1_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch1:' || g
            FROM generate_series(1, 100000) g
    ''')
    branch1_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_200 = branch1_cur.fetchone()[0]
    log.info(f'LSN after 100 rows: {lsn_200}')

    # Create branch2.
    env.zenith_cli.create_branch('branch2', 'branch1', ancestor_start_lsn=lsn_200)
    pg_branch2 = env.postgres.create_start('branch2')
    log.info("postgres is running on 'branch1' branch")

    branch2_cur = pg_branch2.connect().cursor()
    branch2_cur.execute("SHOW zenith.zenith_timeline")
    branch2_lsn = branch2_cur.fetchone()[0]
    log.info(f"b2 timeline {branch1_timeline}")

    branch2_cur.execute('SELECT pg_current_wal_insert_lsn()')
    branch2_lsn = branch2_cur.fetchone()[0]
    log.info(f"b2 at lsn {branch2_lsn}")

    # Insert 100k rows.
    branch2_cur.execute('''
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch2:' || g
            FROM generate_series(1, 100000) g
    ''')
    branch2_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_300 = branch2_cur.fetchone()[0]
    log.info(f'LSN after 300 rows: {lsn_300}')

    branch0_cur.execute('SELECT count(*) FROM foo')
    assert branch0_cur.fetchone() == (100000, )

    branch1_cur.execute('SELECT count(*) FROM foo')
    assert branch1_cur.fetchone() == (200000, )

    branch2_cur.execute('SELECT count(*) FROM foo')
    assert branch2_cur.fetchone() == (300000, )
