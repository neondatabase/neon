from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, ZenithPageserverApiException


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
    env = zenith_env_builder.init_start()

    # Override defaults, 1M gc_horizon and 4M checkpoint_distance.
    # Extend compaction_period and gc_period to disable background compaction and gc.
    tenant, _ = env.zenith_cli.create_tenant(
        conf={
            'gc_period': '10 m',
            'gc_horizon': '1048576',
            'checkpoint_distance': '4194304',
            'compaction_period': '10 m',
            'compaction_threshold': '2',
            'compaction_target_size': '4194304',
        })

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:
            pscur.execute("failpoints flush-frozen=sleep(10000)")

    pg_branch0 = env.postgres.create_start('main', tenant_id=tenant)
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
    log.info(f'LSN after 100k rows: {lsn_100}')

    # Create branch1.
    env.zenith_cli.create_branch('branch1', 'main', tenant_id=tenant, ancestor_start_lsn=lsn_100)
    pg_branch1 = env.postgres.create_start('branch1', tenant_id=tenant)
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
    log.info(f'LSN after 200k rows: {lsn_200}')

    # Create branch2.
    env.zenith_cli.create_branch('branch2', 'branch1', tenant_id=tenant, ancestor_start_lsn=lsn_200)
    pg_branch2 = env.postgres.create_start('branch2', tenant_id=tenant)
    log.info("postgres is running on 'branch2' branch")
    branch2_cur = pg_branch2.connect().cursor()

    branch2_cur.execute("SHOW zenith.zenith_timeline")
    branch2_timeline = branch2_cur.fetchone()[0]
    log.info(f"b2 timeline {branch2_timeline}")

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
    log.info(f'LSN after 300k rows: {lsn_300}')

    # Run compaction on branch1.
    psconn = env.pageserver.connect()
    log.info(f'compact {tenant.hex} {branch1_timeline} {lsn_200}')
    psconn.cursor().execute(f'''compact {tenant.hex} {branch1_timeline} {lsn_200}''')

    branch0_cur.execute('SELECT count(*) FROM foo')
    assert branch0_cur.fetchone() == (100000, )

    branch1_cur.execute('SELECT count(*) FROM foo')
    assert branch1_cur.fetchone() == (200000, )

    branch2_cur.execute('SELECT count(*) FROM foo')
    assert branch2_cur.fetchone() == (300000, )


def test_ancestor_branch_detach(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env

    parent_timeline_id = env.zenith_cli.create_branch("test_ancestor_branch_detach_parent", "empty")

    env.zenith_cli.create_branch("test_ancestor_branch_detach_branch1",
                                 "test_ancestor_branch_detach_parent")

    ps_http = env.pageserver.http_client()
    with pytest.raises(ZenithPageserverApiException,
                       match="Failed to detach inmem tenant timeline"):
        ps_http.timeline_detach(env.initial_tenant, parent_timeline_id)
