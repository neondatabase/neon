import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnvBuilder


def test_branching(zenith_env_builder: ZenithEnvBuilder, pg_bin):

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()

    env.pageserver.start(overrides=[
        '--pageserver-config-override="gc_period"="10 s"',
        '--pageserver-config-override="gc_horizon"=1024',
        '--pageserver-config-override="checkpoint_distance"=4194304',
        '--pageserver-config-override="compaction_period"="10 m"',
        '--pageserver-config-override="compaction_threshold"=2',
        '--pageserver-config-override="compaction_target_size"=4194304'
    ])
    env.safekeepers[0].start()

    env.zenith_cli.create_branch('b0')
    pg = env.postgres.create_start('b0')
    connstr = pg.connstr()
    branches = 10

    pg_bin.run_capture(['pgbench', '-i', '-s', '100', connstr])
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -N -M prepared'.split() + [connstr])
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -S -M prepared'.split() + [connstr])

    for i in range(branches):
        env.zenith_cli.create_branch('b{}'.format(i+1), 'b{}'.format(i))
        pg = env.postgres.create_start('b{}'.format(i+1))
        connstr = pg.connstr()
        pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -N -M prepared'.split() + [connstr])
        pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -S -M prepared'.split() + [connstr])

    conn = pg.connect()
    cur = conn.cursor()
    cur.execute('SELECT count(aid) from pgbench_accounts')
    assert cur.fetchone()[0] == 10000000
