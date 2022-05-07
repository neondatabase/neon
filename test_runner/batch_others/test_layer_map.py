from contextlib import closing

import psycopg2.extras
import pytest
import time
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, ZenithPageserverApiException


#
# Create ancestor branches off the main branch.
#
def test_layer_map(zenith_env_builder: ZenithEnvBuilder, zenbenchmark):

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()
    n_iters = 10
    n_records = 100000

    # Override defaults, 1M gc_horizon and 4M checkpoint_distance.
    # Extend compaction_period and gc_period to disable background compaction and gc.
    tenant = env.zenith_cli.create_tenant(
        conf={
            'gc_period': '100 m',
            'gc_horizon': '1048576',
            'checkpoint_distance': '8192',
            'compaction_period': '1 s',
            'compaction_threshold': '1',
            'compaction_target_size': '8192',
        })

    env.zenith_cli.create_timeline(f'main', tenant_id=tenant)
    pg = env.postgres.create_start('main', tenant_id=tenant)
    cur = pg.connect().cursor()
    cur.execute("create table t(x integer)")
    for i in range(n_iters):
        cur.execute(f'insert into t values (generate_series(1,{n_records}))')
        time.sleep(1)

    cur.execute('vacuum t')
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT count(*) from t')
        assert cur.fetchone()[0] == n_iters * n_records
