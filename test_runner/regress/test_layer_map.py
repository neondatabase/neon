from contextlib import closing

import psycopg2.extras
import pytest
import time
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, NeonPageserverApiException


#
# Create ancestor branches off the main branch.
#
def test_layer_map(neon_env_builder: NeonEnvBuilder, zenbenchmark):

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/neondb/neon/issues/1068
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()
    n_iters = 10
    n_records = 100000

    # Override defaults, 1M gc_horizon and 4M checkpoint_distance.
    # Extend compaction_period and gc_period to disable background compaction and gc.
    tenant, _ = env.neon_cli.create_tenant(
        conf={
            'gc_period': '100 m',
            'gc_horizon': '1048576',
            'checkpoint_distance': '8192',
            'compaction_period': '1 s',
            'compaction_threshold': '1',
            'compaction_target_size': '8192',
        })

    env.neon_cli.create_timeline('test_layer_map', tenant_id=tenant)
    pg = env.postgres.create_start('test_layer_map', tenant_id=tenant)
    cur = pg.connect().cursor()
    cur.execute("create table t(x integer)")
    for i in range(n_iters):
        cur.execute(f'insert into t values (generate_series(1,{n_records}))')
        time.sleep(1)

    cur.execute('vacuum t')
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT count(*) from t')
        assert cur.fetchone()[0] == n_iters * n_records
