import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnvBuilder


def test_many_branches(zenith_env_builder: ZenithEnvBuilder, zenbenchmark):

    branches = 50
    records = 10000

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('b0')
    pg = env.postgres.create_start('b0')

    conn = pg.connect()
    cur = conn.cursor()
    cur.execute('create table t(x integer)')

    for i in range(branches):
        env.zenith_cli.create_branch('b{}'.format(i + 1), 'b{}'.format(i))
        pg = env.postgres.create_start('b{}'.format(i + 1))
        conn = pg.connect()
        cur = conn.cursor()
        cur.execute(f'insert into t values (generate_series(1,{records}))')
        cur.execute('SELECT count(*) from t')
        rows = cur.fetchone()[0]
        print('Iteration {i} rows {rows}')
    cur.execute('vacuum t')
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT count(*) from t')
        assert cur.fetchone()[0] == branches * records
