# Test sequential scan speed
#
from contextlib import closing
from dataclasses import dataclass
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.compare_fixtures import PgCompare
import pytest

def test_update(zenith_with_baseline: PgCompare):
    env = zenith_with_baseline

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t (pk integer, val bigint default 0, t text default repeat(' ', 100))")
            cur.execute('insert into t (pk) values (generate_series(1,10000000))')
            with env.record_duration('update'):
                cur.execute('update t set val=val+1')
            with env.record_duration('select'):
                cur.execute('select sum(val) from t')
