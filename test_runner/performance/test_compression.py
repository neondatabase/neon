# Test sequential scan speed
#
from contextlib import closing
from dataclasses import dataclass
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.compare_fixtures import PgCompare
import pytest


@pytest.mark.parametrize('rows', [pytest.param(10000000)])
def test_compression(zenith_with_baseline: PgCompare, rows: int):
    env = zenith_with_baseline

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            with env.record_duration('insert'):
                cur.execute(
                    f'create table t as select generate_series(1,{rows}) as pk,(random()*10)::bigint as r10,(random()*100)::bigint as r100,(random()*1000)::bigint as r1000,(random()*10000)::bigint as r10000'
                )
            cur.execute("vacuum t")
            with env.record_duration('select'):
                cur.execute('select sum(r100) from t')
