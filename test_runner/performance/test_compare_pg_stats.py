import os
from typing import List

import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.pg_stats import PgStatTable

from performance.test_perf_pgbench import get_scales_matrix


def get_seeds_matrix():
    seeds = os.getenv("TEST_PG_BENCH_SEEDS_MATRIX", default="100")
    return list(map(int, seeds.split(",")))


def get_transactions_matrix():
    transactions = os.getenv("TEST_PG_BENCH_TRANSACTIONS_MATRIX", default="10000")
    return list(map(int, transactions.split(",")))


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("transactions", get_transactions_matrix())
def test_compare_pg_stats_with_pgbench(neon_with_baseline: PgCompare,
                                       pg_stats: List[PgStatTable],
                                       seed: int,
                                       scale: int,
                                       transactions: int):
    env = neon_with_baseline

    env.zenbenchmark.record("seed", seed, '', MetricReport.TEST_PARAM)
    env.zenbenchmark.record("scale", scale, '', MetricReport.TEST_PARAM)
    env.zenbenchmark.record("transactions", transactions, '', MetricReport.TEST_PARAM)

    # initialize pgbench
    with env.record_pg_stats('init', pg_stats):
        env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
        env.flush()

    # run pgbench workloads
    with env.record_pg_stats('simple-update', pg_stats):
        env.pg_bin.run_capture([
            'pgbench',
            '-n',
            f'-t{transactions}',
            f'--random-seed={seed}',
            '-Mprepared',
            env.pg.connstr()
        ])
        env.flush()
    with env.record_pg_stats('simple-select', pg_stats):
        env.pg_bin.run_capture([
            'pgbench',
            '-S',
            f'-t{transactions}',
            f'--random-seed={seed}',
            '-Mprepared',
            env.pg.connstr()
        ])
        env.flush()
