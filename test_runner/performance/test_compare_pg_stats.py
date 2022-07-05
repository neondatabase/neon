import os
from typing import List

import pytest
from fixtures.compare_fixtures import PgCompare
from fixtures.pg_stats import PgStatTable

from performance.test_perf_pgbench import get_durations_matrix, get_scales_matrix


def get_seeds_matrix(default: int = 100):
    seeds = os.getenv("TEST_PG_BENCH_SEEDS_MATRIX", default=str(default))
    return list(map(int, seeds.split(",")))


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_rw_with_pgbench_default(neon_with_baseline: PgCompare,
                                                  seed: int,
                                                  scale: int,
                                                  duration: int,
                                                  pg_stats_rw: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_rw):
        env.pg_bin.run_capture(
            ['pgbench', f'-T{duration}', f'--random-seed={seed}', env.pg.connstr()])
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wo_with_pgbench_simple_update(neon_with_baseline: PgCompare,
                                                        seed: int,
                                                        scale: int,
                                                        duration: int,
                                                        pg_stats_wo: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wo):
        env.pg_bin.run_capture(
            ['pgbench', '-N', f'-T{duration}', f'--random-seed={seed}', env.pg.connstr()])
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_ro_with_pgbench_select_only(neon_with_baseline: PgCompare,
                                                      seed: int,
                                                      scale: int,
                                                      duration: int,
                                                      pg_stats_ro: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_ro):
        env.pg_bin.run_capture(
            ['pgbench', '-S', f'-T{duration}', f'--random-seed={seed}', env.pg.connstr()])
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wal_with_pgbench_default(neon_with_baseline: PgCompare,
                                                   seed: int,
                                                   scale: int,
                                                   duration: int,
                                                   pg_stats_wal: List[PgStatTable]):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(['pgbench', f'-s{scale}', '-i', env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wal):
        env.pg_bin.run_capture(
            ['pgbench', f'-T{duration}', f'--random-seed={seed}', env.pg.connstr()])
        env.flush()
