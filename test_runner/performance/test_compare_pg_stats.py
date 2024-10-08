from __future__ import annotations

import os
import threading
import time

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
def test_compare_pg_stats_rw_with_pgbench_default(
    neon_with_baseline: PgCompare,
    seed: int,
    scale: int,
    duration: int,
    pg_stats_rw: list[PgStatTable],
):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(["pgbench", f"-s{scale}", "-i", env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_rw):
        env.pg_bin.run_capture(
            ["pgbench", f"-T{duration}", f"--random-seed={seed}", env.pg.connstr()]
        )
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wo_with_pgbench_simple_update(
    neon_with_baseline: PgCompare,
    seed: int,
    scale: int,
    duration: int,
    pg_stats_wo: list[PgStatTable],
):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(["pgbench", f"-s{scale}", "-i", env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wo):
        env.pg_bin.run_capture(
            ["pgbench", "-N", f"-T{duration}", f"--random-seed={seed}", env.pg.connstr()]
        )
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_ro_with_pgbench_select_only(
    neon_with_baseline: PgCompare,
    seed: int,
    scale: int,
    duration: int,
    pg_stats_ro: list[PgStatTable],
):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(["pgbench", f"-s{scale}", "-i", env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_ro):
        env.pg_bin.run_capture(
            ["pgbench", "-S", f"-T{duration}", f"--random-seed={seed}", env.pg.connstr()]
        )
        env.flush()


@pytest.mark.parametrize("seed", get_seeds_matrix())
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix(5))
def test_compare_pg_stats_wal_with_pgbench_default(
    neon_with_baseline: PgCompare,
    seed: int,
    scale: int,
    duration: int,
    pg_stats_wal: list[PgStatTable],
):
    env = neon_with_baseline
    # initialize pgbench
    env.pg_bin.run_capture(["pgbench", f"-s{scale}", "-i", env.pg.connstr()])
    env.flush()

    with env.record_pg_stats(pg_stats_wal):
        env.pg_bin.run_capture(
            ["pgbench", f"-T{duration}", f"--random-seed={seed}", env.pg.connstr()]
        )
        env.flush()


@pytest.mark.parametrize("n_tables", [1, 10])
@pytest.mark.parametrize("duration", get_durations_matrix(10))
def test_compare_pg_stats_wo_with_heavy_write(
    neon_with_baseline: PgCompare, n_tables: int, duration: int, pg_stats_wo: list[PgStatTable]
):
    env = neon_with_baseline
    with env.pg.connect().cursor() as cur:
        for i in range(n_tables):
            cur.execute(
                f"CREATE TABLE t{i}(key serial primary key, t text default 'foooooooooooooooooooooooooooooooooooooooooooooooooooo')"
            )

    def start_single_table_workload(table_id: int):
        start = time.time()
        with env.pg.connect().cursor() as cur:
            while time.time() - start < duration:
                cur.execute(f"INSERT INTO t{table_id} SELECT FROM generate_series(1,1000)")

    with env.record_pg_stats(pg_stats_wo):
        threads = [
            threading.Thread(target=start_single_table_workload, args=(i,)) for i in range(n_tables)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
