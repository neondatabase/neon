import random
import time
import statistics
import threading
import timeit
import pytest
from typing import List
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import NeonCompare
from fixtures.log_helper import log
from performance.test_perf_pgbench import run_pgbench


@pytest.mark.parametrize("n_branches", [20])
# Test measures the latency of branch creation during a heavy [1] workload.
#
# [1]: to simulate a heavy workload, the test tweaks the GC and compaction settings
# to increase the task's frequency. The test runs `pgbench` in each new branch.
# Each branch is created from a randomly picked source branch.
def test_branch_creation(neon_compare: NeonCompare, n_branches: int):
    env = neon_compare.env
    pg_bin = neon_compare.pg_bin

    # Use aggressive GC and checkpoint settings, so GC and compaction happen more often during the test
    tenant, _ = env.neon_cli.create_tenant(
         conf={
             'gc_period': '5 s',
             'gc_horizon': f'{4 * 1024 ** 2}',
             'checkpoint_distance': f'{2 * 1024 ** 2}',
             'compaction_target_size': f'{1024 ** 2}',
             'compaction_threshold': '2',
             # set PITR interval to be small, so we can do GC
             'pitr_interval': '5 s'
         })

    def run_pgbench(branch: str):
        log.info(f"Start a pgbench workload on branch {branch}")

        pg = env.postgres.create_start(branch, tenant_id=tenant)
        connstr = pg.connstr()

        pg_bin.run_capture(['pgbench', '-i', connstr])
        pg_bin.run_capture(['pgbench', '-c10', '-T10', connstr])

        pg.stop()

    env.neon_cli.create_branch('b0', tenant_id=tenant)

    threads: List[threading.Thread] = []
    threads.append(threading.Thread(target=run_pgbench, args=('b0', ), daemon=True))
    threads[-1].start()

    branch_creation_durations = []
    for i in range(n_branches):
        time.sleep(1.0)

        # random a source branch
        p = random.randint(0, i)

        timer = timeit.default_timer()
        env.neon_cli.create_branch('b{}'.format(i + 1), 'b{}'.format(p), tenant_id=tenant)
        dur = timeit.default_timer() - timer

        log.info(f"Creating branch b{i+1} took {dur}s")
        branch_creation_durations.append(dur)

        threads.append(threading.Thread(target=run_pgbench, args=(f'b{i+1}', ), daemon=True))
        threads[-1].start()

    for thread in threads:
        thread.join()

    neon_compare.zenbenchmark.record("branch_creation_duration_max",
                                     max(branch_creation_durations),
                                     's',
                                     MetricReport.LOWER_IS_BETTER)
    neon_compare.zenbenchmark.record("branch_creation_duration_avg",
                                     statistics.mean(branch_creation_durations),
                                     's',
                                     MetricReport.LOWER_IS_BETTER)
    neon_compare.zenbenchmark.record("branch_creation_duration_stdev",
                                     statistics.stdev(branch_creation_durations),
                                     's',
                                     MetricReport.LOWER_IS_BETTER)


@pytest.mark.parametrize("n_branches", [1024])
def test_branch_with_many_children(neon_compare: NeonCompare, n_branches: int):
    env = neon_compare.env

    env.neon_cli.create_branch('b0')
    env.neon_cli.create_branch('other')

    pg0 = env.postgres.create_start('b0')
    pg_other = env.postgres.create_start('other')

    neon_compare.pg_bin.run_capture(['pgbench', '-i', '-s10', pg0.connstr()])
    neon_compare.pg_bin.run_capture(['pgbench', '-i', '-s10', pg_other.connstr()])

    for i in range(n_branches):
        env.neon_cli.create_branch('b{}'.format(i + 1), 'b0')

    run_pgbench(neon_compare, "many_children_branch", ['pgbench', '-c10', '-T10', pg0.connstr()])
    run_pgbench(neon_compare, "no_children_branch", ['pgbench', '-c10', '-T10', pg_other.connstr()])
