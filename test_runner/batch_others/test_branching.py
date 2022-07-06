from typing import List
import threading
import pytest
from fixtures.neon_fixtures import NeonEnv, PgBin, Postgres
import time
import random
from fixtures.log_helper import log
from performance.test_perf_pgbench import get_scales_matrix


@pytest.mark.parametrize("n_branches", [10])
@pytest.mark.parametrize("scale", get_scales_matrix(1))
def test_cascade_branching_with_pgbench(neon_simple_env: NeonEnv,
                                        pg_bin: PgBin,
                                        n_branches: int,
                                        scale: int):
    env = neon_simple_env

    tenant, _ = env.neon_cli.create_tenant(
         conf={
             'gc_period': '5 s',
             'gc_horizon': f'{1024 ** 2}',
             'checkpoint_distance': f'{1024 ** 2}',
             'compaction_target_size': f'{1024 ** 2}',
             'compaction_period': '5 s',
             'compaction_threshold': '2',
             # set PITR interval to be small, so we can do GC
             'pitr_interval': '5 s'
         })

    def run_pgbench(pg: Postgres):
        connstr = pg.connstr()

        log.info(f"Start a pgbench workload on pg {connstr}")

        pg_bin.run_capture(['pgbench', '-i', f'-s{scale}', connstr])
        pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -N -M prepared'.split() + [connstr])
        pg_bin.run_capture(['pgbench'] + '-c 10 -T 10 -S -M prepared'.split() + [connstr])

    env.neon_cli.create_branch('b0', tenant_id=tenant)
    pgs: List[Postgres] = []
    pgs.append(env.postgres.create_start('b0', tenant_id=tenant))

    threads: List[threading.Thread] = []
    threads.append(threading.Thread(target=run_pgbench, args=(pgs[0], )))
    threads[-1].start()

    for i in range(n_branches):
        # random a delay between [0, 5]
        delay = random.random() * 5
        time.sleep(delay)
        log.info(f"Sleep {delay}s")

        env.neon_cli.create_branch('b{}'.format(i + 1), 'b{}'.format(i), tenant_id=tenant)
        pgs.append(env.postgres.create_start('b{}'.format(i + 1), tenant_id=tenant))

        threads.append(threading.Thread(target=run_pgbench, args=(pgs[-1], )))
        threads[-1].start()

    for thread in threads:
        thread.join()

    for pg in pgs:
        res = pg.safe_psql('SELECT count(*) from pgbench_accounts')
        assert res[0] == (100000 * scale, )
