import asyncio
import uuid

import asyncpg
import random
import time

from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, Postgres, Safekeeper
from fixtures.log_helper import getLogger
from fixtures.utils import lsn_from_hex, lsn_to_hex
from typing import List, Optional

log = getLogger('root.safekeeper_async')


class BankClient(object):
    def __init__(self, conn: asyncpg.Connection, n_accounts, init_amount):
        self.conn: asyncpg.Connection = conn
        self.n_accounts = n_accounts
        self.init_amount = init_amount

    async def initdb(self):
        await self.conn.execute('DROP TABLE IF EXISTS bank_accs')
        await self.conn.execute('CREATE TABLE bank_accs(uid int primary key, amount int)')
        await self.conn.execute(
            '''
            INSERT INTO bank_accs
            SELECT *, $1 FROM generate_series(0, $2)
        ''',
            self.init_amount,
            self.n_accounts - 1)
        await self.conn.execute('DROP TABLE IF EXISTS bank_log')
        await self.conn.execute('CREATE TABLE bank_log(from_uid int, to_uid int, amount int)')

    async def check_invariant(self):
        row = await self.conn.fetchrow('SELECT sum(amount) AS sum FROM bank_accs')
        assert row['sum'] == self.n_accounts * self.init_amount


async def bank_transfer(conn: asyncpg.Connection, from_uid, to_uid, amount):
    # avoid deadlocks by sorting uids
    if from_uid > to_uid:
        from_uid, to_uid, amount = to_uid, from_uid, -amount

    async with conn.transaction():
        await conn.execute(
            'UPDATE bank_accs SET amount = amount + ($1) WHERE uid = $2',
            amount,
            to_uid,
        )
        await conn.execute(
            'UPDATE bank_accs SET amount = amount - ($1) WHERE uid = $2',
            amount,
            from_uid,
        )
        await conn.execute(
            'INSERT INTO bank_log VALUES ($1, $2, $3)',
            from_uid,
            to_uid,
            amount,
        )


class WorkerStats(object):
    def __init__(self, n_workers):
        self.counters = [0] * n_workers
        self.running = True

    def reset(self):
        self.counters = [0] * len(self.counters)

    def inc_progress(self, worker_id):
        self.counters[worker_id] += 1

    def check_progress(self):
        log.debug("Workers progress: {}".format(self.counters))

        # every worker should finish at least one tx
        assert all(cnt > 0 for cnt in self.counters)

        progress = sum(self.counters)
        log.info('All workers made {} transactions'.format(progress))


async def run_random_worker(stats: WorkerStats, pg: Postgres, worker_id, n_accounts, max_transfer):
    pg_conn = await pg.connect_async()
    log.debug('Started worker {}'.format(worker_id))

    while stats.running:
        from_uid = random.randint(0, n_accounts - 1)
        to_uid = (from_uid + random.randint(1, n_accounts - 1)) % n_accounts
        amount = random.randint(1, max_transfer)

        await bank_transfer(pg_conn, from_uid, to_uid, amount)
        stats.inc_progress(worker_id)

        log.debug('Executed transfer({}) {} => {}'.format(amount, from_uid, to_uid))

    log.debug('Finished worker {}'.format(worker_id))

    await pg_conn.close()


async def wait_for_lsn(safekeeper: Safekeeper,
                       tenant_id: str,
                       timeline_id: str,
                       wait_lsn: str,
                       polling_interval=1,
                       timeout=60):
    """
    Poll flush_lsn from safekeeper until it's greater or equal than
    provided wait_lsn. To do that, timeline_status is fetched from
    safekeeper every polling_interval seconds.
    """

    started_at = time.time()
    client = safekeeper.http_client()

    flush_lsn = client.timeline_status(tenant_id, timeline_id).flush_lsn
    log.info(
        f'Safekeeper at port {safekeeper.port.pg} has flush_lsn {flush_lsn}, waiting for lsn {wait_lsn}'
    )

    while lsn_from_hex(wait_lsn) > lsn_from_hex(flush_lsn):
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(
                f"timed out waiting for safekeeper at port {safekeeper.port.pg} to reach {wait_lsn}, current lsn is {flush_lsn}"
            )

        await asyncio.sleep(polling_interval)
        flush_lsn = client.timeline_status(tenant_id, timeline_id).flush_lsn
        log.debug(f'safekeeper port={safekeeper.port.pg} flush_lsn={flush_lsn} wait_lsn={wait_lsn}')


# This test will run several iterations and check progress in each of them.
# On each iteration 1 acceptor is stopped, and 2 others should allow
# background workers execute transactions. In the end, state should remain
# consistent.
async def run_restarts_under_load(env: NeonEnv,
                                  pg: Postgres,
                                  acceptors: List[Safekeeper],
                                  n_workers=10,
                                  n_accounts=100,
                                  init_amount=100000,
                                  max_transfer=100,
                                  period_time=4,
                                  iterations=10):
    # Set timeout for this test at 5 minutes. It should be enough for test to complete
    # and less than CircleCI's no_output_timeout, taking into account that this timeout
    # is checked only at the beginning of every iteration.
    test_timeout_at = time.monotonic() + 5 * 60

    pg_conn = await pg.connect_async()
    tenant_id = await pg_conn.fetchval("show neon.tenant_id")
    timeline_id = await pg_conn.fetchval("show neon.timeline_id")

    bank = BankClient(pg_conn, n_accounts=n_accounts, init_amount=init_amount)
    # create tables and initial balances
    await bank.initdb()

    stats = WorkerStats(n_workers)
    workers = []
    for worker_id in range(n_workers):
        worker = run_random_worker(stats, pg, worker_id, bank.n_accounts, max_transfer)
        workers.append(asyncio.create_task(worker))

    for it in range(iterations):
        assert time.monotonic() < test_timeout_at, 'test timed out'

        victim_idx = it % len(acceptors)
        victim = acceptors[victim_idx]
        victim.stop()

        flush_lsn = await pg_conn.fetchval('SELECT pg_current_wal_flush_lsn()')
        flush_lsn = lsn_to_hex(flush_lsn)
        log.info(f'Postgres flush_lsn {flush_lsn}')

        pageserver_lsn = env.pageserver.http_client().timeline_detail(
            uuid.UUID(tenant_id), uuid.UUID((timeline_id)))["local"]["last_record_lsn"]
        sk_ps_lag = lsn_from_hex(flush_lsn) - lsn_from_hex(pageserver_lsn)
        log.info(f'Pageserver last_record_lsn={pageserver_lsn} lag={sk_ps_lag / 1024}kb')

        # Wait until alive safekeepers catch up with postgres
        for idx, safekeeper in enumerate(acceptors):
            if idx != victim_idx:
                await wait_for_lsn(safekeeper, tenant_id, timeline_id, flush_lsn)

        stats.reset()
        await asyncio.sleep(period_time)
        # assert that at least one transaction has completed in every worker
        stats.check_progress()

        victim.start()

    log.info('Iterations are finished, exiting coroutines...')
    stats.running = False
    # await all workers
    await asyncio.gather(*workers)
    # assert balances sum hasn't changed
    await bank.check_invariant()
    await pg_conn.close()


# Restart acceptors one by one, while executing and validating bank transactions
def test_restarts_under_load(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_safekeepers_restarts_under_load')
    # Enable backpressure with 1MB maximal lag, because we don't want to block on `wait_for_lsn()` for too long
    pg = env.postgres.create_start('test_safekeepers_restarts_under_load',
                                   config_lines=['max_replication_write_lag=1MB'])

    asyncio.run(run_restarts_under_load(env, pg, env.safekeepers))


# Restart acceptors one by one and test that everything is working as expected
# when checkpoins are triggered frequently by max_wal_size=32MB. Because we have
# wal_keep_size=0, there will be aggressive WAL segments recycling.
def test_restarts_frequent_checkpoints(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_restarts_frequent_checkpoints')
    # Enable backpressure with 1MB maximal lag, because we don't want to block on `wait_for_lsn()` for too long
    pg = env.postgres.create_start('test_restarts_frequent_checkpoints',
                                   config_lines=[
                                       'max_replication_write_lag=1MB',
                                       'min_wal_size=32MB',
                                       'max_wal_size=32MB',
                                       'log_checkpoints=on'
                                   ])

    # we try to simulate large (flush_lsn - truncate_lsn) lag, to test that WAL segments
    # are not removed before broadcasted to all safekeepers, with the help of replication slot
    asyncio.run(run_restarts_under_load(env, pg, env.safekeepers, period_time=15, iterations=5))


def postgres_create_start(env: NeonEnv, branch: str, pgdir_name: Optional[str]):
    pg = Postgres(
        env,
        tenant_id=env.initial_tenant,
        port=env.port_distributor.get_port(),
        # In these tests compute has high probability of terminating on its own
        # before our stop() due to lost consensus leadership.
        check_stop_result=False)

    # embed current time in node name
    node_name = pgdir_name or f'pg_node_{time.time()}'
    return pg.create_start(branch_name=branch,
                           node_name=node_name,
                           config_lines=['log_statement=all'])


async def exec_compute_query(env: NeonEnv,
                             branch: str,
                             query: str,
                             pgdir_name: Optional[str] = None):
    with postgres_create_start(env, branch=branch, pgdir_name=pgdir_name) as pg:
        before_conn = time.time()
        conn = await pg.connect_async()
        res = await conn.fetch(query)
        await conn.close()
        after_conn = time.time()
        log.info(f'{query} took {after_conn - before_conn}s')
        return res


async def run_compute_restarts(env: NeonEnv,
                               queries=16,
                               batch_insert=10000,
                               branch='test_compute_restarts'):
    cnt = 0
    sum = 0

    await exec_compute_query(env, branch, 'CREATE TABLE t (i int)')

    for i in range(queries):
        if i % 4 == 0:
            await exec_compute_query(
                env, branch, f'INSERT INTO t SELECT 1 FROM generate_series(1, {batch_insert})')
            sum += batch_insert
            cnt += batch_insert
        elif (i % 4 == 1) or (i % 4 == 3):
            # Note that select causes lots of FPI's and increases probability of safekeepers
            # standing at different LSNs after compute termination.
            actual_sum = (await exec_compute_query(env, branch, 'SELECT SUM(i) FROM t'))[0][0]
            assert actual_sum == sum, f'Expected sum={sum}, actual={actual_sum}'
        elif i % 4 == 2:
            await exec_compute_query(env, branch, 'UPDATE t SET i = i + 1')
            sum += cnt


# Add a test which creates compute for every query, and then destroys it right after.
def test_compute_restarts(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_compute_restarts')
    asyncio.run(run_compute_restarts(env))


class BackgroundCompute(object):
    def __init__(self, index: int, env: NeonEnv, branch: str):
        self.index = index
        self.env = env
        self.branch = branch
        self.running = False
        self.stopped = False
        self.total_tries = 0
        self.successful_queries: List[int] = []

    async def run(self):
        if self.running:
            raise Exception('BackgroundCompute is already running')

        self.running = True
        i = 0
        while not self.stopped:
            try:
                verify_key = (self.index << 16) + i
                i += 1
                self.total_tries += 1
                res = await exec_compute_query(
                    self.env,
                    self.branch,
                    f'INSERT INTO query_log(index, verify_key) VALUES ({self.index}, {verify_key}) RETURNING verify_key',
                    pgdir_name=f'bgcompute{self.index}_key{verify_key}',
                )
                log.info(f'result: {res}')
                if len(res) != 1:
                    raise Exception('No result returned')
                if res[0][0] != verify_key:
                    raise Exception('Wrong result returned')
                self.successful_queries.append(verify_key)
            except Exception as e:
                log.info(f'BackgroundCompute {self.index} query failed: {e}')

            # With less sleep, there is a very big chance of not committing
            # anything or only 1 xact during test run.
            await asyncio.sleep(2 * random.random())
        self.running = False


async def run_concurrent_computes(env: NeonEnv,
                                  num_computes=10,
                                  run_seconds=20,
                                  branch='test_concurrent_computes'):
    await exec_compute_query(
        env,
        branch,
        'CREATE TABLE query_log (t timestamp default now(), index int, verify_key int)')

    computes = [BackgroundCompute(i, env, branch) for i in range(num_computes)]
    background_tasks = [asyncio.create_task(compute.run()) for compute in computes]

    await asyncio.sleep(run_seconds)
    for compute in computes[1:]:
        compute.stopped = True
    log.info("stopped all tasks but one")

    # work for some time with only one compute -- it should be able to make some xacts
    await asyncio.sleep(8)
    computes[0].stopped = True

    await asyncio.gather(*background_tasks)

    result = await exec_compute_query(env, branch, 'SELECT * FROM query_log')
    # we should have inserted something while single compute was running
    assert len(result) >= 4
    log.info(f'Executed {len(result)} queries')
    for row in result:
        log.info(f'{row[0]} {row[1]} {row[2]}')

    # ensure everything reported as committed wasn't lost
    for compute in computes:
        for verify_key in compute.successful_queries:
            assert verify_key in [row[2] for row in result]


# Run multiple computes concurrently, creating-destroying them after single
# query. Ensure we don't lose any xacts reported as committed and be able to
# progress once only one compute remains.
def test_concurrent_computes(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_concurrent_computes')
    asyncio.run(run_concurrent_computes(env))
