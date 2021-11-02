import asyncio
import asyncpg
import random
import time

from fixtures.zenith_fixtures import ZenithEnvBuilder, Postgres, Safekeeper
from fixtures.log_helper import getLogger
from fixtures.utils import lsn_from_hex, lsn_to_hex
from typing import List

log = getLogger('root.wal_acceptor_async')
pytest_plugins = ("fixtures.zenith_fixtures")


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

        # TODO: Remove when https://github.com/zenithdb/zenith/issues/644 is fixed
        await self.conn.execute('ALTER TABLE bank_accs SET (autovacuum_enabled = false)')
        await self.conn.execute('ALTER TABLE bank_log SET (autovacuum_enabled = false)')

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
                       timeout=600):
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
async def run_restarts_under_load(pg: Postgres, acceptors: List[Safekeeper], n_workers=10):
    n_accounts = 100
    init_amount = 100000
    max_transfer = 100
    period_time = 10
    iterations = 6

    pg_conn = await pg.connect_async()
    tenant_id = await pg_conn.fetchval("show zenith.zenith_tenant")
    timeline_id = await pg_conn.fetchval("show zenith.zenith_timeline")

    bank = BankClient(pg_conn, n_accounts=n_accounts, init_amount=init_amount)
    # create tables and initial balances
    await bank.initdb()

    stats = WorkerStats(n_workers)
    workers = []
    for worker_id in range(n_workers):
        worker = run_random_worker(stats, pg, worker_id, bank.n_accounts, max_transfer)
        workers.append(asyncio.create_task(worker))

    for it in range(iterations):
        victim_idx = it % len(acceptors)
        victim = acceptors[victim_idx]
        victim.stop()

        flush_lsn = await pg_conn.fetchval('SELECT pg_current_wal_flush_lsn()')
        flush_lsn = lsn_to_hex(flush_lsn)
        log.info(f'Postgres flush_lsn {flush_lsn}')

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


# restart acceptors one by one, while executing and validating bank transactions
def test_restarts_under_load(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    env.zenith_cli(["branch", "test_wal_acceptors_restarts_under_load", "main"])
    pg = env.postgres.create_start('test_wal_acceptors_restarts_under_load')

    asyncio.run(run_restarts_under_load(pg, env.safekeepers))

    # TODO: Remove when https://github.com/zenithdb/zenith/issues/644 is fixed
    pg.stop()
