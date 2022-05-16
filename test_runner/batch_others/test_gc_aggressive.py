import asyncio
import random
import uuid

from fixtures.zenith_fixtures import ZenithEnv, Postgres, ZenithEnvBuilder
from fixtures.log_helper import log

# Test configuration
#
# Create a table with {num_rows} rows, and perform {updates_to_perform} random
# UPDATEs on it, using {num_connections} separate connections.
num_connections = 10
num_rows = 100000
updates_to_perform = 10000

updates_performed = 0


# Run random UPDATEs on test table
async def update_table(pg: Postgres):
    global updates_performed
    pg_conn = await pg.connect_async()

    while updates_performed < updates_to_perform:
        updates_performed += 1
        id = random.randrange(1, num_rows)
        row = await pg_conn.fetchrow(f'UPDATE foo SET counter = counter + 1 WHERE id = {id}')


async def select_table(env: ZenithEnv, pg_branch: Postgres):
    while updates_performed < updates_to_perform:
        pg_conn = await pg_branch.connect_async()
        rows = await pg_conn.fetchrow('SELECT count(*) FROM foo')
        assert rows == (num_rows, )


# Perform aggressive GC with 0 horizon
async def gc(env: ZenithEnv, tenant: uuid.UUID, timeline: str):
    psconn = await env.pageserver.connect_async()

    while updates_performed < updates_to_perform:
        await psconn.execute(f"do_gc {tenant.hex} {timeline} 0")


# At the same time, run UPDATEs and GC
async def update_and_gc(env: ZenithEnv, pg: Postgres, tenant: uuid.UUID, timeline: str):
    workers = []
    for worker_id in range(num_connections):
        workers.append(asyncio.create_task(update_table(pg)))
    workers.append(asyncio.create_task(gc(env, tenant, timeline)))

    # await all workers
    await asyncio.gather(*workers)


async def update_select_and_gc(env: ZenithEnv,
                               pg_main: Postgres,
                               pg_branch: Postgres,
                               tenant: uuid.UUID,
                               timeline: str):
    workers = []
    for worker_id in range(num_connections):
        workers.append(asyncio.create_task(update_table(pg_main)))
    for worker_id in range(num_connections):
        workers.append(asyncio.create_task(select_table(env, pg_branch)))
    workers.append(asyncio.create_task(gc(env, tenant, timeline)))

    # await all workers
    await asyncio.gather(*workers)


#
# Aggressively force GC, while running queries.
#
# (repro for https://github.com/zenithdb/zenith/issues/1047)
#
def test_gc_aggressive(zenith_simple_env: ZenithEnv):
    global updates_performed
    global num_connections
    global num_rows
    global updates_to_perform

    updates_to_perform = 10000
    num_connections = 10
    num_rows = 100000
    updates_performed = 0

    env = zenith_simple_env
    env.zenith_cli.create_branch("test_gc_aggressive", "empty")
    pg = env.postgres.create_start('test_gc_aggressive')
    log.info('postgres is running on test_gc_aggressive branch')

    conn = pg.connect()
    cur = conn.cursor()

    cur.execute("SHOW zenith.zenith_timeline")
    timeline = cur.fetchone()[0]

    # Create table, and insert the first 100 rows
    cur.execute('CREATE TABLE foo (id int, counter int, t text)')
    cur.execute(f'''
        INSERT INTO foo
            SELECT g, 0, 'long string to consume some space' || g
            FROM generate_series(1, {num_rows}) g
    ''')
    cur.execute('CREATE INDEX ON foo(id)')

    asyncio.run(update_and_gc(env, pg, env.initial_tenant, timeline))

    cur.execute('SELECT COUNT(*), SUM(counter) FROM foo')
    assert cur.fetchone() == (num_rows, updates_to_perform)


def test_branch_and_async_gc(zenith_env_builder: ZenithEnvBuilder):
    global updates_to_perform
    global num_connections
    global num_rows
    global updates_performed

    updates_to_perform = 500
    num_connections = 10
    num_rows = 100000
    updates_performed = 0

    # Use safekeeper in this test to avoid a subtle race condition.
    # Without safekeeper, walreceiver reconnection can stuck
    # because of IO deadlock.
    #
    # See https://github.com/zenithdb/zenith/issues/1068
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()

    # Override defaults, 1M gc_horizon and 4M checkpoint_distance.
    # Extend compaction_period and gc_period to disable background compaction and gc.
    tenant = env.zenith_cli.create_tenant(
        conf={
            'gc_period': '10 m',
            'gc_horizon': '4194304',
            'image_creation_threshold': '1',
            'checkpoint_distance': '8388608',
            'compaction_period': '10 m',
            'compaction_threshold': '2',
            'compaction_target_size': '4194304',
        })

    env.zenith_cli.create_timeline(f'test_main', tenant_id=tenant)
    pgmain = env.postgres.create_start('test_main', tenant_id=tenant)
    log.info("postgres is running on 'test_main' branch")

    main_cur = pgmain.connect().cursor()
    main_cur.execute("SHOW zenith.zenith_timeline")
    timeline = main_cur.fetchone()[0]

    # Create table, and insert 100 rows.
    main_cur.execute(
        'CREATE TABLE foo (id int, counter int, t text) WITH (autovacuum_enabled = off)')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    init_lsn = main_cur.fetchone()[0]
    main_cur.execute(f'''
        INSERT INTO foo
            SELECT g, 0, '00112233445566778899AABBCCDDEEFF' || ':main:' || g
            FROM generate_series(1, {num_rows}) g
    ''')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    lsn_rows = main_cur.fetchone()[0]
    log.info(f'LSN after {num_rows} rows: {lsn_rows}')

    # Create branch.
    env.zenith_cli.create_branch('test_branch',
                                 'test_main',
                                 tenant_id=tenant,
                                 ancestor_start_lsn=lsn_rows)
    pg_branch = env.postgres.create_start('test_branch', tenant_id=tenant)
    branch_cur = pg_branch.connect().cursor()

    asyncio.run(update_select_and_gc(env, pgmain, pg_branch, tenant, timeline))

    branch_cur.execute('SELECT count(*) FROM foo')
    assert branch_cur.fetchone() == (num_rows, )
