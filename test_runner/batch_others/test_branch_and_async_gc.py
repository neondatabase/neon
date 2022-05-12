import subprocess
import asyncio
import uuid
import random
import time
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import Postgres, ZenithEnv, ZenithEnvBuilder

updates_to_perform = 500
updates_performed = 0
num_connections = 10
num_rows = 100000


# Run random UPDATEs on test table
async def update_table(pg: Postgres):
    global updates_performed
    pg_conn = await pg.connect_async()

    while updates_performed < updates_to_perform:
        updates_performed += 1
        id = random.randrange(1, num_rows)
        row = await pg_conn.fetchrow(f'UPDATE foo SET t=t WHERE id = {id}')
        if id % 10 == 0:
            log.info(f'''updates {updates_performed}, id {id}''')


# Perform aggressive GC with 0 horizon
async def gc(env: ZenithEnv, tenant: uuid.UUID, timeline: str):
    psconn = await env.pageserver.connect_async()

    while updates_performed < updates_to_perform:
        log.info(f'''run gc''')
        await psconn.execute(f"do_gc {tenant.hex} {timeline} 0")
        log.info(f'''run gc done''')
        await asyncio.sleep(10)


async def select_table(env: ZenithEnv, pg_branch: Postgres):
    while updates_performed < updates_to_perform:
        pg_conn = await pg_branch.connect_async()
        rows = await pg_conn.fetchrow('SELECT count(*) FROM foo')
        log.info(f'''found {rows}''')
        assert rows == (num_rows, )


# At the same time, run UPDATEs and GC
async def update_and_gc(env: ZenithEnv,
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
# Create a branches off the main branch and run gc.
#
def test_branch_and_async_gc(zenith_env_builder: ZenithEnvBuilder):

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
    main_cur.execute('CREATE TABLE foo (id int, t text) WITH (autovacuum_enabled = off)')
    main_cur.execute('SELECT pg_current_wal_insert_lsn()')
    init_lsn = main_cur.fetchone()[0]
    main_cur.execute(f'''
        INSERT INTO foo
            SELECT g, '00112233445566778899AABBCCDDEEFF' || ':main:' || g
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

    asyncio.run(update_and_gc(env, pgmain, pg_branch, tenant, timeline))

    branch_cur.execute('SELECT count(*) FROM foo')
    assert branch_cur.fetchone() == (num_rows, )
