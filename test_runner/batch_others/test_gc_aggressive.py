from contextlib import closing

import asyncio
import asyncpg
import random

from fixtures.zenith_fixtures import ZenithEnv, Postgres, Safekeeper
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


# Perform aggressive GC with 0 horizon
async def gc(env: ZenithEnv, timeline: str):
    psconn = await env.pageserver.connect_async()

    while updates_performed < updates_to_perform:
        await psconn.execute(f"do_gc {env.initial_tenant} {timeline} 0")


# At the same time, run UPDATEs and GC
async def update_and_gc(env: ZenithEnv, pg: Postgres, timeline: str):
    workers = []
    for worker_id in range(num_connections):
        workers.append(asyncio.create_task(update_table(pg)))
    workers.append(asyncio.create_task(gc(env, timeline)))

    # await all workers
    await asyncio.gather(*workers)


#
# Aggressively force GC, while running queries.
#
# (repro for https://github.com/zenithdb/zenith/issues/1047)
#
def test_gc_aggressive(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_gc_aggressive", "empty"])

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

    asyncio.run(update_and_gc(env, pg, timeline))

    row = cur.execute('SELECT COUNT(*), SUM(counter) FROM foo')
    assert cur.fetchone() == (num_rows, updates_to_perform)
