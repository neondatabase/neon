import asyncio
import concurrent.futures
import random

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, Postgres
from fixtures.types import TimelineId
from fixtures.utils import query_scalar

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
        await pg_conn.fetchrow(f"UPDATE foo SET counter = counter + 1 WHERE id = {id}")


# Perform aggressive GC with 0 horizon
async def gc(env: NeonEnv, timeline: TimelineId):
    pageserver_http = env.pageserver.http_client()

    loop = asyncio.get_running_loop()

    def do_gc():
        pageserver_http.timeline_checkpoint(env.initial_tenant, timeline)
        pageserver_http.timeline_gc(env.initial_tenant, timeline, 0)

    with concurrent.futures.ThreadPoolExecutor() as pool:
        while updates_performed < updates_to_perform:
            await loop.run_in_executor(pool, do_gc)

# At the same time, run UPDATEs and GC
async def update_and_gc(env: NeonEnv, pg: Postgres, timeline: TimelineId):
    workers = []
    for worker_id in range(num_connections):
        workers.append(asyncio.create_task(update_table(pg)))
    workers.append(asyncio.create_task(gc(env, timeline)))

    # await all workers
    await asyncio.gather(*workers)


#
# Aggressively force GC, while running queries.
#
# (repro for https://github.com/neondatabase/neon/issues/1047)
#
def test_gc_aggressive(neon_env_builder: NeonEnvBuilder):

    # Disable pitr, because here we want to test branch creation after GC
    neon_env_builder.pageserver_config_override = "tenant_config={pitr_interval = '0 sec'}"
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_gc_aggressive", "main")
    pg = env.postgres.create_start("test_gc_aggressive")
    log.info("postgres is running on test_gc_aggressive branch")

    with pg.cursor() as cur:
        timeline = TimelineId(query_scalar(cur, "SHOW neon.timeline_id"))

        # Create table, and insert the first 100 rows
        cur.execute("CREATE TABLE foo (id int, counter int, t text)")
        cur.execute(
            f"""
            INSERT INTO foo
                SELECT g, 0, 'long string to consume some space' || g
                FROM generate_series(1, {num_rows}) g
        """
        )
        cur.execute("CREATE INDEX ON foo(id)")

        asyncio.run(update_and_gc(env, pg, timeline))

        cur.execute("SELECT COUNT(*), SUM(counter) FROM foo")
        r = cur.fetchone()
        assert r is not None
        assert r == (num_rows, updates_to_perform)
