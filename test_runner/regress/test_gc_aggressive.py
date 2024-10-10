from __future__ import annotations

import asyncio
import concurrent.futures
import random

from fixtures.common_types import TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.remote_storage import RemoteStorageKind

# Test configuration
#
# Create a table with {NUM_ROWS} rows, and perform {UPDATES_TO_PERFORM} random
# UPDATEs on it, using {NUM_CONNECTIONS} separate connections.
NUM_CONNECTIONS = 10
NUM_ROWS = 100000
UPDATES_TO_PERFORM = 10000


# At the same time, run UPDATEs and GC
async def update_and_gc(env: NeonEnv, endpoint: Endpoint, timeline: TimelineId):
    workers = []
    updates_performed = 0

    # Perform aggressive GC with 0 horizon
    async def gc(env: NeonEnv, timeline: TimelineId):
        pageserver_http = env.pageserver.http_client()
        nonlocal updates_performed
        global UPDATES_TO_PERFORM

        loop = asyncio.get_running_loop()

        def do_gc():
            pageserver_http.timeline_checkpoint(env.initial_tenant, timeline)
            pageserver_http.timeline_gc(env.initial_tenant, timeline, 0)

        with concurrent.futures.ThreadPoolExecutor() as pool:
            while updates_performed < UPDATES_TO_PERFORM:
                await loop.run_in_executor(pool, do_gc)

    # Run random UPDATEs on test table
    async def update_table(endpoint: Endpoint):
        pg_conn = await endpoint.connect_async()
        nonlocal updates_performed

        while updates_performed < UPDATES_TO_PERFORM:
            updates_performed += 1
            id = random.randrange(1, NUM_ROWS)
            await pg_conn.fetchrow(f"UPDATE foo SET counter = counter + 1 WHERE id = {id}")

    for _ in range(NUM_CONNECTIONS):
        workers.append(asyncio.create_task(update_table(endpoint)))
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
    env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "0 sec"})
    timeline = env.create_branch("test_gc_aggressive", ancestor_branch_name="main")
    endpoint = env.endpoints.create_start("test_gc_aggressive")

    with endpoint.cursor() as cur:
        # Create table, and insert the first 100 rows
        cur.execute("CREATE TABLE foo (id int, counter int, t text)")
        cur.execute(
            f"""
            INSERT INTO foo
                SELECT g, 0, 'long string to consume some space' || g
                FROM generate_series(1, {NUM_ROWS}) g
        """
        )
        cur.execute("CREATE INDEX ON foo(id)")

        asyncio.run(update_and_gc(env, endpoint, timeline))

        cur.execute("SELECT COUNT(*), SUM(counter) FROM foo")
        r = cur.fetchone()
        assert r is not None
        assert r == (NUM_ROWS, UPDATES_TO_PERFORM)


#
def test_gc_index_upload(neon_env_builder: NeonEnvBuilder):
    num_index_uploads = 0

    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    # Disable time-based pitr, we will use LSN-based thresholds in the manual GC calls
    env = neon_env_builder.init_start(initial_tenant_conf={"pitr_interval": "0 sec"})
    tenant_id = env.initial_tenant
    timeline_id = env.create_branch("test_gc_index_upload", ancestor_branch_name="main")
    endpoint = env.endpoints.create_start("test_gc_index_upload")

    pageserver_http = env.pageserver.http_client()

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE TABLE foo (id int, counter int, t text)")
    cur.execute(
        """
        INSERT INTO foo
        SELECT g, 0, 'long string to consume some space' || g
        FROM generate_series(1, 100000) g
        """
    )

    # Helper function that gets the number of given kind of remote ops from the metrics
    def get_num_remote_ops(file_kind: str, op_kind: str) -> int:
        ps_metrics = env.pageserver.http_client().get_metrics()
        total = 0.0
        for sample in ps_metrics.query_all(
            name="pageserver_remote_operation_seconds_count",
            filter={
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
        ):
            total += sample[2]
        return int(total)

    # Sanity check that the metric works
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    pageserver_http.timeline_gc(tenant_id, timeline_id, 10000)
    before = get_num_remote_ops("index", "upload")
    assert before > 0

    # Run many cycles of GC. Then check that the number of index files
    # uploads didn't grow much. In particular we don't want to re-upload the
    # index file on every GC iteration, when it has no work to do.
    #
    # On each iteration, we use a slightly smaller GC horizon, so that the GC
    # at least needs to check if it has work to do.
    for i in range(100):
        cur.execute("INSERT INTO foo VALUES (0, 0, 'foo')")
        pageserver_http.timeline_gc(tenant_id, timeline_id, 10000 - i * 32)
        num_index_uploads = get_num_remote_ops("index", "upload")

        # Also make sure that a no-op compaction doesn't upload the index
        # file unnecessarily.
        pageserver_http.timeline_compact(tenant_id, timeline_id)

        log.info(f"{num_index_uploads} index uploads after GC iteration {i}")

    after = num_index_uploads
    log.info(f"{after - before} new index uploads during test")
    assert after - before < 5
