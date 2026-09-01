"""
Reproduction/regression test for a shard-misrouting bug in
prefetch_register_bufferv() (pgxn/neon/communicator.c).

Bug description
---------------
In prefetch_register_bufferv(), when registering a multi-block batch, the
shard number of every block was computed from the *base* block's tag
(get_shard_number(&tag)) instead of the per-block tag (hashkey.buftag, whose
blockNum has already been advanced by `i`). Since get_shard_number() hashes
blockNum / stripe_size, whenever a multi-block request batch crosses a shard
stripe boundary, the blocks beyond the boundary get the shard number of the
base block and are sent to the wrong pageserver shard connection.

Trigger path (PG 17 read-stream API):

    sequential scan
      -> read_stream_begin_relation()            [access/heap/heapam.c]
      -> StartReadBuffers() / WaitReadBuffers()  [storage/buffer/bufmgr.c]
      -> smgrreadv(nblocks > 1)
      -> neon_readv()                            [pgxn/neon/pagestore_smgr.c]
      -> communicator_read_at_lsnv()             [pgxn/neon/communicator.c]
      -> prefetch_register_bufferv(..., is_prefetch=false)   <- buggy code
      -> prefetch_do_request() -> page_server->send(wrong_shard_no)

Observable symptoms
-------------------
1. The misrouted request reaches a pageserver that does not host the tenant
   shard owning the page. The pageserver counts it in
   `pageserver_misrouted_pagestream_requests_total` and drops the connection
   WITHOUT sending a response (PageStreamError::Reconnect,
   "getpage@lsn request routed to wrong shard" in
   pageserver/src/page_service.rs).
2. The compute detects the disconnect, discards all in-flight prefetches
   (getpage_prefetch_discards_total), and retries the read one block at a
   time. The single-block retry computes the shard from the correct block
   number, so it self-heals: the query SUCCEEDS despite the misrouting.

The regression test asserts the query returns the right result and that
the pageserver misroute counter stays at 0; with the buggy code (verified
manually before the fix) both the misroute counter and the compute
prefetch-discard counter grow.

Note on stripe_size: it is deliberately chosen to be a prime (71), not a
multiple of the read batch size (up to io_combine_limit = 16 blocks), so a
contiguous multi-block read is guaranteed to cross a stripe boundary inside
a single batch. An aligned stripe size (e.g. 64 with 16-block batches) never
crosses inside a batch and does not trigger the bug - see the control test.
"""

from __future__ import annotations

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.pg_version import PgVersion
from fixtures.utils import run_only_on_postgres

MISROUTED_METRIC = "pageserver_misrouted_pagestream_requests_total"
DISCARD_METRIC = "getpage_prefetch_discards_total"

SHARD_COUNT = 2
# Number of pages; large enough to span many stripes so that many multi-block
# batches cross stripe boundaries (~28 boundaries with stripe_size=71).
TABLE_PAGES_TARGET = 2000


def setup_sharded_env(neon_env_builder: NeonEnvBuilder, stripe_size: int):
    neon_env_builder.num_pageservers = SHARD_COUNT

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=SHARD_COUNT,
        initial_tenant_shard_stripe_size=stripe_size,
    )

    # The reproduction only manifests when the two tenant shards live on
    # DIFFERENT pageserver nodes. If both shards are colocated on one node,
    # ShardSelector::Page(key) silently picks the correct local shard and
    # hides the misrouting.
    shards = env.storage_controller.locate(env.initial_tenant)
    node_ids = {int(shard["node_id"]) for shard in shards}
    assert len(shards) == SHARD_COUNT
    assert len(node_ids) == SHARD_COUNT, f"shards must be on different nodes: {shards}"

    return env


def load_table(endpoint, create_index: bool = False) -> int:
    """Fill a table with enough pages to cross many stripe boundaries."""
    cur = endpoint.connect().cursor()
    cur.execute("CREATE TABLE t (id int, filler text)")
    # ~90 rows/page => enough rows for ~TABLE_PAGES_TARGET pages
    n_rows = TABLE_PAGES_TARGET * 90
    cur.execute(
        "INSERT INTO t SELECT g, repeat('x', 64) FROM generate_series(1, %s) g",
        (n_rows,),
    )
    if create_index:
        cur.execute("CREATE INDEX t_id_idx ON t (id)")
    cur.execute("CHECKPOINT")
    cur.execute("SELECT pg_relation_size('t') / 8192")
    npages = cur.fetchone()[0]
    log.info(f"table t has {npages} pages ({n_rows} rows)")
    assert npages > 4 * 71, "table must span several stripes"
    cur.close()
    return n_rows


def read_table_cold(env, n_rows: int, extra_settings: list[str] | None = None,
                    query: str = "SELECT count(*) FROM t"):
    """
    Read the table back on a FRESH endpoint so that shared buffers and the
    local file cache are cold and every read goes to the pageserver via
    neon_readv() (multi-block vectorized reads).
    """
    # NOTE: the test framework sets shared_buffers=1MB by default, which
    # limits the read stream's max_pinned_buffers to 1 (LimitAdditionalPins:
    # NBuffers/MaxBackends - REFCOUNT_ARRAY_ENTRIES) and would prevent any
    # multi-block batches. 128MB lets the stream build io_combine_limit (16
    # blocks) sized batches. A serial scan keeps the block pattern simple.
    endpoint = env.endpoints.create_start(
        "main",
        tenant_id=env.initial_tenant,
        config_lines=[
            "shared_buffers=128MB",
            "max_parallel_workers_per_gather=0",
        ],
    )
    cur = endpoint.connect().cursor()

    for setting in extra_settings or []:
        cur.execute(f"SET {setting}")
    cur.execute(f"EXPLAIN (COSTS FALSE) {query}")
    plan_lines = [row[0] for row in cur.fetchall()]
    for line in plan_lines:
        log.info(f"plan: {line}")
    cur.execute(query)
    (count,) = cur.fetchone()
    assert count == n_rows, f"expected {n_rows} rows, got {count}"

    endpoint.safe_psql("CREATE EXTENSION IF NOT EXISTS neon")
    cur.execute("SELECT metric, value FROM neon_perf_counters ORDER BY metric")
    for metric, value in cur.fetchall():
        if float(value) > 0:
            log.info(f"counter: {metric} = {value}")
    cur.execute(
        f"SELECT coalesce(sum(value), 0) FROM neon_perf_counters WHERE metric = '{DISCARD_METRIC}'"
    )
    discards = cur.fetchone()[0]
    cur.close()
    return endpoint, float(discards), plan_lines


def get_misroutes(env) -> float:
    total = 0.0
    for ps in env.pageservers:
        value = ps.http_client().get_metric_value(MISROUTED_METRIC)
        total += float(value) if value is not None else 0.0
    return total


@run_only_on_postgres([PgVersion.V17], "the read-stream based multi-block read path requires PostgreSQL 17")
def test_prefetch_register_bufferv_wrong_shard(neon_env_builder: NeonEnvBuilder):
    """
    Regression test: stripe_size (71) is not aligned with the read batch
    size (<= 16 blocks), so contiguous multi-block reads cross stripe
    boundaries inside a single batch. With the bug present (shard number
    computed from the batch's base tag), the blocks beyond each boundary
    are routed to the wrong shard: both pageserver misroute counter and
    compute prefetch-discard counter grow (verified manually on unfixed
    code). With the fix, both stay at 0 and every batch is routed to the
    correct shard on the first attempt.
    """
    env = setup_sharded_env(neon_env_builder, stripe_size=71)

    # Populate the table on one endpoint (writes only; no reads needed).
    endpoint = env.endpoints.create_start("main", tenant_id=env.initial_tenant)
    n_rows = load_table(endpoint)
    endpoint.stop()

    # Cold read on a fresh endpoint: triggers multi-block neon reads that
    # cross stripe boundaries.
    _, discards, _ = read_table_cold(env, n_rows)

    misroutes = get_misroutes(env)
    log.info(f"observed: misrouted pagestream requests = {misroutes}, "
             f"compute prefetch discards = {discards}")

    assert misroutes == 0, (
        "multi-block reads crossing a stripe boundary were misrouted to the "
        "wrong pageserver shard; the shard number must be computed from the "
        "per-block tag, not the batch's base tag"
    )


@run_only_on_postgres([PgVersion.V17], "the read-stream based multi-block read path requires PostgreSQL 17")
def test_prefetch_register_bufferv_single_block_control(neon_env_builder: NeonEnvBuilder):
    """
    Control case: identical setup (stripe_size=71), but io_combine_limit=1
    caps every read-stream batch at a single block. Single-block registration
    always computes the shard from the correct block number, so no misrouting
    is possible even with the buggy code. This proves the misrouting in the
    main test is specifically caused by multi-block batches crossing a stripe
    boundary, not by some unrelated mechanism.

    This test must pass both before and after the fix.
    """
    env = setup_sharded_env(neon_env_builder, stripe_size=71)

    endpoint = env.endpoints.create_start("main", tenant_id=env.initial_tenant)
    n_rows = load_table(endpoint)
    endpoint.stop()

    _, discards, _ = read_table_cold(env, n_rows, extra_settings=["io_combine_limit=1"])

    misroutes = get_misroutes(env)
    log.info(f"observed (control): misrouted pagestream requests = {misroutes}, "
             f"compute prefetch discards = {discards}")

    assert misroutes == 0, "single-block reads must not cause misrouted requests"
