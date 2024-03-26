import asyncio
from typing import Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    tenant_get_shards,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import wait_for_last_record_lsn, wait_for_upload
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import wait_until

TIMELINE_COUNT = 10
ENTRIES_PER_TIMELINE = 10_000
CHECKPOINT_TIMEOUT_SECONDS = 60

TENANT_CONF = {
    # Large `checkpoint_distance` effectively disables size
    # based checkpointing.
    "checkpoint_distance": f"{2 * 1024 ** 3}",
    "checkpoint_timeout": f"{CHECKPOINT_TIMEOUT_SECONDS}s",
}


async def run_worker(env: NeonEnv, entries: int) -> Tuple[TenantId, TimelineId, Lsn]:
    tenant, timeline = env.neon_cli.create_tenant(conf=TENANT_CONF)
    with env.endpoints.create_start("main", tenant_id=tenant) as ep:
        conn = await ep.connect_async()
        try:
            await conn.execute("CREATE TABLE IF NOT EXISTS t(key serial primary key, value text)")
            await conn.execute(
                f"INSERT INTO t SELECT i, CONCAT('payload_', i) FROM generate_series(0,{entries}) as i"
            )
        finally:
            await conn.close(timeout=10)

        last_flush_lsn = Lsn(ep.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
        return tenant, timeline, last_flush_lsn


async def workload(
    env: NeonEnv, timelines: int, entries: int
) -> list[Tuple[TenantId, TimelineId, Lsn]]:
    workers = [asyncio.create_task(run_worker(env, entries)) for _ in range(timelines)]
    return await asyncio.gather(*workers)


def wait_until_pageserver_is_caught_up(
    env: NeonEnv, last_flush_lsns: list[Tuple[TenantId, TimelineId, Lsn]]
):
    for tenant, timeline, last_flush_lsn in last_flush_lsns:
        shards = tenant_get_shards(env, tenant)
        for tenant_shard_id, pageserver in shards:
            waited = wait_for_last_record_lsn(
                pageserver.http_client(), tenant_shard_id, timeline, last_flush_lsn
            )
            assert waited >= last_flush_lsn


def wait_until_pageserver_has_uploaded(
    env: NeonEnv, last_flush_lsns: list[Tuple[TenantId, TimelineId, Lsn]]
):
    for tenant, timeline, last_flush_lsn in last_flush_lsns:
        shards = tenant_get_shards(env, tenant)
        for tenant_shard_id, pageserver in shards:
            wait_for_upload(pageserver.http_client(), tenant_shard_id, timeline, last_flush_lsn)


def wait_for_wal_ingest_metric(pageserver_http: PageserverHttpClient) -> float:
    def query():
        value = pageserver_http.get_metric_value("pageserver_wal_ingest_records_received_total")
        assert value is not None
        return value

    # The metric gets initialised on the first update.
    # Retry a few times, but return 0 if it's stable.
    try:
        return float(wait_until(3, 0.5, query))
    except Exception:
        return 0


@pytest.mark.parametrize("immediate_shutdown", [True, False])
def test_pageserver_small_inmemory_layers(
    neon_env_builder: NeonEnvBuilder, immediate_shutdown: bool
):
    """
    Test that open layers get flushed after the `checkpoint_timeout` config
    and do not require WAL reingest upon restart.

    The workload creates a number of timelines and writes some data to each,
    but not enough to trigger flushes via the `checkpoint_distance` config.
    """

    def get_dirty_bytes():
        v = (
            env.pageserver.http_client().get_metric_value("pageserver_timeline_ephemeral_bytes")
            or 0
        )
        log.info(f"dirty_bytes: {v}")
        return v

    def assert_dirty_bytes(v):
        assert get_dirty_bytes() == v

    env = neon_env_builder.init_configs()
    env.start()

    last_flush_lsns = asyncio.run(workload(env, TIMELINE_COUNT, ENTRIES_PER_TIMELINE))
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    # We didn't write enough data to trigger a size-based checkpoint
    assert get_dirty_bytes() > 0

    ps_http_client = env.pageserver.http_client()
    total_wal_ingested_before_restart = wait_for_wal_ingest_metric(ps_http_client)

    # Within ~ the checkpoint interval, all the ephemeral layers should be frozen and flushed,
    # such that there are zero bytes of ephemeral layer left on the pageserver
    log.info("Waiting for background checkpoints...")
    wait_until(CHECKPOINT_TIMEOUT_SECONDS * 2, 1, lambda: assert_dirty_bytes(0))  # type: ignore

    # Zero ephemeral layer bytes does not imply that all the frozen layers were uploaded: they
    # must be uploaded to remain visible to the pageserver after restart.
    wait_until_pageserver_has_uploaded(env, last_flush_lsns)

    env.pageserver.restart(immediate=immediate_shutdown)
    wait_until_pageserver_is_caught_up(env, last_flush_lsns)

    # Catching up with WAL ingest should have resulted in zero bytes of ephemeral layers, since
    # we froze, flushed and uploaded everything before restarting.  There can be no more WAL writes
    # because we shut down compute endpoints before flushing.
    assert get_dirty_bytes() == 0

    total_wal_ingested_after_restart = wait_for_wal_ingest_metric(ps_http_client)

    log.info(f"WAL ingested before restart: {total_wal_ingested_before_restart}")
    log.info(f"WAL ingested after restart: {total_wal_ingested_after_restart}")

    assert total_wal_ingested_after_restart == 0
