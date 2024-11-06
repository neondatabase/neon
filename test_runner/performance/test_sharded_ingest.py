from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn, TenantShardId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    tenant_get_shards,
    wait_for_last_flush_lsn,
)


@pytest.mark.timeout(600)
@pytest.mark.parametrize("shard_count", [1, 8, 32])
def test_sharded_ingest(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    shard_count: int,
):
    """
    Benchmarks sharded ingestion throughput, by ingesting a large amount of WAL into a Safekeeper
    and fanning out to a large number of shards on dedicated Pageservers. Comparing the base case
    (shard_count=1) to the sharded case indicates the overhead of sharding.
    """

    ROW_COUNT = 100_000_000  # about 7 GB of WAL

    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start()

    # Create a sharded tenant and timeline, and migrate it to the respective pageservers. Ensure
    # the storage controller doesn't mess with shard placements.
    #
    # TODO: there should be a way to disable storage controller background reconciliations.
    # Currently, disabling reconciliation also disables foreground operations.
    tenant_id, timeline_id = env.create_tenant(shard_count=shard_count)

    for shard_number in range(0, shard_count):
        tenant_shard_id = TenantShardId(tenant_id, shard_number, shard_count)
        pageserver_id = shard_number + 1
        env.storage_controller.tenant_shard_migrate(tenant_shard_id, pageserver_id)

    shards = tenant_get_shards(env, tenant_id)
    env.storage_controller.reconcile_until_idle()
    assert tenant_get_shards(env, tenant_id) == shards, "shards moved"

    # Start the endpoint.
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    start_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    # Ingest data and measure WAL volume and duration.
    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            log.info("Ingesting data")
            cur.execute("set statement_timeout = 0")
            cur.execute("create table huge (i int, j int)")

            with zenbenchmark.record_duration("pageserver_ingest"):
                with zenbenchmark.record_duration("wal_ingest"):
                    cur.execute(f"insert into huge values (generate_series(1, {ROW_COUNT}), 0)")

                wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)

    end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])
    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)

    assert tenant_get_shards(env, tenant_id) == shards, "shards moved"
