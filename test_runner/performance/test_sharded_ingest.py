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
@pytest.mark.parametrize(
    "wal_receiver_protocol",
    [
        "vanilla",
        "interpreted-bincode-compressed",
        "interpreted-protobuf-compressed",
    ],
)
def test_sharded_ingest(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    shard_count: int,
    wal_receiver_protocol: str,
):
    """
    Benchmarks sharded ingestion throughput, by ingesting a large amount of WAL into a Safekeeper
    and fanning out to a large number of shards on dedicated Pageservers. Comparing the base case
    (shard_count=1) to the sharded case indicates the overhead of sharding.
    """
    ROW_COUNT = 100_000_000  # about 7 GB of WAL

    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_configs()

    for ps in env.pageservers:
        if wal_receiver_protocol == "vanilla":
            ps.patch_config_toml_nonrecursive(
                {
                    "wal_receiver_protocol": {
                        "type": "vanilla",
                    }
                }
            )
        elif wal_receiver_protocol == "interpreted-bincode-compressed":
            ps.patch_config_toml_nonrecursive(
                {
                    "wal_receiver_protocol": {
                        "type": "interpreted",
                        "args": {"format": "bincode", "compression": {"zstd": {"level": 1}}},
                    }
                }
            )
        elif wal_receiver_protocol == "interpreted-protobuf-compressed":
            ps.patch_config_toml_nonrecursive(
                {
                    "wal_receiver_protocol": {
                        "type": "interpreted",
                        "args": {"format": "protobuf", "compression": {"zstd": {"level": 1}}},
                    }
                }
            )
        else:
            raise AssertionError("Test must use explicit wal receiver protocol config")

    env.start()

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

    # Record metrics.
    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)

    total_ingested = 0
    total_records_received = 0
    ingested_by_ps = []
    for pageserver in env.pageservers:
        ingested = pageserver.http_client().get_metric_value(
            "pageserver_wal_ingest_bytes_received_total"
        )
        records_received = pageserver.http_client().get_metric_value(
            "pageserver_wal_ingest_records_received_total"
        )

        if ingested is None:
            ingested = 0

        if records_received is None:
            records_received = 0

        ingested_by_ps.append(
            (
                pageserver.id,
                {
                    "ingested": ingested,
                    "records_received": records_received,
                },
            )
        )

        total_ingested += int(ingested)
        total_records_received += int(records_received)

    total_ingested_mb = total_ingested / (1024 * 1024)
    zenbenchmark.record("wal_ingested", total_ingested_mb, "MB", MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record(
        "records_received", total_records_received, "records", MetricReport.LOWER_IS_BETTER
    )

    ingested_by_ps.sort(key=lambda x: x[0])
    for _, stats in ingested_by_ps:
        for k in stats:
            if k != "records_received":
                stats[k] /= 1024**2

    log.info(f"WAL ingested by each pageserver {ingested_by_ps}")

    assert tenant_get_shards(env, tenant_id) == shards, "shards moved"

    # The pageservers can take a long time to shut down gracefully, presumably due to the upload
    # queue or compactions or something. Just stop them immediately, we don't care.
    env.stop(immediate=True)
