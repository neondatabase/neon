from __future__ import annotations

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_commit_lsn,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import wait_for_last_record_lsn


@pytest.mark.timeout(600)
@pytest.mark.parametrize("size", [1024, 8192, 131072])
@pytest.mark.parametrize("fsync", [True, False], ids=["fsync", "nofsync"])
def test_ingest_logical_message(
    request: pytest.FixtureRequest,
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    fsync: bool,
    size: int,
):
    """
    Benchmarks ingestion of 10 GB of logical message WAL. These are essentially noops, and don't
    incur any pageserver writes.
    """

    VOLUME = 10 * 1024**3
    count = VOLUME // size

    neon_env_builder.safekeepers_enable_fsync = fsync

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            f"fsync = {fsync}",
            # Disable backpressure. We don't want to block on pageserver.
            "max_replication_apply_lag = 0",
            "max_replication_flush_lag = 0",
            "max_replication_write_lag = 0",
        ],
    )
    client = env.pageserver.http_client()

    # Wait for the timeline to be propagated to the pageserver.
    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    # Ingest data and measure durations.
    start_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    with endpoint.cursor() as cur:
        cur.execute("set statement_timeout = 0")

        # Postgres will return once the logical messages have been written to its local WAL, without
        # waiting for Safekeeper commit. We measure ingestion time both for Postgres, Safekeeper,
        # and Pageserver to detect bottlenecks.
        log.info("Ingesting data")
        with zenbenchmark.record_duration("pageserver_ingest"):
            with zenbenchmark.record_duration("safekeeper_ingest"):
                with zenbenchmark.record_duration("postgres_ingest"):
                    cur.execute(f"""
                        select pg_logical_emit_message(false, '', repeat('x', {size}))
                        from generate_series(1, {count})
                    """)

                    end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

                # Wait for Safekeeper.
                log.info("Waiting for Safekeeper to catch up")
                wait_for_commit_lsn(env, env.initial_tenant, env.initial_timeline, end_lsn)

            # Wait for Pageserver.
            log.info("Waiting for Pageserver to catch up")
            wait_for_last_record_lsn(client, env.initial_tenant, env.initial_timeline, end_lsn)

    # Now that all data is ingested, delete and recreate the tenant in the pageserver. This will
    # reingest all the WAL from the safekeeper without any other constraints. This gives us a
    # baseline of how fast the pageserver can ingest this WAL in isolation.
    status = env.storage_controller.inspect(tenant_shard_id=env.initial_tenant)
    assert status is not None

    client.tenant_delete(env.initial_tenant)
    env.pageserver.tenant_create(tenant_id=env.initial_tenant, generation=status[0])

    with zenbenchmark.record_duration("pageserver_recover_ingest"):
        log.info("Recovering WAL into pageserver")
        client.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline)
        wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    # Emit metrics.
    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)
    zenbenchmark.record("message_count", count, "messages", MetricReport.TEST_PARAM)

    props = {p["name"]: p["value"] for _, p in request.node.user_properties}
    for name in ("postgres", "safekeeper", "pageserver", "pageserver_recover"):
        throughput = int(wal_written_mb / props[f"{name}_ingest"])
        zenbenchmark.record(f"{name}_throughput", throughput, "MB/s", MetricReport.HIGHER_IS_BETTER)
