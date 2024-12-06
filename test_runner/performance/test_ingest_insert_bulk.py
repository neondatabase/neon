from __future__ import annotations

import random
from concurrent.futures import ThreadPoolExecutor

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_for_upload_queue_empty,
)
from fixtures.remote_storage import s3_storage


@pytest.mark.timeout(900)
@pytest.mark.parametrize("size", [8, 1024, 8192])
@pytest.mark.parametrize("s3", [True, False], ids=["s3", "local"])
@pytest.mark.parametrize("backpressure", [True, False], ids=["backpressure", "nobackpressure"])
@pytest.mark.parametrize("fsync", [True, False], ids=["fsync", "nofsync"])
def test_ingest_insert_bulk(
    request: pytest.FixtureRequest,
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    fsync: bool,
    backpressure: bool,
    s3: bool,
    size: int,
):
    """
    Benchmarks ingestion of 5 GB of sequential insert WAL. Measures ingestion and S3 upload
    separately. Also does a Safekeeper→Pageserver re-ingestion to measure Pageserver ingestion in
    isolation.
    """

    CONCURRENCY = 1  # 1 is optimal without fsync or backpressure
    VOLUME = 5 * 1024**3
    rows = VOLUME // (size + 64)  # +64 roughly accounts for per-row WAL overhead

    neon_env_builder.safekeepers_enable_fsync = fsync

    if s3:
        neon_env_builder.enable_pageserver_remote_storage(s3_storage())
        # NB: don't use S3 for Safekeeper. It doesn't affect throughput (no backpressure), but it
        # would compete with Pageserver for bandwidth.
        # neon_env_builder.enable_safekeeper_remote_storage(s3_storage())

    neon_env_builder.disable_scrub_on_exit()  # immediate shutdown may leave stray layers
    env = neon_env_builder.init_start()

    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            f"fsync = {fsync}",
            "max_replication_apply_lag = 0",
            f"max_replication_flush_lag = {'10GB' if backpressure else '0'}",
            # NB: neon_local defaults to 15MB, which is too slow -- production uses 500MB.
            f"max_replication_write_lag = {'500MB' if backpressure else '0'}",
        ],
    )
    endpoint.safe_psql("create extension neon")

    # Wait for the timeline to be propagated to the pageserver.
    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    # Ingest rows.
    log.info("Ingesting data")
    start_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    def insert_rows(endpoint, table, count, value):
        with endpoint.connect().cursor() as cur:
            cur.execute("set statement_timeout = 0")
            cur.execute(f"create table {table} (id int, data bytea)")
            cur.execute(f"insert into {table} values (generate_series(1, {count}), %s)", (value,))

    with zenbenchmark.record_duration("upload"):
        with zenbenchmark.record_duration("ingest"):
            with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
                for i in range(CONCURRENCY):
                    # Write a random value for all rows. This is sufficient to prevent compression,
                    # e.g. in TOAST. Randomly generating every row is too slow.
                    value = random.randbytes(size)
                    worker_rows = rows / CONCURRENCY
                    pool.submit(insert_rows, endpoint, f"table{i}", worker_rows, value)

        end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

        # Wait for pageserver to ingest the WAL.
        client = env.pageserver.http_client()
        wait_for_last_record_lsn(client, env.initial_tenant, env.initial_timeline, end_lsn)

        # Wait for pageserver S3 upload. Checkpoint to flush the last in-memory layer.
        client.timeline_checkpoint(
            env.initial_tenant,
            env.initial_timeline,
            compact=False,
            wait_until_flushed=False,
        )
        wait_for_upload(client, env.initial_tenant, env.initial_timeline, end_lsn, timeout=600)

    # Empty out upload queue for next benchmark.
    wait_for_upload_queue_empty(client, env.initial_tenant, env.initial_timeline)

    backpressure_time = endpoint.safe_psql("select backpressure_throttling_time()")[0][0]

    # Now that all data is ingested, delete and recreate the tenant in the pageserver. This will
    # reingest all the WAL directly from the safekeeper. This gives us a baseline of how fast the
    # pageserver can ingest this WAL in isolation.
    status = env.storage_controller.inspect(tenant_shard_id=env.initial_tenant)
    assert status is not None

    endpoint.stop()  # avoid spurious getpage errors
    client.tenant_delete(env.initial_tenant)
    env.pageserver.tenant_create(tenant_id=env.initial_tenant, generation=status[0])

    with zenbenchmark.record_duration("recover"):
        log.info("Recovering WAL into pageserver")
        client.timeline_create(env.pg_version, env.initial_tenant, env.initial_timeline)
        wait_for_last_record_lsn(client, env.initial_tenant, env.initial_timeline, end_lsn)

    # Emit metrics.
    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)
    zenbenchmark.record("row_count", rows, "rows", MetricReport.TEST_PARAM)
    zenbenchmark.record("concurrency", CONCURRENCY, "clients", MetricReport.TEST_PARAM)
    zenbenchmark.record(
        "backpressure_time", backpressure_time // 1000, "ms", MetricReport.LOWER_IS_BETTER
    )

    props = {p["name"]: p["value"] for _, p in request.node.user_properties}
    for name in ("ingest", "upload", "recover"):
        throughput = int(wal_written_mb / props[name])
        zenbenchmark.record(f"{name}_throughput", throughput, "MB/s", MetricReport.HIGHER_IS_BETTER)

    # Pageserver shutdown will likely get stuck on the upload queue, just shut it down immediately.
    env.stop(immediate=True)
