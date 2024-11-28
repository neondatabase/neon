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
from fixtures.pageserver.utils import wait_for_last_record_lsn
from fixtures.pg_version import PgVersion


@pytest.mark.timeout(600)
@pytest.mark.parametrize("size", [8, 64, 1024, 8192])
@pytest.mark.parametrize("backpressure", [True, False])
@pytest.mark.parametrize("fsync", [True, False])
def test_ingest_insert_bulk(
    request: pytest.FixtureRequest,
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
    fsync: bool,
    backpressure: bool,
    size: int,
):
    """
    Benchmarks ingestion of 8 GB of sequential insert WAL with concurrent inserts.
    """

    CONCURRENCY = 1  # 1 is optimal without fsync or backpressure
    VOLUME = 8 * 1024**3
    rows = VOLUME // (size + 64)  # +64 roughly accounts for per-row WAL overhead

    # Change Direct IO modes
    neon_env_builder.pageserver_virtual_file_io_mode = "direct"
    neon_env_builder.safekeepers_enable_fsync = fsync
    env = neon_env_builder.init_start()

    # NB: neon_local defaults to max_replication_write_lag=15MB, which is too low.
    # Production uses 500MB.
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            f"fsync = {fsync}",
            "max_replication_apply_lag = 0",
            f"max_replication_flush_lag = {'10GB' if backpressure else '0'}",
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

    with zenbenchmark.record_duration("ingest"):
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            for i in range(CONCURRENCY):
                # Write a random value for all rows. This is sufficient to prevent compression, e.g.
                # in TOAST. Randomly generating every row is too slow.
                value = random.randbytes(size)
                worker_rows = rows / CONCURRENCY
                pool.submit(insert_rows, endpoint, f"table{i}", worker_rows, value)

    end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])
    client = env.pageserver.http_client()
    wait_for_last_record_lsn(client, env.initial_tenant, env.initial_timeline, end_lsn)

    backpressure_time = endpoint.safe_psql("select backpressure_throttling_time()")[0][0]

    # Now that all data is ingested, delete and recreate the tenant in the pageserver. This will
    # reingest all the WAL directly from the safekeeper. This gives us a baseline of how fast the
    # pageserver can ingest this WAL in isolation.
    pg_version = PgVersion(
        str(client.timeline_detail(env.initial_tenant, env.initial_timeline)["pg_version"])
    )
    status = env.storage_controller.inspect(tenant_shard_id=env.initial_tenant)
    assert status is not None

    endpoint.stop()  # avoid spurious getpage errors
    client.tenant_delete(env.initial_tenant)
    env.pageserver.tenant_create(tenant_id=env.initial_tenant, generation=status[0])

    with zenbenchmark.record_duration("recover"):
        log.info("Recovering WAL into pageserver")
        client.timeline_create(pg_version, env.initial_tenant, env.initial_timeline)
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
    for name in ("ingest", "recover"):
        throughput = int(wal_written_mb / props[name])
        zenbenchmark.record(f"{name}_throughput", throughput, "MB/s", MetricReport.HIGHER_IS_BETTER)
