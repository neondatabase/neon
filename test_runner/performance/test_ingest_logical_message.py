from __future__ import annotations

from contextlib import closing

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    wait_for_last_flush_lsn,
)


@pytest.mark.timeout(600)
@pytest.mark.parametrize("size", [1024, 8192, 131072])
@pytest.mark.parametrize("fsync", [True, False])
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
    endpoint = env.endpoints.create_start("main")

    # Ingest data and measure durations.
    start_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")

            # Postgres will return once the logical messages have been written to its local WAL,
            # without waiting for Safekeeper commit. We measure ingestion time both for Postgres,
            # Safekeeper, and Pageserver to detect bottlenecks.
            with zenbenchmark.record_duration("pageserver_ingest"):
                with zenbenchmark.record_duration("safekeeper_ingest"):
                    with zenbenchmark.record_duration("postgres_ingest"):
                        log.info("Ingesting data")
                        cur.execute(f"""
                            select pg_logical_emit_message(false, '', repeat('x', {size}))
                            from generate_series(1, {count})
                        """)

                    # Send a transactional message to synchronize with the Safekeeper.
                    log.info("Waiting for Safekeeper to catch up")
                    cur.execute("select pg_logical_emit_message(true, 'sync', '')")

                # Wait for Pageserver ingestion.
                log.info("Waiting for Pageserver to catch up")
                wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    # Emit metrics.
    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)
    zenbenchmark.record("message_count", count, "messages", MetricReport.TEST_PARAM)

    props = {p["name"]: p["value"] for _, p in request.node.user_properties}
    for name in ("postgres", "safekeeper", "pageserver"):
        throughput = int(wal_written_mb / props[f"{name}_ingest"])
        zenbenchmark.record(f"{name}_throughput", throughput, "MB/s", MetricReport.HIGHER_IS_BETTER)
