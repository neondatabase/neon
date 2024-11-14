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

    # Ingest data and measure duration.
    start_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            log.info("Ingesting data")
            cur.execute("set statement_timeout = 0")

            with zenbenchmark.record_duration("pageserver_ingest"):
                with zenbenchmark.record_duration("wal_ingest"):
                    cur.execute(f"""
                        select pg_logical_emit_message(false, '', repeat('x', {size}))
                        from generate_series(1, {count})
                    """)

                wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, env.initial_timeline)

    end_lsn = Lsn(endpoint.safe_psql("select pg_current_wal_lsn()")[0][0])

    wal_written_mb = round((end_lsn - start_lsn) / (1024 * 1024))
    zenbenchmark.record("wal_written", wal_written_mb, "MB", MetricReport.TEST_PARAM)
    zenbenchmark.record("message_count", count, "messages", MetricReport.TEST_PARAM)

    props = {p["name"]: p["value"] for _, p in request.node.user_properties}
    wal_throughput = int(wal_written_mb / props["wal_ingest"])
    pageserver_throughput = int(wal_written_mb / props["pageserver_ingest"])
    zenbenchmark.record("wal_throughput", wal_throughput, "MB/s", MetricReport.HIGHER_IS_BETTER)
    zenbenchmark.record(
        "pageserver_throughput", pageserver_throughput, "MB/s", MetricReport.HIGHER_IS_BETTER
    )
