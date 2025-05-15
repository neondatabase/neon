from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder, PgBin


# Just start and measure duration.
#
# This test runs pretty quickly and can be informative when used in combination
# with emulated network delay. Some useful delay commands:
#
# 1. Add 2msec delay to all localhost traffic
# `sudo tc qdisc add dev lo root handle 1:0 netem delay 2msec`
#
# 2. Test that it works (you should see 4ms ping)
# `ping localhost`
#
# 3. Revert back to normal
# `sudo tc qdisc del dev lo root netem`
#
# NOTE this test might not represent the real startup time because the basebackup
#      for a large database might be larger if there's a lof of transaction metadata,
#      or safekeepers might need more syncing, or there might be more operations to
#      apply during config step, like more users, databases, or extensions. By default
#      we load extensions 'neon,pg_stat_statements,timescaledb,pg_cron', but in this
#      test we only load neon.
def test_compute_startup_simple(
    neon_env_builder: NeonEnvBuilder,
    zenbenchmark: NeonBenchmarker,
):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    env.create_branch("test_startup")

    endpoint = None

    # We do two iterations so we can see if the second startup is faster. It should
    # be because the compute node should already be configured with roles, databases,
    # extensions, etc from the first run.
    for i in range(2):
        # Start
        with zenbenchmark.record_duration(f"{i}_start_and_select"):
            if endpoint:
                endpoint.start()
            else:
                endpoint = env.endpoints.create(
                    "test_startup",
                    # Shared buffers need to be allocated during startup, so they
                    # impact startup time. This is the default value we use for
                    # 1CPU pods (maybe different for VMs).
                    #
                    # TODO extensions also contribute to shared memory allocation,
                    #      and this test doesn't include all default extensions we
                    #      load.
                    config_lines=["shared_buffers=262144"],
                )
                # Do not skip pg_catalog updates at first start, i.e.
                # imitate 'the first start after project creation'.
                endpoint.respec(skip_pg_catalog_updates=False)
                endpoint.start()
            endpoint.safe_psql("select 1;")

        # Get metrics
        metrics = endpoint.http_client().metrics_json()
        durations = {
            "wait_for_spec_ms": f"{i}_wait_for_spec",
            "sync_safekeepers_ms": f"{i}_sync_safekeepers",
            "sync_sk_check_ms": f"{i}_sync_sk_check",
            "basebackup_ms": f"{i}_basebackup",
            "start_postgres_ms": f"{i}_start_postgres",
            "config_ms": f"{i}_config",
            "total_startup_ms": f"{i}_total_startup",
        }
        for key, name in durations.items():
            value = metrics[key]
            zenbenchmark.record(name, value, "ms", report=MetricReport.LOWER_IS_BETTER)

        # Check basebackup size makes sense
        basebackup_bytes = metrics["basebackup_bytes"]
        if i > 0:
            assert basebackup_bytes < 100 * 1024

        # Stop so we can restart
        endpoint.stop()

        # Imitate optimizations that console would do for the second start
        endpoint.respec(skip_pg_catalog_updates=True)


# Start and measure duration with huge SLRU segments.
# This test is similar to test_compute_startup_simple, but it creates huge number of transactions
# and records containing this XIDs. Autovacuum is disable for the table to prevent CLOG truncation.
# TODO: this is very suspicious test, I doubt that it does what it's supposed to do,
# e.g. these two starts do not make much sense. Looks like it's just copy-paste.
# To be fixed within https://github.com/neondatabase/cloud/issues/8673
@pytest.mark.timeout(1800)
@pytest.mark.parametrize("slru", ["lazy", "eager"])
def test_compute_ondemand_slru_startup(
    slru: str, neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker
):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    lazy_slru_download = "true" if slru == "lazy" else "false"
    tenant, _ = env.create_tenant(
        conf={
            "lazy_slru_download": lazy_slru_download,
        }
    )

    endpoint = env.endpoints.create_start("main", tenant_id=tenant)
    with endpoint.cursor() as cur:
        cur.execute("CREATE TABLE t (pk integer PRIMARY KEY, x integer)")
        cur.execute("ALTER TABLE t SET (autovacuum_enabled = false)")
        cur.execute("INSERT INTO t VALUES (1, 0)")
        cur.execute(
            """
            CREATE PROCEDURE updating() as
            $$
                DECLARE
                i integer;
                BEGIN
                FOR i IN 1..1000000 LOOP
                    UPDATE t SET x = x + 1 WHERE pk=1;
                    COMMIT;
                END LOOP;
                END
            $$ LANGUAGE plpgsql
            """
        )
        cur.execute("SET statement_timeout=0")
        cur.execute("call updating()")

    endpoint.stop()

    # We do two iterations so we can see if the second startup is faster. It should
    # be because the compute node should already be configured with roles, databases,
    # extensions, etc from the first run.
    for i in range(2):
        # Start
        with zenbenchmark.record_duration(f"{slru}_{i}_start"):
            endpoint.start()

        with zenbenchmark.record_duration(f"{slru}_{i}_select"):
            sum = endpoint.safe_psql("select sum(x) from t")[0][0]
            assert sum == 1000000

        # Get metrics
        metrics = endpoint.http_client().metrics_json()
        durations = {
            "wait_for_spec_ms": f"{slru}_{i}_wait_for_spec",
            "sync_safekeepers_ms": f"{slru}_{i}_sync_safekeepers",
            "sync_sk_check_ms": f"{slru}_{i}_sync_sk_check",
            "basebackup_ms": f"{slru}_{i}_basebackup",
            "start_postgres_ms": f"{slru}_{i}_start_postgres",
            "config_ms": f"{slru}_{i}_config",
            "total_startup_ms": f"{slru}_{i}_total_startup",
        }
        for key, name in durations.items():
            value = metrics[key]
            zenbenchmark.record(name, value, "ms", report=MetricReport.LOWER_IS_BETTER)

        basebackup_bytes = metrics["basebackup_bytes"]
        zenbenchmark.record(
            f"{slru}_{i}_basebackup_bytes",
            basebackup_bytes,
            "bytes",
            report=MetricReport.LOWER_IS_BETTER,
        )

        # Stop so we can restart
        endpoint.stop()

        # Imitate optimizations that console would do for the second start
        endpoint.respec(skip_pg_catalog_updates=True)


@pytest.mark.timeout(240)
def test_compute_startup_latency(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    zenbenchmark: NeonBenchmarker,
):
    """
    Do NUM_STARTS 'optimized' starts, i.e. with pg_catalog updates skipped,
    and measure the duration of each step. Report p50, p90, p99 latencies.
    """
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    endpoint = env.endpoints.create_start("main")
    pg_bin.run_capture(["pgbench", "-i", "-I", "dtGvp", "-s4", endpoint.connstr()])
    endpoint.stop()

    NUM_STARTS = 100

    durations: dict[str, list[int]] = {
        "sync_sk_check_ms": [],
        "sync_safekeepers_ms": [],
        "basebackup_ms": [],
        "start_postgres_ms": [],
        "total_startup_ms": [],
    }

    for _i in range(NUM_STARTS):
        endpoint.start()
        client = endpoint.http_client()
        metrics = client.metrics_json()
        for key in durations.keys():
            value = metrics[key]
            durations[key].append(value)
        endpoint.stop()

    for key in durations.keys():
        durations[key] = sorted(durations[key])
        zenbenchmark.record(
            f"{key}_p50",
            durations[key][len(durations[key]) // 2],
            "ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
        zenbenchmark.record(
            f"{key}_p90",
            durations[key][len(durations[key]) * 9 // 10],
            "ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
        zenbenchmark.record(
            f"{key}_p99",
            durations[key][len(durations[key]) * 99 // 100],
            "ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
