from __future__ import annotations

import time
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extras
import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import AuxFileStore, logical_replication_sync

if TYPE_CHECKING:
    from fixtures.benchmark_fixture import NeonBenchmarker
    from fixtures.neon_fixtures import NeonEnv, PgBin, RemoteNeonApiEndpoint


@pytest.mark.parametrize("pageserver_aux_file_policy", [AuxFileStore.V2])
@pytest.mark.timeout(1000)
def test_logical_replication(neon_simple_env: NeonEnv, pg_bin: PgBin, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start("test_logical_replication")

    log.info("postgres is running on 'test_logical_replication' branch")
    pg_bin.run_capture(["pgbench", "-i", "-s10", endpoint.connstr()])

    endpoint.safe_psql("create publication pub1 for table pgbench_accounts, pgbench_history")

    # now start subscriber
    vanilla_pg.start()
    pg_bin.run_capture(["pgbench", "-i", "-s10", vanilla_pg.connstr()])

    vanilla_pg.safe_psql("truncate table pgbench_accounts")
    vanilla_pg.safe_psql("truncate table pgbench_history")

    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)

    pg_bin.run_capture(["pgbench", "-c10", "-T100", "-Mprepared", endpoint.connstr()])

    # Wait logical replication to sync
    start = time.time()
    logical_replication_sync(vanilla_pg, endpoint)
    log.info(f"Sync with master took {time.time() - start} seconds")

    sum_master = endpoint.safe_psql("select sum(abalance) from pgbench_accounts")[0][0]
    sum_replica = vanilla_pg.safe_psql("select sum(abalance) from pgbench_accounts")[0][0]
    assert sum_master == sum_replica


def check_pgbench_still_running(pgbench, label=""):
    rc = pgbench.poll()
    if rc is not None:
        raise RuntimeError(f"{label} pgbench terminated early with return code {rc}")


def measure_logical_replication_lag(sub_cur, pub_cur, timeout_sec=600):
    start = time.time()
    pub_cur.execute("SELECT pg_current_wal_flush_lsn()")
    pub_lsn = Lsn(pub_cur.fetchall()[0][0])
    while (time.time() - start) < timeout_sec:
        sub_cur.execute("SELECT latest_end_lsn FROM pg_catalog.pg_stat_subscription")
        res = sub_cur.fetchall()[0][0]
        if res:
            log.info(f"subscriber_lsn={res}")
            sub_lsn = Lsn(res)
            log.info(f"Subscriber LSN={sub_lsn}, publisher LSN={pub_lsn}")
            if sub_lsn >= pub_lsn:
                return time.time() - start
        time.sleep(0.5)
    raise TimeoutError(f"Logical replication sync took more than {timeout_sec} sec")


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_subscriber_lag(
    pg_bin: PgBin,
    benchmark_project_pub: RemoteNeonApiEndpoint,
    benchmark_project_sub: RemoteNeonApiEndpoint,
    zenbenchmark: NeonBenchmarker,
):
    """
    Creates a publisher and subscriber, runs pgbench inserts on publisher and pgbench selects
    on subscriber. Periodically restarts subscriber while still running the inserts, and
    measures how long sync takes after restart.
    """
    test_duration_min = 60
    sync_interval_min = 5
    pgbench_duration = f"-T{test_duration_min * 60 * 2}"

    pub_env = benchmark_project_pub.pgbench_env
    sub_env = benchmark_project_sub.pgbench_env
    pub_connstr = benchmark_project_pub.connstr
    sub_connstr = benchmark_project_sub.connstr

    pg_bin.run_capture(["pgbench", "-i", "-s100"], env=pub_env)
    pg_bin.run_capture(["pgbench", "-i", "-s100"], env=sub_env)

    pub_conn = psycopg2.connect(pub_connstr)
    sub_conn = psycopg2.connect(sub_connstr)
    pub_conn.autocommit = True
    sub_conn.autocommit = True
    with pub_conn.cursor() as pub_cur, sub_conn.cursor() as sub_cur:
        if benchmark_project_pub.is_new:
            pub_cur.execute("create publication pub1 for table pgbench_accounts, pgbench_history")

        if benchmark_project_sub.is_new:
            sub_cur.execute("truncate table pgbench_accounts")
            sub_cur.execute("truncate table pgbench_history")

            sub_cur.execute(f"create subscription sub1 connection '{pub_connstr}' publication pub1")

        initial_sync_lag = measure_logical_replication_lag(sub_cur, pub_cur)
    pub_conn.close()
    sub_conn.close()

    zenbenchmark.record("initial_sync_lag", initial_sync_lag, "s", MetricReport.LOWER_IS_BETTER)

    pub_workload = pg_bin.run_nonblocking(
        ["pgbench", "-c10", pgbench_duration, "-Mprepared"], env=pub_env
    )
    try:
        sub_workload = pg_bin.run_nonblocking(
            ["pgbench", "-c10", pgbench_duration, "-S"],
            env=sub_env,
        )
        try:
            start = time.time()
            while time.time() - start < test_duration_min * 60:
                time.sleep(sync_interval_min * 60)
                check_pgbench_still_running(pub_workload, "pub")
                check_pgbench_still_running(sub_workload, "sub")

                with psycopg2.connect(pub_connstr) as pub_conn, psycopg2.connect(
                    sub_connstr
                ) as sub_conn:
                    with pub_conn.cursor() as pub_cur, sub_conn.cursor() as sub_cur:
                        lag = measure_logical_replication_lag(sub_cur, pub_cur)

                log.info(f"Replica lagged behind master by {lag} seconds")
                zenbenchmark.record("replica_lag", lag, "s", MetricReport.LOWER_IS_BETTER)
                sub_workload.terminate()
                benchmark_project_sub.restart()

                sub_workload = pg_bin.run_nonblocking(
                    ["pgbench", "-c10", pgbench_duration, "-S"],
                    env=sub_env,
                )

                # Measure storage to make sure replication information isn't bloating storage
                sub_storage = benchmark_project_sub.get_synthetic_storage_size()
                pub_storage = benchmark_project_pub.get_synthetic_storage_size()
                zenbenchmark.record("sub_storage", sub_storage, "B", MetricReport.LOWER_IS_BETTER)
                zenbenchmark.record("pub_storage", pub_storage, "B", MetricReport.LOWER_IS_BETTER)
        finally:
            sub_workload.terminate()
    finally:
        pub_workload.terminate()


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_publisher_restart(
    pg_bin: PgBin,
    benchmark_project_pub: RemoteNeonApiEndpoint,
    benchmark_project_sub: RemoteNeonApiEndpoint,
    zenbenchmark: NeonBenchmarker,
):
    """
    Creates a publisher and subscriber, runs pgbench inserts on publisher and pgbench selects
    on subscriber. Periodically restarts publisher (to exercise on-demand WAL download), and
    measures how long sync takes after restart.
    """
    test_duration_min = 60
    sync_interval_min = 5
    pgbench_duration = f"-T{test_duration_min * 60 * 2}"

    pub_env = benchmark_project_pub.pgbench_env
    sub_env = benchmark_project_sub.pgbench_env
    pub_connstr = benchmark_project_pub.connstr
    sub_connstr = benchmark_project_sub.connstr

    pg_bin.run_capture(["pgbench", "-i", "-s100"], env=pub_env)
    pg_bin.run_capture(["pgbench", "-i", "-s100"], env=sub_env)

    pub_conn = psycopg2.connect(pub_connstr)
    sub_conn = psycopg2.connect(sub_connstr)
    pub_conn.autocommit = True
    sub_conn.autocommit = True
    with pub_conn.cursor() as pub_cur, sub_conn.cursor() as sub_cur:
        if benchmark_project_pub.is_new:
            pub_cur.execute("create publication pub1 for table pgbench_accounts, pgbench_history")

        if benchmark_project_sub.is_new:
            sub_cur.execute("truncate table pgbench_accounts")
            sub_cur.execute("truncate table pgbench_history")

            sub_cur.execute(f"create subscription sub1 connection '{pub_connstr}' publication pub1")

        initial_sync_lag = measure_logical_replication_lag(sub_cur, pub_cur)
    pub_conn.close()
    sub_conn.close()

    zenbenchmark.record("initial_sync_lag", initial_sync_lag, "s", MetricReport.LOWER_IS_BETTER)

    pub_workload = pg_bin.run_nonblocking(
        ["pgbench", "-c10", pgbench_duration, "-Mprepared"], env=pub_env
    )
    try:
        sub_workload = pg_bin.run_nonblocking(
            ["pgbench", "-c10", pgbench_duration, "-S"],
            env=sub_env,
        )
        try:
            start = time.time()
            while time.time() - start < test_duration_min * 60:
                time.sleep(sync_interval_min * 60)
                check_pgbench_still_running(pub_workload, "pub")
                check_pgbench_still_running(sub_workload, "sub")

                pub_workload.terminate()
                benchmark_project_pub.restart()
                pub_workload = pg_bin.run_nonblocking(
                    ["pgbench", "-c10", pgbench_duration, "-Mprepared"],
                    env=pub_env,
                )
                with psycopg2.connect(pub_connstr) as pub_conn, psycopg2.connect(
                    sub_connstr
                ) as sub_conn:
                    with pub_conn.cursor() as pub_cur, sub_conn.cursor() as sub_cur:
                        lag = measure_logical_replication_lag(sub_cur, pub_cur)

                log.info(f"Replica lagged behind master by {lag} seconds")
                zenbenchmark.record("replica_lag", lag, "s", MetricReport.LOWER_IS_BETTER)

                # Measure storage to make sure replication information isn't bloating storage
                sub_storage = benchmark_project_sub.get_synthetic_storage_size()
                pub_storage = benchmark_project_pub.get_synthetic_storage_size()
                zenbenchmark.record("sub_storage", sub_storage, "B", MetricReport.LOWER_IS_BETTER)
                zenbenchmark.record("pub_storage", pub_storage, "B", MetricReport.LOWER_IS_BETTER)
        finally:
            sub_workload.terminate()
    finally:
        pub_workload.terminate()
