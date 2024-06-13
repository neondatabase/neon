from __future__ import annotations

import time
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extras
import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_api import connection_parameters_to_env
from fixtures.neon_fixtures import logical_replication_sync

if TYPE_CHECKING:
    from fixtures.benchmark_fixture import NeonBenchmarker
    from fixtures.neon_api import NeonAPI
    from fixtures.neon_fixtures import NeonEnv, PgBin
    from fixtures.pg_version import PgVersion


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
    print(f"connstr='{connstr}'")
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


def measure_logical_replication_lag(sub_cur, pub_cur):
    start = time.time()
    pub_cur.execute("SELECT pg_current_wal_flush_lsn()")
    pub_lsn = Lsn(pub_cur.fetchall()[0][0])
    while True:
        sub_cur.execute("SELECT latest_end_lsn FROM pg_catalog.pg_stat_subscription")
        res = sub_cur.fetchall()[0][0]
        if res:
            log.info(f"subscriber_lsn={res}")
            sub_lsn = Lsn(res)
            log.info(f"Subscriber LSN={sub_lsn}, publisher LSN={pub_lsn}")
            if sub_lsn >= pub_lsn:
                return time.time() - start
        time.sleep(0.5)


@pytest.mark.remote_cluster
@pytest.mark.timeout(2 * 60 * 60)
def test_subscriber_lag(
    pg_bin: PgBin,
    neon_api: NeonAPI,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    test_duration_min = 2
    sync_interval_min = 0.5

    pub_project = neon_api.create_project(pg_version)
    pub_project_id = pub_project["project"]["id"]
    neon_api.wait_for_operation_to_finish(pub_project_id)
    should_delete_pub = True
    try:
        sub_project = neon_api.create_project(pg_version)
        sub_project_id = sub_project["project"]["id"]
        sub_endpoint_id = sub_project["endpoints"][0]["id"]
        neon_api.wait_for_operation_to_finish(sub_project_id)
        should_delete_sub = True
        try:
            pub_env = connection_parameters_to_env(
                pub_project["connection_uris"][0]["connection_parameters"]
            )
            sub_env = connection_parameters_to_env(
                sub_project["connection_uris"][0]["connection_parameters"]
            )
            pub_connstr = pub_project["connection_uris"][0]["connection_uri"]
            sub_connstr = sub_project["connection_uris"][0]["connection_uri"]

            pub_conn = psycopg2.connect(pub_connstr)
            sub_conn = psycopg2.connect(sub_connstr)

            pub_conn.autocommit = True
            sub_conn.autocommit = True

            pub_cur = pub_conn.cursor()
            sub_cur = sub_conn.cursor()

            pg_bin.run_capture(["pgbench", "-i", "-s100", pub_connstr])
            pg_bin.run_capture(["pgbench", "-i", "-s100", sub_connstr])
            sub_cur.execute("truncate table pgbench_accounts")
            sub_cur.execute("truncate table pgbench_history")

            pub_cur.execute("create publication pub1 for table pgbench_accounts, pgbench_history")
            sub_cur.execute(f"create subscription sub1 connection '{pub_connstr}' publication pub1")

            initial_sync_lag = measure_logical_replication_lag(sub_cur, pub_cur)
            zenbenchmark.record(
                "initial_sync_lag", initial_sync_lag, "s", MetricReport.LOWER_IS_BETTER
            )

            pub_workload = pg_bin.run_nonblocking(
                ["pgbench", "-c10", "-T1000", "-Mprepared"], env=pub_env
            )
            try:
                sub_workload = pg_bin.run_nonblocking(
                    ["pgbench", "-c10", "-T1000", "-S"],
                    env=sub_env,
                )
                try:
                    start = time.time()
                    while time.time() - start < test_duration_min * 60:
                        time.sleep(sync_interval_min * 60)
                        lag = measure_logical_replication_lag(sub_cur, pub_cur)
                        log.info(f"Replica lagged behind master by {lag} seconds")
                        zenbenchmark.record("replica_lag", lag, "s", MetricReport.LOWER_IS_BETTER)
                        sub_workload.terminate()
                        neon_api.restart_endpoint(
                            sub_project_id,
                            sub_endpoint_id,
                        )

                        sub_workload = pg_bin.run_nonblocking(
                            ["pgbench", "-c10", "-T1000", "-S", sub_connstr]
                        )

                finally:
                    sub_workload.terminate()
            finally:
                pub_workload.terminate()
        finally:
            if should_delete_sub:
                neon_api.delete_project(sub_project_id)
    finally:
        if should_delete_pub:
            neon_api.delete_project(pub_project_id)
