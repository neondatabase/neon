import csv
import os
import subprocess
import time
from pathlib import Path
from typing import Any, List, Optional

import psycopg2
import psycopg2.extras
import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import PgBin
from fixtures.pg_version import PgVersion

from performance.neon_api import (
    neon_create_endpoint,
    neon_create_project,
    neon_delete_project,
    neon_get_connection_uri,
    neon_suspend_endpoint,
    neon_wait_for_operation_to_finish,
)


# Granularity of ~0.5 sec
def measure_replication_lag(master, replica):
    start = time.time()
    master.execute("SELECT pg_current_wal_flush_lsn()")
    master_lsn = Lsn(master.fetchall()[0][0])
    while True:
        replica.execute("select pg_last_wal_replay_lsn()")
        replica_lsn = replica.fetchall()[0][0]
        if replica_lsn:
            if Lsn(replica_lsn) >= master_lsn:
                return time.time() - start
        time.sleep(0.5)


@pytest.mark.remote_cluster
@pytest.mark.timeout(0)
def test_ro_replica_lag(
    pg_bin: PgBin,
    neon_api_key: str,
    neon_api_base_url: str,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    test_duration_min = 5
    sync_interval_min = 1

    project = neon_create_project(neon_api_key, neon_api_base_url, pg_version)
    project_id = project["project"]["id"]
    neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)
    should_delete = True
    try:
        branch_id = project["branch"]["id"]
        master_connstr = project["connection_uris"][0]["connection_uri"]

        replica = neon_create_endpoint(
            neon_api_key,
            neon_api_base_url,
            project_id,
            branch_id,
            endpoint_type="read_only",
        )
        neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)

        replica_connstr = neon_get_connection_uri(
            neon_api_key,
            neon_api_base_url,
            project_id,
            endpoint_id=replica["endpoint"]["id"],
        )["uri"]

        pg_bin.run_capture(["pgbench", "-i", "-s100", master_connstr])
        conn_master = psycopg2.connect(master_connstr)
        cur_master = conn_master.cursor()
        conn_replica = psycopg2.connect(replica_connstr)
        cur_replica = conn_replica.cursor()

        master_workload = pg_bin.run_nonblocking(
            ["pgbench", "-c10", "-T1000", "-Mprepared", master_connstr]
        )
        try:
            replica_workload = pg_bin.run_nonblocking(
                ["pgbench", "-c10", "-T1000", "-S", replica_connstr]
            )
            try:
                start = time.time()
                while time.time() - start < test_duration_min * 60:
                    time.sleep(sync_interval_min * 60)
                    lag = measure_replication_lag(cur_master, cur_replica)
                    log.info(f"Replica lagged behind master by {lag} seconds")
                    zenbenchmark.record("replica_lag", lag, "s", MetricReport.LOWER_IS_BETTER)
            finally:
                replica_workload.terminate()
        finally:
            master_workload.terminate()
    except Exception:
        should_delete = False
    finally:
        if should_delete:
            neon_delete_project(neon_api_key, neon_api_base_url, project_id)


def report_pgbench_aggregate_intervals(
    output_dir: Path,
    prefix: str,
    zenbenchmark: NeonBenchmarker,
):
    for filename in os.listdir(output_dir):
        if filename.startswith(prefix):
            # The file will be in the form <prefix>_<node>.<pid>
            # So we first lop off the .<pid>, and then lop off the prefix and the _
            node = filename.split(".")[0][len(prefix) + 1 :]
            with open(output_dir / filename) as f:
                reader = csv.reader(f, delimiter=" ")
                for line in reader:
                    num_transactions = int(line[1])
                    if num_transactions == 0:
                        continue
                    sum_latency = int(line[2])
                    sum_lag = int(line[3])
                    zenbenchmark.record(
                        f"{node}_num_txns", num_transactions, "txns", MetricReport.HIGHER_IS_BETTER
                    )
                    zenbenchmark.record(
                        f"{node}_avg_latency",
                        sum_latency / num_transactions,
                        "s",
                        MetricReport.LOWER_IS_BETTER,
                    )
                    zenbenchmark.record(
                        f"{node}_avg_lag",
                        sum_lag / num_transactions,
                        "s",
                        MetricReport.LOWER_IS_BETTER,
                    )


@pytest.mark.remote_cluster
@pytest.mark.timeout(0)
def test_replication_start_stop(
    pg_bin: PgBin,
    test_output_dir: Path,
    neon_api_key: str,
    neon_api_base_url: str,
    pg_version: PgVersion,
    zenbenchmark: NeonBenchmarker,
):
    """
    Cycles through different configurations of read replicas being enabled disabled. The whole time,
    there's a pgbench read/write workload going on the master. For each replica, we either turn it
    on or off, and see how long it takes to catch up after some set amount of time of replicating
    the pgbench.
    """

    prefix = "pgbench_agg"
    num_replicas = 2
    configuration_test_time_sec = 5
    should_delete = True

    project = neon_create_project(neon_api_key, neon_api_base_url, pg_version)
    project_id = project["project"]["id"]
    neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)
    try:
        branch_id = project["branch"]["id"]
        master_connstr = project["connection_uris"][0]["connection_uri"]

        replicas = []
        for _ in range(num_replicas):
            replicas.append(
                neon_create_endpoint(
                    neon_api_key,
                    neon_api_base_url,
                    project_id,
                    branch_id,
                    endpoint_type="read_only",
                )
            )
            neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)

        replica_connstr = [
            neon_get_connection_uri(
                neon_api_key,
                neon_api_base_url,
                project_id,
                endpoint_id=replicas[i]["endpoint"]["id"],
            )["uri"]
            for i in range(num_replicas)
        ]

        pg_bin.run_capture(["pgbench", "-i", "-s10", master_connstr])
        conn_master = psycopg2.connect(master_connstr)
        cur_master = conn_master.cursor()

        conn_replica: List[Optional[psycopg2.connection]] = [
            psycopg2.connect(replica_connstr[i]) for i in range(num_replicas)
        ]
        cur_replica: List[Optional[psycopg2.cursor]] = []
        for i in range(num_replicas):
            conn = conn_replica[i]
            assert conn is not None
            cur_replica.append(conn.cursor())

        # Sync replicas
        for i in range(num_replicas):
            measure_replication_lag(cur_master, cur_replica[i])

        master_pgbench = pg_bin.run_nonblocking(
            [
                "pgbench",
                "-c10",
                "-T1000",
                "-Mprepared",
                "--log",
                f"--log-prefix={test_output_dir}/{prefix}_master",
                f"--aggregate-interval={configuration_test_time_sec}",
                master_connstr,
            ]
        )
        replica_pgbench: List[Optional[subprocess.Popen[Any]]] = [None for i in range(num_replicas)]

        # Use the bits of iconfig to tell us which configuration we are on. For example
        # a iconfig of 2 is 10 in binary, indicating replica 0 is suspended and replica 1 is
        # alive.
        for iconfig in range((1 << num_replicas) - 1, -1, -1):

            def replica_enabled(ireplica, iconfig=iconfig):
                return bool((iconfig >> 1) & 1)

            # Change configuration
            for ireplica in range(num_replicas):
                if replica_enabled(ireplica) and replica_pgbench[ireplica] is None:
                    replica_pgbench[ireplica] = pg_bin.run_nonblocking(
                        [
                            "pgbench",
                            "-c10",
                            "-S",
                            "-T1000",
                            "--log",
                            f"--log-prefix={test_output_dir}/{prefix}_replica_{ireplica}",
                            f"--aggregate-interval={configuration_test_time_sec}",
                            replica_connstr[ireplica],
                        ]
                    )
                elif not replica_enabled(ireplica) and replica_pgbench[ireplica] is not None:
                    pgb = replica_pgbench[ireplica]
                    assert pgb is not None
                    pgb.terminate()
                    pgb.wait()
                    replica_pgbench[ireplica] = None
                    conn_replica[ireplica] = None
                    cur_replica[ireplica] = None

                    neon_suspend_endpoint(
                        neon_api_key,
                        neon_api_base_url,
                        project_id,
                        replicas[ireplica]["endpoint"]["id"],
                    )
                    neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)
            time.sleep(configuration_test_time_sec)

            for ireplica in range(num_replicas):
                conn = conn_replica[ireplica]
                if conn is None:
                    conn = psycopg2.connect(replica_connstr[i])
                    conn_replica[ireplica] = conn
                    cur_replica[ireplica] = conn.cursor()

                lag = measure_replication_lag(cur_master, cur_replica[ireplica])
                zenbenchmark.record(
                    f"Replica {ireplica} lag", lag, "s", MetricReport.LOWER_IS_BETTER
                )
                log.info(
                    f"Replica {ireplica} lagging behind master by {lag} seconds after configuration {iconfig:>b}"
                )
        master_pgbench.terminate()
    except Exception:
        should_delete = False
    finally:
        if should_delete:
            neon_delete_project(neon_api_key, neon_api_base_url, project_id)
            # Only report results if we didn't error out
            report_pgbench_aggregate_intervals(test_output_dir, prefix, zenbenchmark)
