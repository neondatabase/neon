import time
import re
import psycopg2
import psycopg2.extras
from pathlib import Path

import pytest
from fixtures.log_helper import log
from performance.neon_api import *
from fixtures.neon_fixtures import PgBin
from fixtures.common_types import Lsn

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
):
    test_duration_min = 5
    sync_interval_min = 1

    project = neon_create_project(neon_api_key, neon_api_base_url, pg_version)
    neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)
    project_id = project["project"]["id"]
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

        master_workload = pg_bin.run_nonblocking(["pgbench", "-c10", "-T1000", "-Mprepared", master_connstr])
        try:
            replica_workload = pg_bin.run_nonblocking(["pgbench", "-c10", "-T1000", "-S", replica_connstr])
            try:

                start = time.time()
                while time.time() - start < test_duration_min * 60:
                    time.sleep(sync_interval_min * 60)
                    lag = measure_replication_lag(cur_master, cur_replica)
                    log.info(f"Replica lagged behind master by {lag} seconds")
            finally:
                replica_workload.terminate()
        finally:
            replica_workload.terminate()
    except:
        should_delete = False
    finally:
        if should_delete:
            neon_delete_project(neon_api_key, neon_api_base_url, project_id)

@pytest.mark.remote_cluster
@pytest.mark.timeout(0)
def test_replication_start_stop(
        pg_bin: PgBin,
        test_output_dir: Path,
        neon_api_key: str,
        neon_api_base_url: str,
        pg_version: PgVersion,
):
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
        for i in range(num_replicas):
            replicas.append(neon_create_endpoint(
                neon_api_key,
                neon_api_base_url,
                project_id,
                branch_id,
                endpoint_type="read_only",
            ))
            neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)

        replica_connstr = [neon_get_connection_uri(
            neon_api_key,
            neon_api_base_url,
            project_id,
            endpoint_id=replicas[i]["endpoint"]["id"],
        )["uri"] for i in range(num_replicas)]

        pg_bin.run_capture(["pgbench", "-i", "-s10", master_connstr])
        conn_master = psycopg2.connect(master_connstr)
        cur_master = conn_master.cursor()

        conn_replica = [psycopg2.connect(replica_connstr[i]) for i in range(num_replicas)]
        cur_replica = [conn_replica[i].cursor() for i in range(num_replicas)]

        # Sync replicas
        for i in range(num_replicas):
            measure_replication_lag(cur_master, cur_replica[i])

        master_pgbench = pg_bin.run_nonblocking(["pgbench", "-c10", "-T1000", "-Mprepared", "--log", f"--log-prefix={test_output_dir}/pgbench_master", f"--aggregate-interval={configuration_test_time_sec}", master_connstr])
        replica_pgbench = [None for i in range(num_replicas)]

        for iconfig in range((1 << num_replicas) - 1, -1, -1):
            def replica_enabled(ireplica):
                return bool((iconfig >> 1) & 1)
            
            # Change configuration
            for ireplica in range(num_replicas):
                if replica_enabled(ireplica) and replica_pgbench[ireplica] is None:
                    replica_pgbench[ireplica] = pg_bin.run_nonblocking(
                        ["pgbench", "-c10", "-S", "-T1000", "--log", f"--log-prefix={test_output_dir}/pgbench_replica_{ireplica}", f"--aggregate-interval={configuration_test_time_sec}", replica_connstr[ireplica]]
                    )
                elif not replica_enabled(ireplica) and replica_pgbench[ireplica] is not None:
                    replica_pgbench[ireplica].terminate()
                    replica_pgbench[ireplica].wait()
                    replica_pgbench[ireplica] = None
                    conn_replica[ireplica] = None
                    cur_replica[ireplica] = None

                    neon_suspend_compute(
                        neon_api_key,
                        neon_api_base_url,
                        project_id,
                        replicas[ireplica]["endpoint"]["id"],
                    )
                    neon_wait_for_operation_to_finish(neon_api_key, neon_api_base_url, project_id)
            time.sleep(configuration_test_time_sec)

            for ireplica in range(num_replicas):
                if conn_replica[ireplica] is None:
                    conn_replica[ireplica] = psycopg2.connect(replica_connstr[i])
                    cur_replica[ireplica] = conn_replica[ireplica].cursor()

                lag = measure_replication_lag(cur_master, cur_replica[ireplica])
                log.info(f"Replica {ireplica} lagging behind master by {lag} seconds after configuration {iconfig:>b}")
    except:
        should_delete = False
    finally:
        if should_delete:
            neon_delete_project(neon_api_key, neon_api_base_url, project_id)
