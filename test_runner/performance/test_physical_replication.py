import time
import psycopg2
import psycopg2.extras


import pytest
from fixtures.log_helper import log
from performance.neon_api import *
from fixtures.neon_fixtures import PgBin

def is_operation_ongoing(neon_api_key: str, neon_api_base_url: str, project_id: str):
    operations = neon_get_operations(neon_api_key, neon_api_base_url, project_id)["operations"]
    for op in operations:
        if op["status"] in {"scheduling", "running", "cancelling"}:
            return True

    return False

def replication_sync(master, replica):
    master.execute("SELECT pg_current_wal_flush_lsn()")
    master_lsn = master.fetchall()[0][0]
    while True:
        replica.execute("select pg_last_wal_replay_lsn()")
        replica_lsn = replica.fetchall()[0][0]
        if replica_lsn:
            if master_lsn >= replica_lsn:
                return replica_lsn
        time.sleep(0.5)

def test_ro_replica_lag(
        pg_bin: PgBin,
        neon_api_key: str,
        neon_api_base_url: str,
        pg_version: PgVersion,
):
    project = neon_create_project(neon_api_key, neon_api_base_url, pg_version)
    project_id = project["project"]["id"]
    branch_id = project["branch"]["id"]
    master_connstr = project["connection_uris"][0]["connection_uri"]

    while is_operation_ongoing(neon_api_key, neon_api_base_url, project_id):
        time.sleep(1)

    replica = neon_create_endpoint(
        neon_api_key,
        neon_api_base_url,
        project_id,
        branch_id,
        endpoint_type="read_only",
    )

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

    pg_bin.run_capture(["pgbench", "-c10", "-T100", "-Mprepared", master_connstr])

    start = time.time()
    replication_sync(cur_master, cur_replica)
    log.info(f"Sync with master took {time.time() - start} seconds")
