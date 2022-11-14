import os
from subprocess import TimeoutExpired

from fixtures.log_helper import log
from fixtures.neon_fixtures import ComputeCtl, NeonEnvBuilder, PgBin


# Test that compute_ctl works and prints "--sync-safekeepers" logs.
def test_sync_safekeepers_logs(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()
    ctl = ComputeCtl(env)

    env.neon_cli.create_branch("test_compute_ctl", "main")
    pg = env.postgres.create_start("test_compute_ctl")
    pg.safe_psql("CREATE TABLE t(key int primary key, value text)")

    with open(pg.config_file_path(), "r") as f:
        cfg_lines = f.readlines()
    cfg_map = {}
    for line in cfg_lines:
        if "=" in line:
            k, v = line.split("=")
            cfg_map[k] = v.strip("\n '\"")
    log.info(f"postgres config: {cfg_map}")
    pgdata = pg.pg_data_dir_path()
    pg_bin_path = os.path.join(pg_bin.pg_bin_path, "postgres")

    pg.stop_and_destroy()

    spec = (
        """
{
    "format_version": 1.0,

    "timestamp": "2021-05-23T18:25:43.511Z",
    "operation_uuid": "0f657b36-4b0f-4a2d-9c2e-1dcd615e7d8b",

    "cluster": {
        "cluster_id": "test-cluster-42",
        "name": "Neon Test",
        "state": "restarted",
        "roles": [
        ],
        "databases": [
        ],
        "settings": [
            {
                "name": "fsync",
                "value": "off",
                "vartype": "bool"
            },
            {
                "name": "wal_level",
                "value": "replica",
                "vartype": "enum"
            },
            {
                "name": "hot_standby",
                "value": "on",
                "vartype": "bool"
            },
            {
                "name": "neon.safekeepers",
                "value": """
        + f'"{cfg_map["neon.safekeepers"]}"'
        + """,
                "vartype": "string"
            },
            {
                "name": "wal_log_hints",
                "value": "on",
                "vartype": "bool"
            },
            {
                "name": "log_connections",
                "value": "on",
                "vartype": "bool"
            },
            {
                "name": "shared_buffers",
                "value": "32768",
                "vartype": "integer"
            },
            {
                "name": "port",
                "value": """
        + f'"{cfg_map["port"]}"'
        + """,
                "vartype": "integer"
            },
            {
                "name": "max_connections",
                "value": "100",
                "vartype": "integer"
            },
            {
                "name": "max_wal_senders",
                "value": "10",
                "vartype": "integer"
            },
            {
                "name": "listen_addresses",
                "value": "0.0.0.0",
                "vartype": "string"
            },
            {
                "name": "wal_sender_timeout",
                "value": "0",
                "vartype": "integer"
            },
            {
                "name": "password_encryption",
                "value": "md5",
                "vartype": "enum"
            },
            {
                "name": "maintenance_work_mem",
                "value": "65536",
                "vartype": "integer"
            },
            {
                "name": "max_parallel_workers",
                "value": "8",
                "vartype": "integer"
            },
            {
                "name": "max_worker_processes",
                "value": "8",
                "vartype": "integer"
            },
            {
                "name": "neon.tenant_id",
                "value": """
        + f'"{cfg_map["neon.tenant_id"]}"'
        + """,
                "vartype": "string"
            },
            {
                "name": "max_replication_slots",
                "value": "10",
                "vartype": "integer"
            },
            {
                "name": "neon.timeline_id",
                "value": """
        + f'"{cfg_map["neon.timeline_id"]}"'
        + """,
                "vartype": "string"
            },
            {
                "name": "shared_preload_libraries",
                "value": "neon",
                "vartype": "string"
            },
            {
                "name": "synchronous_standby_names",
                "value": "walproposer",
                "vartype": "string"
            },
            {
                "name": "neon.pageserver_connstring",
                "value": """
        + f'"{cfg_map["neon.pageserver_connstring"]}"'
        + """,
                "vartype": "string"
            }
        ]
    },
    "delta_operations": [
    ]
}
"""
    )

    ps_connstr = cfg_map["neon.pageserver_connstring"]
    log.info(f"ps_connstr: {ps_connstr}, pgdata: {pgdata}")

    # run compute_ctl and wait for 10s
    try:
        ctl.raw_cli(
            [
                "--connstr",
                "postgres://invalid/",
                "--pgdata",
                pgdata,
                "--spec",
                spec,
                "--pgbin",
                pg_bin_path,
            ],
            timeout=10,
        )
    except TimeoutExpired as exc:
        ctl_logs = exc.stderr.decode("utf-8")
        log.info("compute_ctl output:\n" + ctl_logs)

    start = "starting safekeepers syncing"
    end = "safekeepers synced at LSN"
    start_pos = ctl_logs.index(start)
    assert start_pos != -1
    end_pos = ctl_logs.index(end, start_pos)
    assert end_pos != -1
    sync_safekeepers_logs = ctl_logs[start_pos : end_pos + len(end)]
    log.info("sync_safekeepers_logs:\n" + sync_safekeepers_logs)

    # assert that --sync-safekeepers logs are present in the output
    assert "connecting with node" in sync_safekeepers_logs
    assert "connected with node" in sync_safekeepers_logs
    assert "proposer connected to quorum (2)" in sync_safekeepers_logs
    assert "got votes from majority (2)" in sync_safekeepers_logs
    assert "sending elected msg to node" in sync_safekeepers_logs
