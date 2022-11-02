import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, Union

import pytest
import toml
from fixtures.neon_fixtures import (
    NeonCli,
    NeonEnvBuilder,
    PageserverHttpClient,
    PgBin,
    PortDistributor,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn
from pytest import FixtureRequest

DEFAILT_LOCAL_SNAPSHOT_DIR = "test_output/test_prepare_snapshot/compatibility_snapshot_pg14"


def dump_differs(first: Path, second: Path, output: Path) -> bool:
    """
    Runs diff(1) command on two SQL dumps and write the output to the given output file.
    Returns True if the dumps differ, False otherwise.
    """

    with output.open("w") as stdout:
        rv = subprocess.run(
            [
                "diff",
                "--unified",  # Make diff output more readable
                "--ignore-matching-lines=^--",  # Ignore changes in comments
                "--ignore-blank-lines",
                str(first),
                str(second),
            ],
            stdout=stdout,
        )

    return rv.returncode != 0


class PortReplacer(object):
    """
    Class-helper for replacing ports in config files.
    """

    def __init__(self, port_distributor: PortDistributor):
        self.port_distributor = port_distributor
        self.port_map: Dict[int, int] = {}

    def replace_port(self, value: Union[int, str]) -> Union[int, str]:
        if isinstance(value, int):
            if (known_port := self.port_map.get(value)) is not None:
                return known_port

            self.port_map[value] = self.port_distributor.get_port()
            return self.port_map[value]

        if isinstance(value, str):
            # Use regex to find port in a string
            # urllib.parse.urlparse produces inconvenient results for cases without scheme like "localhost:5432"
            # See https://bugs.python.org/issue27657
            ports = re.findall(r":(\d+)(?:/|$)", value)
            assert len(ports) == 1, f"can't find port in {value}"
            port_int = int(ports[0])

            if (known_port := self.port_map.get(port_int)) is not None:
                return value.replace(f":{port_int}", f":{known_port}")

            self.port_map[port_int] = self.port_distributor.get_port()
            return value.replace(f":{port_int}", f":{self.port_map[port_int]}")

        raise TypeError(f"unsupported type {type(value)} of {value=}")


@pytest.mark.order(after="test_prepare_snapshot")
def test_backward_compatibility(
    pg_bin: PgBin, port_distributor: PortDistributor, test_output_dir: Path, request: FixtureRequest
):
    compatibility_snapshot_dir = Path(
        os.environ.get("COMPATIBILITY_SNAPSHOT_DIR", DEFAILT_LOCAL_SNAPSHOT_DIR)
    )
    assert compatibility_snapshot_dir.exists(), (
        f"{compatibility_snapshot_dir} doesn't exist. Please run `test_prepare_snapshot` test first "
        "to create the snapshot or set COMPATIBILITY_SNAPSHOT_DIR env variable to the existing snapshot"
    )
    compatibility_snapshot_dir = compatibility_snapshot_dir.resolve()

    # Make compatibility snapshot artifacts pickupable by Allure
    # by copying the snapshot directory to the curent test output directory.
    repo_dir = test_output_dir / "compatibility_snapshot" / "repo"

    shutil.copytree(compatibility_snapshot_dir / "repo", repo_dir)

    # Remove old logs to avoid confusion in test artifacts
    for logfile in repo_dir.glob("**/*.log"):
        logfile.unlink()

    # Remove tenants data for computes
    for tenant in (repo_dir / "pgdatadirs" / "tenants").glob("*"):
        shutil.rmtree(tenant)

    # Remove wal-redo temp directory
    for tenant in (repo_dir / "tenants").glob("*"):
        shutil.rmtree(tenant / "wal-redo-datadir.___temp")

    # Update paths and ports in config files
    pr = PortReplacer(port_distributor)

    pageserver_toml = repo_dir / "pageserver.toml"
    pageserver_config = toml.load(pageserver_toml)
    new_local_path = pageserver_config["remote_storage"]["local_path"].replace(
        "/test_prepare_snapshot/",
        "/test_backward_compatibility/compatibility_snapshot/",
    )

    pageserver_config["remote_storage"]["local_path"] = new_local_path
    pageserver_config["listen_http_addr"] = pr.replace_port(pageserver_config["listen_http_addr"])
    pageserver_config["listen_pg_addr"] = pr.replace_port(pageserver_config["listen_pg_addr"])
    pageserver_config["broker_endpoints"] = [
        pr.replace_port(ep) for ep in pageserver_config["broker_endpoints"]
    ]

    with pageserver_toml.open("w") as f:
        toml.dump(pageserver_config, f)

    snapshot_config_toml = repo_dir / "config"
    snapshot_config = toml.load(snapshot_config_toml)
    snapshot_config["etcd_broker"]["broker_endpoints"] = [
        pr.replace_port(ep) for ep in snapshot_config["etcd_broker"]["broker_endpoints"]
    ]
    snapshot_config["pageserver"]["listen_http_addr"] = pr.replace_port(
        snapshot_config["pageserver"]["listen_http_addr"]
    )
    snapshot_config["pageserver"]["listen_pg_addr"] = pr.replace_port(
        snapshot_config["pageserver"]["listen_pg_addr"]
    )
    for sk in snapshot_config["safekeepers"]:
        sk["http_port"] = pr.replace_port(sk["http_port"])
        sk["pg_port"] = pr.replace_port(sk["pg_port"])

    with (snapshot_config_toml).open("w") as f:
        toml.dump(snapshot_config, f)

    # Ensure that snapshot doesn't contain references to the original path
    rv = subprocess.run(
        [
            "grep",
            "--recursive",
            "--binary-file=without-match",
            "--files-with-matches",
            "test_prepare_snapshot/repo",
            str(repo_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert (
        rv.returncode != 0
    ), f"there're files referencing `test_prepare_snapshot/repo`, this path should be replaced with {repo_dir}:\n{rv.stdout}"

    # NeonEnv stub to make NeonCli happy
    config: Any = type("NeonEnvStub", (object,), {})
    config.rust_log_override = None
    config.repo_dir = repo_dir
    config.pg_version = "14"  # Note: `pg_dumpall` (from pg_bin) version is set by DEFAULT_PG_VERSION_DEFAULT and can be overriden by DEFAULT_PG_VERSION env var
    config.initial_tenant = snapshot_config["default_tenant_id"]

    # Check that we can start the project
    cli = NeonCli(config)
    try:
        cli.raw_cli(["start"])
        request.addfinalizer(lambda: cli.raw_cli(["stop"]))

        result = cli.pg_start("main", port=port_distributor.get_port())
        request.addfinalizer(lambda: cli.pg_stop("main"))
    except Exception:
        breaking_changes_allowed = (
            os.environ.get("ALLOW_BREAKING_CHANGES", "false").lower() == "true"
        )
        if breaking_changes_allowed:
            pytest.xfail("Breaking changes are allowed by ALLOW_BREAKING_CHANGES env var")
        else:
            raise

    connstr_all = re.findall(r"Starting postgres node at '([^']+)'", result.stdout)
    assert len(connstr_all) == 1, f"can't parse connstr from {result.stdout}"
    connstr = connstr_all[0]

    # Check that the project produces the same dump as the previous version.
    # The assert itself deferred to the end of the test
    # to allow us to perform checks that change data before failing
    pg_bin.run(["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"])
    initial_dump_differs = dump_differs(
        compatibility_snapshot_dir / "dump.sql",
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )

    # Check that project can be recovered from WAL
    # loosely based on https://github.com/neondatabase/cloud/wiki/Recovery-from-WAL
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]
    pageserver_port = snapshot_config["pageserver"]["listen_http_addr"].split(":")[-1]
    auth_token = snapshot_config["pageserver"]["auth_token"]
    pageserver_http = PageserverHttpClient(
        port=pageserver_port,
        is_testing_enabled_or_skip=lambda: True,  # TODO: check if testing really enabled
        auth_token=auth_token,
    )

    shutil.rmtree(repo_dir / "local_fs_remote_storage")
    pageserver_http.timeline_delete(tenant_id, timeline_id)
    pageserver_http.timeline_create(tenant_id, timeline_id)
    pg_bin.run(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump-from-wal.sql'}"]
    )
    # The assert itself deferred to the end of the test
    # to allow us to perform checks that change data before failing
    dump_from_wal_differs = dump_differs(
        test_output_dir / "dump.sql",
        test_output_dir / "dump-from-wal.sql",
        test_output_dir / "dump-from-wal.filediff",
    )

    # Check that we can interract with the data
    pg_bin.run(["pgbench", "--time=10", "--progress=2", connstr])

    assert not dump_from_wal_differs, "dump from WAL differs"
    assert not initial_dump_differs, "initial dump differs"


# Note: if renaming this test, don't forget to update a reference to it in a workflow file:
# "Upload compatibility snapshot" step in .github/actions/run-python-test-set/action.yml
def test_prepare_snapshot(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, test_output_dir: Path):
    # The test doesn't really test anything
    # it creates a new snapshot for releases after we tested the current version against the previous snapshot in `test_backward_compatibility`.
    #
    # There's no cleanup here, it allows to adjust the data in `test_backward_compatibility` itself without re-collecting it.
    neon_env_builder.pg_version = "14"
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_local_fs_remote_storage()

    env = neon_env_builder.init_start()
    pg = env.postgres.create_start("main")
    pg_bin.run(["pgbench", "--initialize", "--scale=10", pg.connstr()])
    pg_bin.run(["pgbench", "--time=60", "--progress=2", pg.connstr()])
    pg_bin.run(["pg_dumpall", f"--dbname={pg.connstr()}", f"--file={test_output_dir / 'dump.sql'}"])

    snapshot_config = toml.load(test_output_dir / "repo" / "config")
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]

    pageserver_http = env.pageserver.http_client()
    lsn = Lsn(pg.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, lsn)

    env.postgres.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    env.pageserver.stop()

    shutil.copytree(test_output_dir, test_output_dir / "compatibility_snapshot_pg14")
    # Directory `test_output_dir / "compatibility_snapshot_pg14"` is uploaded to S3 in a workflow, keep the name in sync with it
