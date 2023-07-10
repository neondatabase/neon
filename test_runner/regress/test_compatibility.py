import copy
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

import pytest
import toml  # TODO: replace with tomllib for Python >= 3.11
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonCli,
    NeonEnvBuilder,
    PgBin,
    PortDistributor,
    parse_project_git_version_output,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.pg_version import PgVersion
from fixtures.types import Lsn
from pytest import FixtureRequest

#
# A test suite that help to prevent unintentionally breaking backward or forward compatibility between Neon releases.
# - `test_create_snapshot` a script wrapped in a test that creates a data snapshot.
# - `test_backward_compatibility` checks that the current version of Neon can start/read/interract with a data snapshot created by the previous version.
#   The path to the snapshot is configured by COMPATIBILITY_SNAPSHOT_DIR environment variable.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE=true.
# - `test_forward_compatibility` checks that a snapshot created by the current version can be started/read/interracted by the previous version of Neon.
#   Paths to Neon and Postgres are configured by COMPATIBILITY_NEON_BIN and COMPATIBILITY_POSTGRES_DISTRIB_DIR environment variables.
#   If the breakage is intentional, the test can be xfaild with setting ALLOW_FORWARD_COMPATIBILITY_BREAKAGE=true.
#
# The file contains a couple of helper functions:
# - prepare_snapshot copies the snapshot, cleans it up and makes it ready for the current version of Neon (replaces paths and ports in config files).
# - check_neon_works performs the test itself, feel free to add more checks there.
#

check_ondisk_data_compatibility_if_enabled = pytest.mark.skipif(
    os.environ.get("CHECK_ONDISK_DATA_COMPATIBILITY") is None,
    reason="CHECK_ONDISK_DATA_COMPATIBILITY env is not set",
)


@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(before="test_forward_compatibility")
def test_create_snapshot(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
    top_output_dir: Path,
    test_output_dir: Path,
    pg_version: PgVersion,
):
    # The test doesn't really test anything
    # it creates a new snapshot for releases after we tested the current version against the previous snapshot in `test_backward_compatibility`.
    #
    # There's no cleanup here, it allows to adjust the data in `test_backward_compatibility` itself without re-collecting it.
    neon_env_builder.pg_version = pg_version
    neon_env_builder.num_safekeepers = 3
    neon_env_builder.enable_local_fs_remote_storage()
    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")

    # FIXME: Is this expected?
    env.pageserver.allowed_errors.append(
        ".*init_tenant_mgr: marking .* as locally complete, while it doesnt exist in remote index.*"
    )

    pg_bin.run(["pgbench", "--initialize", "--scale=10", endpoint.connstr()])
    pg_bin.run(["pgbench", "--time=60", "--progress=2", endpoint.connstr()])
    pg_bin.run(
        ["pg_dumpall", f"--dbname={endpoint.connstr()}", f"--file={test_output_dir / 'dump.sql'}"]
    )

    snapshot_config = toml.load(test_output_dir / "repo" / "config")
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]

    pageserver_http = env.pageserver.http_client()
    lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])

    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, lsn)

    env.endpoints.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    env.pageserver.stop()

    # Directory `compatibility_snapshot_dir` is uploaded to S3 in a workflow, keep the name in sync with it
    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )
    if compatibility_snapshot_dir.exists():
        shutil.rmtree(compatibility_snapshot_dir)
    shutil.copytree(test_output_dir, compatibility_snapshot_dir)


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_backward_compatibility(
    pg_bin: PgBin,
    port_distributor: PortDistributor,
    test_output_dir: Path,
    neon_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    request: FixtureRequest,
):
    """
    Test that the new binaries can read old data
    """
    compatibility_snapshot_dir_env = os.environ.get("COMPATIBILITY_SNAPSHOT_DIR")
    assert (
        compatibility_snapshot_dir_env is not None
    ), f"COMPATIBILITY_SNAPSHOT_DIR is not set. It should be set to `compatibility_snapshot_pg{pg_version.v_prefixed}` path generateted by test_create_snapshot (ideally generated by the previous version of Neon)"
    compatibility_snapshot_dir = Path(compatibility_snapshot_dir_env).resolve()

    breaking_changes_allowed = (
        os.environ.get("ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        # Copy the snapshot to current directory, and prepare for the test
        prepare_snapshot(
            from_dir=compatibility_snapshot_dir,
            to_dir=test_output_dir / "compatibility_snapshot",
            neon_binpath=neon_binpath,
            port_distributor=port_distributor,
        )

        check_neon_works(
            test_output_dir / "compatibility_snapshot" / "repo",
            neon_binpath,
            neon_binpath,
            pg_distrib_dir,
            pg_version,
            port_distributor,
            test_output_dir,
            pg_bin,
            request,
        )
    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert (
        not breaking_changes_allowed
    ), "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_forward_compatibility(
    test_output_dir: Path,
    top_output_dir: Path,
    port_distributor: PortDistributor,
    pg_version: PgVersion,
    request: FixtureRequest,
    neon_binpath: Path,
):
    """
    Test that the old binaries can read new data
    """
    compatibility_neon_bin_env = os.environ.get("COMPATIBILITY_NEON_BIN")
    assert compatibility_neon_bin_env is not None, (
        "COMPATIBILITY_NEON_BIN is not set. It should be set to a path with Neon binaries "
        "(ideally generated by the previous version of Neon)"
    )
    compatibility_neon_bin = Path(compatibility_neon_bin_env).resolve()

    compatibility_postgres_distrib_dir_env = os.environ.get("COMPATIBILITY_POSTGRES_DISTRIB_DIR")
    assert (
        compatibility_postgres_distrib_dir_env is not None
    ), "COMPATIBILITY_POSTGRES_DISTRIB_DIR is not set. It should be set to a pg_install directrory (ideally generated by the previous version of Neon)"
    compatibility_postgres_distrib_dir = Path(compatibility_postgres_distrib_dir_env).resolve()

    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )

    breaking_changes_allowed = (
        os.environ.get("ALLOW_FORWARD_COMPATIBILITY_BREAKAGE", "false").lower() == "true"
    )

    try:
        # Copy the snapshot to current directory, and prepare for the test
        prepare_snapshot(
            from_dir=compatibility_snapshot_dir,
            to_dir=test_output_dir / "compatibility_snapshot",
            port_distributor=port_distributor,
            neon_binpath=compatibility_neon_bin,
            pg_distrib_dir=compatibility_postgres_distrib_dir,
        )

        check_neon_works(
            test_output_dir / "compatibility_snapshot" / "repo",
            compatibility_neon_bin,
            neon_binpath,
            compatibility_postgres_distrib_dir,
            pg_version,
            port_distributor,
            test_output_dir,
            PgBin(test_output_dir, compatibility_postgres_distrib_dir, pg_version),
            request,
        )
    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert (
        not breaking_changes_allowed
    ), "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


def prepare_snapshot(
    from_dir: Path,
    to_dir: Path,
    port_distributor: PortDistributor,
    neon_binpath: Path,
    pg_distrib_dir: Optional[Path] = None,
):
    assert from_dir.exists(), f"Snapshot '{from_dir}' doesn't exist"
    assert (from_dir / "repo").exists(), f"Snapshot '{from_dir}' doesn't contain a repo directory"
    assert (from_dir / "dump.sql").exists(), f"Snapshot '{from_dir}' doesn't contain a dump.sql"

    log.info(f"Copying snapshot from {from_dir} to {to_dir}")
    shutil.copytree(from_dir, to_dir)

    repo_dir = to_dir / "repo"

    # Remove old logs to avoid confusion in test artifacts
    for logfile in repo_dir.glob("**/*.log"):
        logfile.unlink()

    # Remove old computes in 'endpoints'. Old versions of the control plane used a directory
    # called "pgdatadirs". Delete it, too.
    if (repo_dir / "endpoints").exists():
        shutil.rmtree(repo_dir / "endpoints")
    if (repo_dir / "pgdatadirs").exists():
        shutil.rmtree(repo_dir / "pgdatadirs")
    os.mkdir(repo_dir / "endpoints")

    # Remove wal-redo temp directory if it exists. Newer pageserver versions don't create
    # them anymore, but old versions did.
    for tenant in (repo_dir / "tenants").glob("*"):
        wal_redo_dir = tenant / "wal-redo-datadir.___temp"
        if wal_redo_dir.exists() and wal_redo_dir.is_dir():
            shutil.rmtree(wal_redo_dir)

    # Update paths and ports in config files
    pageserver_toml = repo_dir / "pageserver.toml"
    pageserver_config = toml.load(pageserver_toml)
    pageserver_config["remote_storage"]["local_path"] = str(repo_dir / "local_fs_remote_storage")
    pageserver_config["listen_http_addr"] = port_distributor.replace_with_new_port(
        pageserver_config["listen_http_addr"]
    )
    pageserver_config["listen_pg_addr"] = port_distributor.replace_with_new_port(
        pageserver_config["listen_pg_addr"]
    )
    # since storage_broker these are overriden by neon_local during pageserver
    # start; remove both to prevent unknown options during etcd ->
    # storage_broker migration. TODO: remove once broker is released
    pageserver_config.pop("broker_endpoint", None)
    pageserver_config.pop("broker_endpoints", None)
    etcd_broker_endpoints = [f"http://localhost:{port_distributor.get_port()}/"]
    if get_neon_version(neon_binpath) == "49da498f651b9f3a53b56c7c0697636d880ddfe0":
        pageserver_config["broker_endpoints"] = etcd_broker_endpoints  # old etcd version

    # Older pageserver versions had just one `auth_type` setting. Now there
    # are separate settings for pg and http ports. We don't use authentication
    # in compatibility tests so just remove authentication related settings.
    pageserver_config.pop("auth_type", None)
    pageserver_config.pop("pg_auth_type", None)
    pageserver_config.pop("http_auth_type", None)

    if pg_distrib_dir:
        pageserver_config["pg_distrib_dir"] = str(pg_distrib_dir)

    with pageserver_toml.open("w") as f:
        toml.dump(pageserver_config, f)

    snapshot_config_toml = repo_dir / "config"
    snapshot_config = toml.load(snapshot_config_toml)

    # Provide up/downgrade etcd <-> storage_broker to make forward/backward
    # compatibility test happy. TODO: leave only the new part once broker is released.
    if get_neon_version(neon_binpath) == "49da498f651b9f3a53b56c7c0697636d880ddfe0":
        # old etcd version
        snapshot_config["etcd_broker"] = {
            "etcd_binary_path": shutil.which("etcd"),
            "broker_endpoints": etcd_broker_endpoints,
        }
        snapshot_config.pop("broker", None)
    else:
        # new storage_broker version
        broker_listen_addr = f"127.0.0.1:{port_distributor.get_port()}"
        snapshot_config["broker"] = {"listen_addr": broker_listen_addr}
        snapshot_config.pop("etcd_broker", None)

    snapshot_config["pageserver"]["listen_http_addr"] = port_distributor.replace_with_new_port(
        snapshot_config["pageserver"]["listen_http_addr"]
    )
    snapshot_config["pageserver"]["listen_pg_addr"] = port_distributor.replace_with_new_port(
        snapshot_config["pageserver"]["listen_pg_addr"]
    )
    for sk in snapshot_config["safekeepers"]:
        sk["http_port"] = port_distributor.replace_with_new_port(sk["http_port"])
        sk["pg_port"] = port_distributor.replace_with_new_port(sk["pg_port"])

    if pg_distrib_dir:
        snapshot_config["pg_distrib_dir"] = str(pg_distrib_dir)

    with snapshot_config_toml.open("w") as f:
        toml.dump(snapshot_config, f)

    # Ensure that snapshot doesn't contain references to the original path
    rv = subprocess.run(
        [
            "grep",
            "--recursive",
            "--binary-file=without-match",
            "--files-with-matches",
            "test_create_snapshot/repo",
            str(repo_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert (
        rv.returncode != 0
    ), f"there're files referencing `test_create_snapshot/repo`, this path should be replaced with {repo_dir}:\n{rv.stdout}"


# get git SHA of neon binary
def get_neon_version(neon_binpath: Path):
    out = subprocess.check_output([neon_binpath / "neon_local", "--version"]).decode("utf-8")
    return parse_project_git_version_output(out)


def check_neon_works(
    repo_dir: Path,
    neon_target_binpath: Path,
    neon_current_binpath: Path,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    port_distributor: PortDistributor,
    test_output_dir: Path,
    pg_bin: PgBin,
    request: FixtureRequest,
):
    snapshot_config_toml = repo_dir / "config"
    snapshot_config = toml.load(snapshot_config_toml)
    snapshot_config["neon_distrib_dir"] = str(neon_target_binpath)
    snapshot_config["postgres_distrib_dir"] = str(pg_distrib_dir)
    with (snapshot_config_toml).open("w") as f:
        toml.dump(snapshot_config, f)

    # TODO: replace with NeonEnvBuilder / NeonEnv
    config: Any = type("NeonEnvStub", (object,), {})
    config.rust_log_override = None
    config.repo_dir = repo_dir
    config.pg_version = pg_version
    config.initial_tenant = snapshot_config["default_tenant_id"]
    config.pg_distrib_dir = pg_distrib_dir
    config.preserve_database_files = True

    # Use the "target" binaries to launch the storage nodes
    config_target = config
    config_target.neon_binpath = neon_target_binpath
    cli_target = NeonCli(config_target)

    # And the current binaries to launch computes
    snapshot_config["neon_distrib_dir"] = str(neon_current_binpath)
    with (snapshot_config_toml).open("w") as f:
        toml.dump(snapshot_config, f)
    config_current = copy.copy(config)
    config_current.neon_binpath = neon_current_binpath
    cli_current = NeonCli(config_current)

    cli_target.raw_cli(["start"])
    request.addfinalizer(lambda: cli_target.raw_cli(["stop"]))

    pg_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    cli_current.endpoint_start("main", pg_port=pg_port, http_port=http_port)
    request.addfinalizer(lambda: cli_current.endpoint_stop("main"))

    connstr = f"host=127.0.0.1 port={pg_port} user=cloud_admin dbname=postgres"
    pg_bin.run(["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"])
    initial_dump_differs = dump_differs(
        repo_dir.parent / "dump.sql",
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )

    # Check that project can be recovered from WAL
    # loosely based on https://github.com/neondatabase/cloud/wiki/Recovery-from-WAL
    tenant_id = snapshot_config["default_tenant_id"]
    timeline_id = dict(snapshot_config["branch_name_mappings"]["main"])[tenant_id]
    pageserver_port = snapshot_config["pageserver"]["listen_http_addr"].split(":")[-1]
    pageserver_http = PageserverHttpClient(
        port=pageserver_port,
        is_testing_enabled_or_skip=lambda: True,  # TODO: check if testing really enabled
    )

    shutil.rmtree(repo_dir / "local_fs_remote_storage")
    timeline_delete_wait_completed(pageserver_http, tenant_id, timeline_id)
    pageserver_http.timeline_create(pg_version, tenant_id, timeline_id)
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


def dump_differs(first: Path, second: Path, output: Path) -> bool:
    """
    Runs diff(1) command on two SQL dumps and write the output to the given output file.
    Returns True if the dumps differ, False otherwise.
    """

    with output.open("w") as stdout:
        res = subprocess.run(
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

    differs = res.returncode != 0

    # TODO: Remove after https://github.com/neondatabase/neon/pull/4425 is merged, and a couple of releases are made
    if differs:
        with tempfile.NamedTemporaryFile(mode="w") as tmp:
            tmp.write(PR4425_ALLOWED_DIFF)
            tmp.flush()

            allowed = subprocess.run(
                [
                    "diff",
                    "--unified",  # Make diff output more readable
                    r"--ignore-matching-lines=^---",  # Ignore diff headers
                    r"--ignore-matching-lines=^\+\+\+",  # Ignore diff headers
                    "--ignore-matching-lines=^@@",  # Ignore diff blocks location
                    "--ignore-matching-lines=^ *$",  # Ignore lines with only spaces
                    "--ignore-matching-lines=^ --.*",  # Ignore the " --" lines for compatibility with PG14
                    "--ignore-blank-lines",
                    str(output),
                    str(tmp.name),
                ],
            )

            differs = allowed.returncode != 0

    return differs


PR4425_ALLOWED_DIFF = """
--- /tmp/test_output/test_backward_compatibility[release-pg15]/compatibility_snapshot/dump.sql 2023-06-08 18:12:45.000000000 +0000
+++ /tmp/test_output/test_backward_compatibility[release-pg15]/dump.sql        2023-06-13 07:25:35.211733653 +0000
@@ -13,12 +13,20 @@

 CREATE ROLE cloud_admin;
 ALTER ROLE cloud_admin WITH SUPERUSER INHERIT CREATEROLE CREATEDB LOGIN REPLICATION BYPASSRLS;
+CREATE ROLE neon_superuser;
+ALTER ROLE neon_superuser WITH NOSUPERUSER INHERIT CREATEROLE CREATEDB NOLOGIN NOREPLICATION NOBYPASSRLS;

 --
 -- User Configurations
 --


+--
+-- Role memberships
+--
+
+GRANT pg_read_all_data TO neon_superuser GRANTED BY cloud_admin;
+GRANT pg_write_all_data TO neon_superuser GRANTED BY cloud_admin;
"""
