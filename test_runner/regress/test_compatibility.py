import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import toml
from fixtures.neon_fixtures import (
    NeonCli,
    NeonEnvBuilder,
    PgBin,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn
from pytest import FixtureRequest


def test_backward_compatibility(pg_bin: PgBin, test_output_dir: Path, request: FixtureRequest):
    compatibility_snapshot_dir_env = os.environ.get("COMPATIBILITY_SNAPSHOT_DIR")
    assert (
        compatibility_snapshot_dir_env is not None
    ), "COMPATIBILITY_SNAPSHOT_DIR is not set. It should be set to `compatibility_snapshot_pg14` path generateted by test_prepare_snapshot"
    compatibility_snapshot_dir: Path = Path(compatibility_snapshot_dir_env).resolve()

    # Make compatibility snapshot artifacts pickupable by Allure
    # by copying the snapshot directory to the curent test output directory.
    repo_dir = test_output_dir / "compatibility_snapshot" / "repo"

    shutil.copytree(compatibility_snapshot_dir / "repo", repo_dir)

    # Remove old logs to avoid confusion in test artifacts
    for logfile in repo_dir.glob("**/*.log"):
        logfile.unlink()

    # Update paths in configs
    pageserver_config = (repo_dir / "pageserver.toml").read_text()
    pageserver_config = pageserver_config.replace(
        "/test_prepare_snapshot/", "/test_backward_compatibility/compatibility_snapshot/"
    )
    (repo_dir / "pageserver.toml").write_text(pageserver_config)

    for postmaster_opts in repo_dir.glob("**/postmaster.opts"):
        postmaster_opts_content = postmaster_opts.read_text()
        postmaster_opts_content = postmaster_opts_content.replace(
            "/test_prepare_snapshot/", "/test_backward_compatibility/compatibility_snapshot/"
        )
        postmaster_opts.write_text(postmaster_opts_content)

    snapshot_config = toml.load(repo_dir / "config")

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

        result = cli.pg_start("main")
        request.addfinalizer(lambda: cli.pg_stop("main"))
    except Exception:
        breaking_changes_allowed = (
            os.environ.get("ALLOW_BREAKING_CHANGES", "false").lower() == "true"
        )
        if breaking_changes_allowed:
            pytest.xfail("Breaking changes are allowed by ALLOW_BREAKING_CHANGES env var")
        else:
            raise

    connstr_re = re.compile(r"Starting postgres node at '([^']+)'")
    connstr_all = connstr_re.findall(result.stdout)
    assert len(connstr_all) == 1, f"can't parse connstr from {result.stdout}"
    connstr = connstr_all[0]

    # Check that the project produces the same dump
    pg_bin.run(["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"])
    with (test_output_dir / "dump.filediff").open("w") as stdout:
        rv = subprocess.run(
            [
                "diff",
                "--unified",  # Make diff output more readable
                "--ignore-matching-lines=^--",  # Ignore changes in comments
                "--ignore-blank-lines",
                str(compatibility_snapshot_dir / "dump.sql"),
                str(test_output_dir / "dump.sql"),
            ],
            stdout=stdout,
        )
        # A real assert deferred to the end of the test,
        # to allow us to perform checks that change data
        dump_differs = rv.returncode != 0

    # Check that we can interract with the project
    pg_bin.run(["pgbench", "--time=10", "--progress=2", connstr])

    assert not dump_differs, "dump differs"


@pytest.mark.order(after="test_backward_compatibility")
# Note: if renaming this test, don't forget to update a reference to it in a workflow file:
#   "Upload compatibility snapshot" step in .github/actions/run-python-test-set/action.yml
def test_prepare_snapshot(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, test_output_dir: Path):
    # The test doesn't really test anything
    # it creates a new snapshot for releases after we tested the current version against the previous snapshot in `test_backward_compatibility`
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

    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, lsn)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, lsn)

    env.postgres.stop_all()
    for sk in env.safekeepers:
        sk.stop()
    env.pageserver.stop()

    shutil.copytree(test_output_dir, test_output_dir / "compatibility_snapshot_pg14")
    # Directory `test_output_dir / "compatibility_snapshot_pg14"` is uploaded to S3 in a workflow, keep the name in sync with it
