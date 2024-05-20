import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import List, Optional

import pytest
import toml
from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.pg_version import PgVersion
from fixtures.remote_storage import RemoteStorageKind

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
# - check_neon_works performs the test itself, feel free to add more checks there.
# - dump_differs compares two SQL dumps and writes the diff to a file.
#
#
# How to run `test_backward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=15
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#    export COMPATIBILITY_SNAPSHOT_DIR=test_output/compatibility_snapshot_pgv${DEFAULT_PG_VERSION}
#
#    # Build previous version of binaries and create a data snapshot:
#    rm -rf pg_install target
#    git checkout <previous version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    ./scripts/pytest -k test_create_snapshot
#
#    # Build current version of binaries
#    rm -rf pg_install target
#    git checkout <current version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#
#    # Run backward compatibility test
#    ./scripts/pytest -k test_backward_compatibility
#
#
# How to run `test_forward_compatibility` locally:
#
#    export DEFAULT_PG_VERSION=15
#    export BUILD_TYPE=release
#    export CHECK_ONDISK_DATA_COMPATIBILITY=true
#    export COMPATIBILITY_NEON_BIN=neon_previous/target/${BUILD_TYPE}
#    export COMPATIBILITY_POSTGRES_DISTRIB_DIR=neon_previous/pg_install
#
#    # Build previous version of binaries and store them somewhere:
#    rm -rf pg_install target
#    git checkout <previous version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    mkdir -p neon_previous/target
#    cp -a target/${BUILD_TYPE} ./neon_previous/target/${BUILD_TYPE}
#    cp -a pg_install ./neon_previous/pg_install
#
#    # Build current version of binaries and create a data snapshot:
#    rm -rf pg_install target
#    git checkout <current version>
#    CARGO_BUILD_FLAGS="--features=testing" make -s -j`nproc`
#    ./scripts/pytest -k test_create_snapshot
#
#    # Run forward compatibility test
#    ./scripts/pytest -k test_forward_compatibility
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
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")

    pg_bin.run_capture(["pgbench", "--initialize", "--scale=10", endpoint.connstr()])
    pg_bin.run_capture(["pgbench", "--time=60", "--progress=2", endpoint.connstr()])
    pg_bin.run_capture(
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
    env.storage_controller.stop()

    # Directory `compatibility_snapshot_dir` is uploaded to S3 in a workflow, keep the name in sync with it
    compatibility_snapshot_dir = (
        top_output_dir / f"compatibility_snapshot_pg{pg_version.v_prefixed}"
    )
    if compatibility_snapshot_dir.exists():
        shutil.rmtree(compatibility_snapshot_dir)

    shutil.copytree(
        test_output_dir,
        compatibility_snapshot_dir,
        ignore=shutil.ignore_patterns("pg_dynshmem"),
    )


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_backward_compatibility(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    pg_version: PgVersion,
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
        neon_env_builder.num_safekeepers = 3
        env = neon_env_builder.from_repo_dir(compatibility_snapshot_dir / "repo")
        neon_env_builder.start()

        check_neon_works(
            env,
            test_output_dir=test_output_dir,
            sql_dump_path=compatibility_snapshot_dir / "dump.sql",
            repo_dir=env.repo_dir,
        )
    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert not breaking_changes_allowed, "Breaking changes are allowed by ALLOW_BACKWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


@check_ondisk_data_compatibility_if_enabled
@pytest.mark.xdist_group("compatibility")
@pytest.mark.order(after="test_create_snapshot")
def test_forward_compatibility(
    neon_env_builder: NeonEnvBuilder,
    test_output_dir: Path,
    top_output_dir: Path,
    pg_version: PgVersion,
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
        # Previous version neon_local and pageserver are not aware
        # of the new config.
        # TODO: remove these once the previous version of neon local supports them
        neon_env_builder.pageserver_get_impl = None
        neon_env_builder.pageserver_validate_vectored_get = None

        neon_env_builder.num_safekeepers = 3

        # Use previous version's production binaries (pageserver, safekeeper, pg_distrib_dir, etc.).
        # But always use the current version's neon_local binary.
        # This is because we want to test the compatibility of the data format, not the compatibility of the neon_local CLI.
        neon_env_builder.neon_binpath = compatibility_neon_bin
        neon_env_builder.pg_distrib_dir = compatibility_postgres_distrib_dir
        neon_env_builder.neon_local_binpath = neon_env_builder.neon_local_binpath

        env = neon_env_builder.from_repo_dir(
            compatibility_snapshot_dir / "repo",
        )

        # not using env.pageserver.version because it was initialized before
        prev_pageserver_version_str = env.get_binary_version("pageserver")
        prev_pageserver_version_match = re.search(
            "Neon page server git-env:(.*) failpoints: (.*), features: (.*)",
            prev_pageserver_version_str,
        )
        if prev_pageserver_version_match is not None:
            prev_pageserver_version = prev_pageserver_version_match.group(1)
        else:
            raise AssertionError(
                "cannot find git hash in the version string: " + prev_pageserver_version_str
            )

        # does not include logs from previous runs
        assert not env.pageserver.log_contains("git-env:" + prev_pageserver_version)

        neon_env_builder.start()

        # ensure the specified pageserver is running
        assert env.pageserver.log_contains("git-env:" + prev_pageserver_version)

        check_neon_works(
            env,
            test_output_dir=test_output_dir,
            sql_dump_path=compatibility_snapshot_dir / "dump.sql",
            repo_dir=env.repo_dir,
        )

    except Exception:
        if breaking_changes_allowed:
            pytest.xfail(
                "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE env var"
            )
        else:
            raise

    assert not breaking_changes_allowed, "Breaking changes are allowed by ALLOW_FORWARD_COMPATIBILITY_BREAKAGE, but the test has passed without any breakage"


def check_neon_works(env: NeonEnv, test_output_dir: Path, sql_dump_path: Path, repo_dir: Path):
    ep = env.endpoints.create_start("main")
    connstr = ep.connstr()

    pg_bin = PgBin(test_output_dir, env.pg_distrib_dir, env.pg_version)

    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump.sql'}"]
    )
    initial_dump_differs = dump_differs(
        sql_dump_path,
        test_output_dir / "dump.sql",
        test_output_dir / "dump.filediff",
    )

    # Check that project can be recovered from WAL
    # loosely based on https://www.notion.so/neondatabase/Storage-Recovery-from-WAL-d92c0aac0ebf40df892b938045d7d720
    pageserver_http = env.pageserver.http_client()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    pg_version = env.pg_version

    # Stop endpoint while we recreate timeline
    ep.stop()

    try:
        pageserver_http.timeline_preserve_initdb_archive(tenant_id, timeline_id)
    except PageserverApiException as e:
        # Allow the error as we might be running the old pageserver binary
        log.info(f"Got allowed error: '{e}'")

    # Delete all files from local_fs_remote_storage except initdb-preserved.tar.zst,
    # the file is required for `timeline_create` with `existing_initdb_timeline_id`.
    #
    # TODO: switch to Path.walk() in Python 3.12
    # for dirpath, _dirnames, filenames in (repo_dir / "local_fs_remote_storage").walk():
    for dirpath, _dirnames, filenames in os.walk(repo_dir / "local_fs_remote_storage"):
        for filename in filenames:
            if filename != "initdb-preserved.tar.zst" and filename != "initdb.tar.zst":
                (Path(dirpath) / filename).unlink()

    timeline_delete_wait_completed(pageserver_http, tenant_id, timeline_id)
    pageserver_http.timeline_create(
        pg_version=pg_version,
        tenant_id=tenant_id,
        new_timeline_id=timeline_id,
        existing_initdb_timeline_id=timeline_id,
    )

    # Timeline exists again: restart the endpoint
    ep.start()

    pg_bin.run_capture(
        ["pg_dumpall", f"--dbname={connstr}", f"--file={test_output_dir / 'dump-from-wal.sql'}"]
    )
    # The assert itself deferred to the end of the test
    # to allow us to perform checks that change data before failing
    dump_from_wal_differs = dump_differs(
        test_output_dir / "dump.sql",
        test_output_dir / "dump-from-wal.sql",
        test_output_dir / "dump-from-wal.filediff",
    )

    pg_bin.run_capture(["pg_amcheck", connstr, "--install-missing", "--verbose"])

    # Check that we can interract with the data
    pg_bin.run_capture(["pgbench", "--time=10", "--progress=2", connstr])

    assert not dump_from_wal_differs, "dump from WAL differs"
    assert not initial_dump_differs, "initial dump differs"


def dump_differs(
    first: Path, second: Path, output: Path, allowed_diffs: Optional[List[str]] = None
) -> bool:
    """
    Runs diff(1) command on two SQL dumps and write the output to the given output file.
    The function supports allowed diffs, if the diff is in the allowed_diffs list, it's not considered as a difference.
    See the example of it in https://github.com/neondatabase/neon/pull/4425/files#diff-15c5bfdd1d5cc1411b9221091511a60dd13a9edf672bdfbb57dd2ef8bb7815d6

    Returns True if the dumps differ and produced diff is not allowed, False otherwise (in most cases we want it to return False).
    """

    if not first.exists():
        raise FileNotFoundError(f"{first} doesn't exist")
    if not second.exists():
        raise FileNotFoundError(f"{second} doesn't exist")

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

    allowed_diffs = allowed_diffs or []
    if differs and len(allowed_diffs) > 0:
        for allowed_diff in allowed_diffs:
            with tempfile.NamedTemporaryFile(mode="w") as tmp:
                tmp.write(allowed_diff)
                tmp.flush()

                allowed = subprocess.run(
                    [
                        "diff",
                        "--unified",  # Make diff output more readable
                        r"--ignore-matching-lines=^---",  # Ignore diff headers
                        r"--ignore-matching-lines=^\+\+\+",  # Ignore diff headers
                        "--ignore-matching-lines=^@@",  # Ignore diff blocks location
                        "--ignore-matching-lines=^ *$",  # Ignore lines with only spaces
                        "--ignore-matching-lines=^ --.*",  # Ignore SQL comments in diff
                        "--ignore-blank-lines",
                        str(output),
                        str(tmp.name),
                    ],
                )

                differs = allowed.returncode != 0
                if not differs:
                    break

    return differs
