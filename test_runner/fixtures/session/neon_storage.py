import os
import shutil
import subprocess
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, cast

import pytest
from _pytest.config import Config
from _pytest.fixtures import FixtureRequest

from fixtures import overlayfs
from fixtures.broker import NeonBroker
from fixtures.log_helper import log
from fixtures.neon_api import NeonAPI
from fixtures.neon_fixtures import (
    DEFAULT_OUTPUT_DIR,
    NeonEnv,
    NeonEnvBuilder,
    get_test_output_dir,
    get_test_overlay_dir,
    get_test_repo_dir,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import MockS3Server
from fixtures.utils import AuxFileStore, allure_attach_from_dir, get_self_dir

BASE_PORT: int = 15000


@pytest.fixture(scope="session")
def base_dir() -> Iterator[Path]:
    # find the base directory (currently this is the git root)
    base_dir = get_self_dir().parent.parent
    log.info(f"base_dir is {base_dir}")

    yield base_dir


@pytest.fixture(scope="session")
def neon_binpath(base_dir: Path, build_type: str) -> Iterator[Path]:
    if os.getenv("REMOTE_ENV"):
        # we are in remote env and do not have neon binaries locally
        # this is the case for benchmarks run on self-hosted runner
        return

    # Find the neon binaries.
    if env_neon_bin := os.environ.get("NEON_BIN"):
        binpath = Path(env_neon_bin)
    else:
        binpath = base_dir / "target" / build_type
    log.info(f"neon_binpath is {binpath}")

    if not (binpath / "pageserver").exists():
        raise Exception(f"neon binaries not found at '{binpath}'")

    yield binpath


@pytest.fixture(scope="session")
def pg_distrib_dir(base_dir: Path) -> Iterator[Path]:
    if env_postgres_bin := os.environ.get("POSTGRES_DISTRIB_DIR"):
        distrib_dir = Path(env_postgres_bin).resolve()
    else:
        distrib_dir = base_dir / "pg_install"

    log.info(f"pg_distrib_dir is {distrib_dir}")
    yield distrib_dir


@pytest.fixture(scope="session")
def top_output_dir(base_dir: Path) -> Iterator[Path]:
    # Compute the top-level directory for all tests.
    if env_test_output := os.environ.get("TEST_OUTPUT"):
        output_dir = Path(env_test_output).resolve()
    else:
        output_dir = base_dir / DEFAULT_OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)

    log.info(f"top_output_dir is {output_dir}")
    yield output_dir


@pytest.fixture(scope="session")
def versioned_pg_distrib_dir(pg_distrib_dir: Path, pg_version: PgVersion) -> Iterator[Path]:
    versioned_dir = pg_distrib_dir / pg_version.v_prefixed

    psql_bin_path = versioned_dir / "bin/psql"
    postgres_bin_path = versioned_dir / "bin/postgres"

    if os.getenv("REMOTE_ENV"):
        # When testing against a remote server, we only need the client binary.
        if not psql_bin_path.exists():
            raise Exception(f"psql not found at '{psql_bin_path}'")
    else:
        if not postgres_bin_path.exists():
            raise Exception(f"postgres not found at '{postgres_bin_path}'")

    log.info(f"versioned_pg_distrib_dir is {versioned_dir}")
    yield versioned_dir


@pytest.fixture(scope="session")
def neon_api_key() -> str:
    api_key = os.getenv("NEON_API_KEY")
    if not api_key:
        raise AssertionError("Set the NEON_API_KEY environment variable")

    return api_key


@pytest.fixture(scope="session")
def neon_api_base_url() -> str:
    return os.getenv("NEON_API_BASE_URL", "https://console-stage.neon.build/api/v2")


@pytest.fixture(scope="session")
def neon_api(neon_api_key: str, neon_api_base_url: str) -> NeonAPI:
    return NeonAPI(neon_api_key, neon_api_base_url)


@pytest.fixture(scope="session")
def worker_port_num():
    return (32768 - BASE_PORT) // int(os.environ.get("PYTEST_XDIST_WORKER_COUNT", "1"))


@pytest.fixture(scope="session")
def worker_seq_no(worker_id: str) -> int:
    # worker_id is a pytest-xdist fixture
    # it can be master or gw<number>
    # parse it to always get a number
    if worker_id == "master":
        return 0
    assert worker_id.startswith("gw")
    return int(worker_id[2:])


@pytest.fixture(scope="session")
def worker_base_port(worker_seq_no: int, worker_port_num: int) -> int:
    # so we divide ports in ranges of ports
    # so workers have disjoint set of ports for services
    return BASE_PORT + worker_seq_no * worker_port_num


@pytest.fixture(scope="session")
def port_distributor(worker_base_port: int, worker_port_num: int) -> PortDistributor:
    return PortDistributor(base_port=worker_base_port, port_number=worker_port_num)


@pytest.fixture(scope="session")
def shared_broker(
    port_distributor: PortDistributor,
    shared_test_output_dir: Path,
    neon_binpath: Path,
) -> Iterator[NeonBroker]:
    # multiple pytest sessions could get launched in parallel, get them different ports/datadirs
    client_port = port_distributor.get_port()
    broker_logfile = shared_test_output_dir / "repo" / "storage_broker.log"

    broker = NeonBroker(logfile=broker_logfile, port=client_port, neon_binpath=neon_binpath)
    yield broker
    broker.stop()


@pytest.fixture(scope="session")
def run_id() -> Iterator[uuid.UUID]:
    yield uuid.uuid4()


@pytest.fixture(scope="session", autouse=True)
def neon_shared_env(
    pytestconfig: Config,
    port_distributor: PortDistributor,
    mock_s3_server: MockS3Server,
    shared_broker: NeonBroker,
    run_id: uuid.UUID,
    top_output_dir: Path,
    shared_test_output_dir: Path,
    neon_binpath: Path,
    build_type: str,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
    pageserver_virtual_file_io_engine: str,
    pageserver_aux_file_policy: Optional[AuxFileStore],
    pageserver_default_tenant_config_compaction_algorithm: Optional[Dict[str, Any]],
    pageserver_io_buffer_alignment: Optional[int],
    request: FixtureRequest,
) -> Iterator[NeonEnv]:
    """
    Simple Neon environment, with no authentication and no safekeepers.

    This fixture will use RemoteStorageKind.LOCAL_FS with pageserver.
    """

    prefix = f"shared[{build_type}-{pg_version.v_prefixed}]-"

    # Create the environment in the per-test output directory
    repo_dir = get_test_repo_dir(request, top_output_dir, prefix)

    with NeonEnvBuilder(
        top_output_dir=top_output_dir,
        repo_dir=repo_dir,
        port_distributor=port_distributor,
        broker=shared_broker,
        mock_s3_server=mock_s3_server,
        neon_binpath=neon_binpath,
        pg_distrib_dir=pg_distrib_dir,
        pg_version=pg_version,
        run_id=run_id,
        preserve_database_files=cast(bool, pytestconfig.getoption("--preserve-database-files")),
        test_name=f"{prefix}{request.node.name}",
        test_output_dir=shared_test_output_dir,
        pageserver_virtual_file_io_engine=pageserver_virtual_file_io_engine,
        pageserver_aux_file_policy=pageserver_aux_file_policy,
        pageserver_default_tenant_config_compaction_algorithm=pageserver_default_tenant_config_compaction_algorithm,
        pageserver_io_buffer_alignment=pageserver_io_buffer_alignment,
        shared=True,
    ) as builder:
        env = builder.init_start()

        yield env


# This is autouse, so the test output directory always gets created, even
# if a test doesn't put anything there.
#
# NB: we request the overlay dir fixture so the fixture does its cleanups
@pytest.fixture(scope="session")
def shared_test_output_dir(
    request: FixtureRequest,
    pg_version: PgVersion,
    build_type: str,
    top_output_dir: Path,
    shared_test_overlay_dir: Path,
) -> Iterator[Path]:
    """Create the working directory for shared tests."""

    prefix = f"shared[{build_type}-{pg_version.v_prefixed}]-"

    # one directory per test
    test_dir = get_test_output_dir(request, top_output_dir, prefix)

    log.info(f"test_output_dir is {test_dir}")
    shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir()

    yield test_dir

    # Allure artifacts creation might involve the creation of `.tar.zst` archives,
    # which aren't going to be used if Allure results collection is not enabled
    # (i.e. --alluredir is not set).
    # Skip `allure_attach_from_dir` in this case
    if not request.config.getoption("--alluredir"):
        return

    preserve_database_files = False
    for k, v in request.node.user_properties:
        # NB: the neon_env_builder fixture uses this fixture (test_output_dir).
        # So, neon_env_builder's cleanup runs before here.
        # The cleanup propagates NeonEnvBuilder.preserve_database_files into this user property.
        if k == "preserve_database_files":
            assert isinstance(v, bool)
            preserve_database_files = v

    allure_attach_from_dir(test_dir, preserve_database_files)


@pytest.fixture(scope="session")
def shared_test_overlay_dir(request: FixtureRequest, top_output_dir: Path) -> Optional[Path]:
    """
    Idempotently create a test's overlayfs mount state directory.
    If the functionality isn't enabled via env var, returns None.

    The procedure cleans up after previous runs that were aborted (e.g. due to Ctrl-C, OOM kills, etc).
    """

    if os.getenv("NEON_ENV_BUILDER_USE_OVERLAYFS_FOR_SNAPSHOTS") is None:
        return None

    overlay_dir = get_test_overlay_dir(request, top_output_dir)
    log.info(f"test_overlay_dir is {overlay_dir}")

    overlay_dir.mkdir(exist_ok=True)
    # unmount stale overlayfs mounts which subdirectories of `overlay_dir/*` as the overlayfs `upperdir` and `workdir`
    for mountpoint in overlayfs.iter_mounts_beneath(get_test_output_dir(request, top_output_dir)):
        cmd = ["sudo", "umount", str(mountpoint)]
        log.info(
            f"Unmounting stale overlayfs mount probably created during earlier test run: {cmd}"
        )
        subprocess.run(cmd, capture_output=True, check=True)
    # the overlayfs `workdir`` is owned by `root`, shutil.rmtree won't work.
    cmd = ["sudo", "rm", "-rf", str(overlay_dir)]
    subprocess.run(cmd, capture_output=True, check=True)

    overlay_dir.mkdir()

    return overlay_dir

    # no need to clean up anything: on clean shutdown,
    # NeonEnvBuilder.overlay_cleanup_teardown takes care of cleanup
    # and on unclean shutdown, this function will take care of it
    # on the next test run
