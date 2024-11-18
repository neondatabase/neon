from __future__ import annotations

import os
import shutil
import subprocess
import threading
from fcntl import LOCK_EX, LOCK_UN, flock
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING

import pytest
from pytest import FixtureRequest

from fixtures import overlayfs
from fixtures.log_helper import log
from fixtures.utils import allure_attach_from_dir

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Optional


BASE_DIR = Path(__file__).parents[2]
COMPUTE_CONFIG_DIR = BASE_DIR / "compute" / "etc"
DEFAULT_OUTPUT_DIR: str = "test_output"


def get_test_dir(
    request: FixtureRequest, top_output_dir: Path, prefix: Optional[str] = None
) -> Path:
    """Compute the path to a working directory for an individual test."""
    test_name = request.node.name
    test_dir = top_output_dir / f"{prefix or ''}{test_name.replace('/', '-')}"

    # We rerun flaky tests multiple times, use a separate directory for each run.
    if (suffix := getattr(request.node, "execution_count", None)) is not None:
        test_dir = test_dir.parent / f"{test_dir.name}-{suffix}"

    return test_dir


def get_test_output_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    """
    The working directory for a test.
    """
    return get_test_dir(request, top_output_dir)


def get_test_overlay_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    """
    Directory that contains `upperdir` and `workdir` for overlayfs mounts
    that a test creates. See `NeonEnvBuilder.overlay_mount`.
    """
    return get_test_dir(request, top_output_dir, "overlay-")


def get_shared_snapshot_dir_path(top_output_dir: Path, snapshot_name: str) -> Path:
    return top_output_dir / "shared-snapshots" / snapshot_name


def get_test_repo_dir(request: FixtureRequest, top_output_dir: Path) -> Path:
    return get_test_output_dir(request, top_output_dir) / "repo"


@pytest.fixture(scope="session")
def base_dir() -> Iterator[Path]:
    # find the base directory (currently this is the git root)
    log.info(f"base_dir is {BASE_DIR}")

    yield BASE_DIR


@pytest.fixture(scope="session")
def compute_config_dir() -> Iterator[Path]:
    """
    Retrieve the path to the compute configuration directory.
    """
    yield COMPUTE_CONFIG_DIR


@pytest.fixture(scope="function")
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

    yield binpath.absolute()


@pytest.fixture(scope="session")
def compatibility_snapshot_dir() -> Iterator[Path]:
    if os.getenv("REMOTE_ENV"):
        return
    compatibility_snapshot_dir_env = os.environ.get("COMPATIBILITY_SNAPSHOT_DIR")
    assert (
        compatibility_snapshot_dir_env is not None
    ), "COMPATIBILITY_SNAPSHOT_DIR is not set. It should be set to `compatibility_snapshot_pg(PG_VERSION)` path generateted by test_create_snapshot (ideally generated by the previous version of Neon)"
    compatibility_snapshot_dir = Path(compatibility_snapshot_dir_env).resolve()
    yield compatibility_snapshot_dir


@pytest.fixture(scope="session")
def compatibility_neon_binpath() -> Iterator[Optional[Path]]:
    if os.getenv("REMOTE_ENV"):
        return
    comp_binpath = None
    if env_compatibility_neon_binpath := os.environ.get("COMPATIBILITY_NEON_BIN"):
        comp_binpath = Path(env_compatibility_neon_binpath).resolve().absolute()
    yield comp_binpath


@pytest.fixture(scope="session")
def pg_distrib_dir(base_dir: Path) -> Iterator[Path]:
    if env_postgres_bin := os.environ.get("POSTGRES_DISTRIB_DIR"):
        distrib_dir = Path(env_postgres_bin).resolve()
    else:
        distrib_dir = base_dir / "pg_install"

    log.info(f"pg_distrib_dir is {distrib_dir}")
    yield distrib_dir


@pytest.fixture(scope="session")
def compatibility_pg_distrib_dir() -> Iterator[Optional[Path]]:
    compat_distrib_dir = None
    if env_compat_postgres_bin := os.environ.get("COMPATIBILITY_POSTGRES_DISTRIB_DIR"):
        compat_distrib_dir = Path(env_compat_postgres_bin).resolve()
        if not compat_distrib_dir.exists():
            raise Exception(f"compatibility postgres directory not found at {compat_distrib_dir}")

    if compat_distrib_dir:
        log.info(f"compatibility_pg_distrib_dir is {compat_distrib_dir}")
    yield compat_distrib_dir


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


# This is autouse, so the test output directory always gets created, even
# if a test doesn't put anything there.
#
# NB: we request the overlay dir fixture so the fixture does its cleanups
@pytest.fixture(scope="function", autouse=True)
def test_output_dir(request: pytest.FixtureRequest, top_output_dir: Path) -> Iterator[Path]:
    """Create the working directory for an individual test."""

    # one directory per test
    test_dir = get_test_output_dir(request, top_output_dir)
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


class FileAndThreadLock:
    def __init__(self, path: Path):
        self.path = path
        self.thread_lock = threading.Lock()
        self.fd: Optional[int] = None

    def __enter__(self):
        self.fd = os.open(self.path, os.O_CREAT | os.O_WRONLY)
        # lock thread lock before file lock so that there's no race
        # around flocking / funlocking the file lock
        self.thread_lock.acquire()
        flock(self.fd, LOCK_EX)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ):
        assert self.fd is not None
        assert self.thread_lock.locked()  # ... by us
        flock(self.fd, LOCK_UN)
        self.thread_lock.release()
        os.close(self.fd)
        self.fd = None


class SnapshotDirLocked:
    def __init__(self, parent: SnapshotDir):
        self._parent = parent

    def is_initialized(self):
        # TODO: in the future, take a `tag` as argument and store it in the marker in set_initialized.
        # Then, in this function, compare marker file contents with the tag to invalidate the snapshot if the tag changed.
        return self._parent.marker_file_path.exists()

    def set_initialized(self):
        self._parent.marker_file_path.write_text("")

    @property
    def path(self) -> Path:
        return self._parent.path / "snapshot"


class SnapshotDir:
    _path: Path

    def __init__(self, path: Path):
        self._path = path
        assert self._path.is_dir()
        self._lock = FileAndThreadLock(self.lock_file_path)

    @property
    def path(self) -> Path:
        return self._path

    @property
    def lock_file_path(self) -> Path:
        return self._path / "initializing.flock"

    @property
    def marker_file_path(self) -> Path:
        return self._path / "initialized.marker"

    def __enter__(self) -> SnapshotDirLocked:
        self._lock.__enter__()
        return SnapshotDirLocked(self)

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ):
        self._lock.__exit__(exc_type, exc_value, exc_traceback)


def shared_snapshot_dir(top_output_dir: Path, ident: str) -> SnapshotDir:
    snapshot_dir_path = get_shared_snapshot_dir_path(top_output_dir, ident)
    snapshot_dir_path.mkdir(exist_ok=True, parents=True)
    return SnapshotDir(snapshot_dir_path)


@pytest.fixture(scope="function")
def test_overlay_dir(request: FixtureRequest, top_output_dir: Path) -> Optional[Path]:
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
