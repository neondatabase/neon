from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Dict, Any, Iterator

import pytest

from fixtures.common_types import TimelineId, Lsn
from fixtures.neon_fixtures import Endpoint, NeonEnv, PgProtocol, Safekeeper
from fixtures.pg_version import PgVersion

"""
In this file most important resources are exposed as function-level fixtures
that depend on session-level resources like pageservers and safekeepers.

The main rationale here is that we don't need to initialize a new SK/PS/etc
every time we want to test something that has branching: we can just as well
reuse still-available PageServers from other tests of the same kind. 
"""


@pytest.fixture(scope="session")
def shared_storage_repo_path(build_type: str, base_dir: Path) -> Path:
    return base_dir / f"shared[{build_type}]" / "repo"



class TestEndpoint(PgProtocol):
    def __init__(self, compute: Endpoint, **kwargs: Any):
        super().__init__(**kwargs)


class TestTimeline:
    def __init__(self, name: str, tenant: 'TestTenant'):
        self.primary = None

    def start_primary(self) -> Optional[TestEndpoint]:
        if not self.primary:
            return None
        self.primary.start()

        return self.primary


class TestPageserver:
    """
    We only need one pageserver for every build_type.
    """

    def __init__(
            self,
            build_type: str,
            bin_path: Path,
            repo_path: Path,
            port: int
    ):
        self.build_type = build_type
        self.bin_path = bin_path
        self.repo_path = repo_path
        self.port = port
        self.started = False

    def _start(self):
        pass

    def _stop(self):
        pass


@pytest.fixture(scope="session")
def shared_pageserver() -> Iterator[TestPageserver]:
    yield None


class TestSafekeeper:
    """
    We only need one safekeeper for every build_type.
    """

    def __init__(
            self,
            build_type: str,
            bin_path: Path,
            repo_path: Path,
            port: int
    ):
        self.build_type = build_type
        self.bin_path = bin_path
        self.repo_path = repo_path
        self.port = port
        self.started = False

    def _start(self):
        pass

    def _stop(self):
        pass


@pytest.fixture(scope="session")
def shared_safekeeper(
    request,
    port: int,
    build_type: str,
    neon_binpath: Path,
    shared_storage_repo_path: Path
) -> Iterator[TestSafekeeper]:
    sk = TestSafekeeper(
        build_type=build_type,
        bin_path=(neon_binpath / "safekeeper"),
        port=port,
        repo_path=shared_storage_repo_path,
    )
    sk.start()

    yield sk
    sk.stop()


class TestTenant:
    """
    An object representing a single test case on a shared pageserver.
    All operations here are safe practically safe.
    """

    def __init__(
            self,
            pageserver: TestPageserver,
            safekeeper: TestSafekeeper,
    ):
        self.pageserver = pageserver

    def create_timeline(
            self,
            name: str,
            parent_name: Optional[str],
            branch_at: Optional[Lsn],
    ) -> TestTimeline:
        pass

    def stop(self):
        for it in self.active_computes:
            it.stop()
