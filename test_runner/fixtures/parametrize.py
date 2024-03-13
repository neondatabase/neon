import os
from typing import Optional

import pytest
from _pytest.python import Metafunc

from fixtures.pg_version import PgVersion

"""
Dynamically parametrize tests by different parameters
"""


@pytest.fixture(scope="function", autouse=True)
def pg_version() -> Optional[PgVersion]:
    return None


@pytest.fixture(scope="function", autouse=True)
def build_type() -> Optional[str]:
    return None


@pytest.fixture(scope="function", autouse=True)
def platform() -> Optional[str]:
    return None


@pytest.fixture(scope="function", autouse=True)
def pageserver_virtual_file_io_engine() -> Optional[str]:
    return None


def pytest_generate_tests(metafunc: Metafunc):
    if (bt := os.getenv("BUILD_TYPE")) is None:
        build_types = ["debug", "release"]
    else:
        build_types = [bt.lower()]

    metafunc.parametrize("build_type", build_types)

    if (v := os.getenv("DEFAULT_PG_VERSION")) is None:
        pg_versions = [version for version in PgVersion if version != PgVersion.NOT_SET]
    else:
        pg_versions = [PgVersion(v)]

    metafunc.parametrize("pg_version", pg_versions, ids=map(lambda v: f"pg{v}", pg_versions))

    # A hacky way to parametrize tests only for `pageserver_virtual_file_io_engine=std-fs`
    # And do not change test name for default `pageserver_virtual_file_io_engine=tokio-epoll-uring` to keep tests statistics
    if (io_engine := os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE", "")) not in (
        "",
        "tokio-epoll-uring",
    ):
        metafunc.parametrize("pageserver_virtual_file_io_engine", [io_engine])

    # For performance tests, parametrize also by platform
    if (
        "test_runner/performance" in metafunc.definition._nodeid
        and (platform := os.getenv("PLATFORM")) is not None
    ):
        metafunc.parametrize("platform", [platform.lower()])
