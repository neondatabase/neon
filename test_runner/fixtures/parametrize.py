from __future__ import annotations

import os
from typing import TYPE_CHECKING

import allure
import pytest
import toml
from _pytest.python import Metafunc

from fixtures.pg_version import PgVersion

if TYPE_CHECKING:
    from typing import Any


"""
Dynamically parametrize tests by different parameters
"""


@pytest.fixture(scope="function", autouse=True)
def pg_version() -> PgVersion | None:
    return None


@pytest.fixture(scope="function", autouse=True)
def build_type() -> str | None:
    return None


@pytest.fixture(scope="session", autouse=True)
def platform() -> str | None:
    return None


@pytest.fixture(scope="function", autouse=True)
def pageserver_virtual_file_io_engine() -> str | None:
    return os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE")


@pytest.fixture(scope="function", autouse=True)
def pageserver_virtual_file_io_mode() -> str | None:
    return os.getenv("PAGESERVER_VIRTUAL_FILE_IO_MODE")


@pytest.fixture(scope="function", autouse=True)
def pageserver_get_vectored_concurrent_io() -> str | None:
    return os.getenv("PAGESERVER_GET_VECTORED_CONCURRENT_IO")


def get_pageserver_default_tenant_config_compaction_algorithm() -> dict[str, Any] | None:
    toml_table = os.getenv("PAGESERVER_DEFAULT_TENANT_CONFIG_COMPACTION_ALGORITHM")
    if toml_table is None:
        return None
    v = toml.loads(toml_table)
    assert isinstance(v, dict)
    return v


@pytest.fixture(scope="function", autouse=True)
def pageserver_default_tenant_config_compaction_algorithm() -> dict[str, Any] | None:
    return get_pageserver_default_tenant_config_compaction_algorithm()


def pytest_generate_tests(metafunc: Metafunc):
    if (bt := os.getenv("BUILD_TYPE")) is None:
        build_types = ["debug", "release"]
    else:
        build_types = [bt.lower()]

    metafunc.parametrize("build_type", build_types)

    pg_versions: list[PgVersion]
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

    # Same hack for pageserver_default_tenant_config_compaction_algorithm
    if (
        explicit_default := get_pageserver_default_tenant_config_compaction_algorithm()
    ) is not None:
        metafunc.parametrize(
            "pageserver_default_tenant_config_compaction_algorithm",
            [explicit_default],
            ids=[explicit_default["kind"]],
        )

    # For performance tests, parametrize also by platform
    if (
        "test_runner/performance" in metafunc.definition._nodeid
        and (platform := os.getenv("PLATFORM")) is not None
    ):
        metafunc.parametrize("platform", [platform.lower()])


@pytest.hookimpl(hookwrapper=True, tryfirst=True)
def pytest_runtest_makereport(*args, **kwargs):
    # Add test parameters to Allue report to distinguish the same tests with different parameters.
    # Names has `__` prefix to avoid conflicts with `pytest.mark.parametrize` parameters

    # A mapping between `uname -m` and `RUNNER_ARCH` values.
    # `RUNNER_ARCH` environment variable is set on GitHub Runners,
    # possible values are X86, X64, ARM, or ARM64.
    # See https://docs.github.com/en/actions/learn-github-actions/variables#default-environment-variables
    uname_m = {
        "aarch64": "ARM64",
        "arm64": "ARM64",
        "x86_64": "X64",
    }.get(os.uname().machine, "UNKNOWN")
    arch = os.getenv("RUNNER_ARCH", uname_m)
    allure.dynamic.parameter("__arch", arch)
    allure.dynamic.parameter(
        "__lfc", "with-lfc" if os.getenv("USE_LFC") != "false" else "without-lfc"
    )
    allure.dynamic.parameter(
        "__sanitizers", "enabled" if os.getenv("SANITIZERS") == "enabled" else "disabled"
    )

    yield
