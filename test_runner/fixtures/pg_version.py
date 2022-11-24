import enum
import os
from typing import Iterator

import pytest
from _pytest.config.argparsing import Parser
from pytest import FixtureRequest

from fixtures.log_helper import log

"""
This fixture is used to determine which version of Postgres to use for tests.
"""


# Inherit PgVersion from str rather than int to make it easier to pass as a command-line argument
# TODO: use enum.StrEnum for Python >= 3.11
@enum.unique
class PgVersion(str, enum.Enum):
    V14 = "14"
    V15 = "15"
    # Instead of making version an optional parameter in methods, we can use this fake entry
    # to explicitly rely on the default server version (could be different from pg_version fixture value)
    NOT_SET = "<-POSTRGRES VERSION IS NOT SET->"

    @staticmethod
    def from_int(int_version: int) -> "PgVersion":
        return PgVersion(str(int_version)[:2])

    # Make it less confusing in logs
    def __repr__(self) -> str:
        return f"'{self.value}'"


DEFAULT_VERSION: PgVersion = PgVersion.V14


def pytest_addoption(parser: Parser):
    parser.addoption(
        "--pg-version",
        action="store",
        type=PgVersion,
        help="Postgres version to use for tests",
    )


@pytest.fixture(scope="session")
def pg_version(request: FixtureRequest) -> Iterator[PgVersion]:
    if v := request.config.getoption("--pg-version"):
        version, source = v, "from --pg-version commad-line argument"
    elif v := os.environ.get("PG_VERSION"):
        version, source = PgVersion(v), "from PG_VERSION environment variable"
    else:
        version, source = DEFAULT_VERSION, "default verson"

    log.info(f"pg_version is {version} ({source})")
    yield version
