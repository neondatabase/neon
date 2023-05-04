import enum
import os
from typing import Iterator, Optional

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

    # Make it less confusing in logs
    def __repr__(self) -> str:
        return f"'{self.value}'"

    @classmethod
    def _missing_(cls, value) -> Optional["PgVersion"]:
        known_values = {v.value for _, v in cls.__members__.items()}

        # Allow passing version as a string with "v" prefix (e.g. "v14")
        if isinstance(value, str) and value.lower().startswith("v") and value[1:] in known_values:
            return cls(value[1:])
        # Allow passing version as an int (e.g. 15 or 150002, both will be converted to PgVersion.V15)
        elif isinstance(value, int) and str(value)[:2] in known_values:
            return cls(str(value)[:2])

        # Make mypy happy
        # See https://github.com/python/mypy/issues/3974
        return None


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
    elif v := os.environ.get("DEFAULT_PG_VERSION"):
        version, source = PgVersion(v), "from DEFAULT_PG_VERSION environment variable"
    else:
        version, source = DEFAULT_VERSION, "default verson"

    log.info(f"pg_version is {version} ({source})")
    yield version
