import os

import pytest

from fixtures.pg_version import DEFAULT_VERSION, PgVersion

"""
Set of utilities to make Allure report more informative.

- It adds BUILD_TYPE and DEFAULT_PG_VERSION to the test names (only in test_runner/regress)
to make tests distinguishable in Allure report.
"""


@pytest.fixture(scope="function", autouse=True)
def allure_noop():
    pass


def pytest_generate_tests(metafunc):
    if "test_runner/regress" in metafunc.definition._nodeid:
        build_type = os.environ.get("BUILD_TYPE", "DEBUG").lower()
        pg_version = PgVersion(os.environ.get("DEFAULT_PG_VERSION", DEFAULT_VERSION))

        metafunc.parametrize("allure_noop", [f"{build_type}-pg{pg_version}"])
