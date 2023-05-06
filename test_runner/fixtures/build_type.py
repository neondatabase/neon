import os

import pytest

"""
Add BUILD_TYPE as a parameter to all tests in test_runner/regress,
to make the distunquishable in unified Allure report.

First use noop() fixture to autouse it in all tests.
Then add BUILD_TYPE parameter for this fixture.
"""


@pytest.fixture(scope="function", autouse=True)
def noop():
    pass


def pytest_generate_tests(metafunc):
    if "test_runner/regress" in metafunc.definition._nodeid:
        metafunc.parametrize("noop", [os.environ.get("BUILD_TYPE", "DEBUG").lower()])
