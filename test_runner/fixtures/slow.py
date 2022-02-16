import pytest
"""
This plugin allows tests to be marked as slow using pytest.mark.slow. By default slow
tests are excluded. They need to be specifically requested with the --runslow flag in
order to run.

Copied from here: https://docs.pytest.org/en/latest/example/simple.html
"""


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
