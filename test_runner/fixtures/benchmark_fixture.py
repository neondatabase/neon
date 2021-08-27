from pprint import pprint

import os
import timeit
import pathlib
import uuid
import psycopg2
import pytest
from _pytest.config import Config
from _pytest.runner import CallInfo
from _pytest.terminal import TerminalReporter
import shutil
import signal
import subprocess
import time

from contextlib import contextmanager
from contextlib import closing
from pathlib import Path
from dataclasses import dataclass

# Type-related stuff
from psycopg2.extensions import connection as PgConnection
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar, cast
from typing_extensions import Literal

from .utils import (get_self_dir, mkdir_if_needed, subprocess_capture)

"""
This file contains fixtures for micro-benchmarks.

To use, declare the 'zenbenchmark' fixture in the test function. Run the
bencmark, and then record the result by calling zenbenchmark.record. For example:

import timeit
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")

def test_mybench(postgres: PostgresFactory, pageserver: ZenithPageserver, zenbenchmark):

    # Initialize the test
    ...
    
    # Run the test, timing how long it takes
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT test_query(...)')

    # Record another measurement
    zenbenchmark.record('speed_of_light', 300000, 'km/s')


You can measure multiple things in one test, and record each one with a separate
call to zenbenchmark. For example, you could time the bulk loading that happens
in the test initialization, or measure disk usage after the test query.

"""


# All the results are collected in this list, as a tuple:
# (test_name: str, metric_name: str, metric_value: float, unit: str)
#
# TODO: It would perhaps be better to store the results as additional
# properties in the pytest TestReport objects, to make them visible to
# other pytest tools.
global zenbenchmark_results
zenbenchmark_results = []

class ZenithBenchmarkResults:
    """ An object for recording benchmark results. """
    def __init__(self):
        self.results = []

    def record(self, test_name: str, metric_name: str, metric_value: float, unit: str):
        """
        Record a benchmark result.
        """

        self.results.append((test_name, metric_name, metric_value, unit))

# Sesssion scope fixture that initializes the results object
@pytest.fixture(autouse=True, scope='session')
def zenbenchmark_global(request) -> Iterator[ZenithBenchmarkResults]:
    """
    This is a python decorator for benchmark fixtures
    """
    global zenbenchmark_results
    zenbenchmark_results = ZenithBenchmarkResults()

    yield zenbenchmark_results

class ZenithBenchmarker:
    """
    An object for recording benchmark results. This is created for each test
    function by the zenbenchmark fixture
    """
    def __init__(self, results, request):
        self.results = results
        self.request = request

    def record(self, metric_name: str, metric_value: float, unit: str):
        """
        Record a benchmark result.
        """
        self.results.record(self.request.node.name, metric_name, metric_value, unit)


    @contextmanager
    def record_duration(self, metric_name):
        """
        Record a duration. Usage:
        
        with zenbenchmark.record_duration('foobar_runtime'):
            foobar()   # measure this
        
        """
        start = timeit.default_timer()
        yield
        end = timeit.default_timer()

        self.results.record(self.request.node.name, metric_name, end - start, 's')

@pytest.fixture(scope='function')
def zenbenchmark(zenbenchmark_global, request) -> Iterator[ZenithBenchmarker]:
    """
    This is a python decorator for benchmark fixtures. It contains functions for
    recording measurements, and prints them out at the end.
    """
    benchmarker = ZenithBenchmarker(zenbenchmark_global, request)
    yield benchmarker


# Hook to print the results at the end
@pytest.hookimpl(hookwrapper=True)
def pytest_terminal_summary(
    terminalreporter: TerminalReporter, exitstatus: int, config: Config
):
    yield

    global zenbenchmark_results

    if not zenbenchmark_results:
        return

    terminalreporter.section('Benchmark results', "-")

    for result in zenbenchmark_results.results:
        func = result[0]
        metric_name = result[1]
        metric_value = result[2]
        unit = result[3]

        terminalreporter.write("{}.{}: ".format(func, metric_name))

        if unit == 'MB':
            terminalreporter.write("{0:,.0f}".format(metric_value), green=True)
        elif unit == 's':
            terminalreporter.write("{0:,.3f}".format(metric_value), green=True)
        else:
            terminalreporter.write("{0:,.4f}".format(metric_value), green=True)

        terminalreporter.line(" {}".format(unit))
