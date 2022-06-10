import dataclasses
import json
import os
from pathlib import Path
import re
import subprocess
import timeit
import calendar
import enum
from datetime import datetime
import uuid
import pytest
from _pytest.config import Config
from _pytest.terminal import TerminalReporter
import warnings

from contextlib import contextmanager

# Type-related stuff
from typing import Iterator, Optional
"""
This file contains fixtures for micro-benchmarks.

To use, declare the 'zenbenchmark' fixture in the test function. Run the
bencmark, and then record the result by calling zenbenchmark.record. For example:

import timeit
from fixtures.neon_fixtures import NeonEnv

def test_mybench(neon_simple_env: env, zenbenchmark):

    # Initialize the test
    ...

    # Run the test, timing how long it takes
    with zenbenchmark.record_duration('test_query'):
        cur.execute('SELECT test_query(...)')

    # Record another measurement
    zenbenchmark.record('speed_of_light', 300000, 'km/s')

There's no need to import this file to use it. It should be declared as a plugin
inside conftest.py, and that makes it available to all tests.

You can measure multiple things in one test, and record each one with a separate
call to zenbenchmark. For example, you could time the bulk loading that happens
in the test initialization, or measure disk usage after the test query.

"""


@dataclasses.dataclass
class PgBenchRunResult:
    number_of_clients: int
    number_of_threads: int
    number_of_transactions_actually_processed: int
    latency_average: float
    latency_stddev: Optional[float]
    tps: float
    run_duration: float
    run_start_timestamp: int
    run_end_timestamp: int

    # TODO progress

    @classmethod
    def parse_from_stdout(
        cls,
        stdout: str,
        run_duration: float,
        run_start_timestamp: int,
        run_end_timestamp: int,
    ):
        stdout_lines = stdout.splitlines()

        latency_stddev = None

        # we know significant parts of these values from test input
        # but to be precise take them from output
        for line in stdout.splitlines():
            # scaling factor: 5
            if line.startswith("scaling factor:"):
                scale = int(line.split()[-1])
            # number of clients: 1
            if line.startswith("number of clients: "):
                number_of_clients = int(line.split()[-1])
            # number of threads: 1
            if line.startswith("number of threads: "):
                number_of_threads = int(line.split()[-1])
            # number of transactions actually processed: 1000/1000
            # OR
            # number of transactions actually processed: 1000
            if line.startswith("number of transactions actually processed"):
                if "/" in line:
                    number_of_transactions_actually_processed = int(line.split("/")[1])
                else:
                    number_of_transactions_actually_processed = int(line.split()[-1])
            # latency average = 19.894 ms
            if line.startswith("latency average"):
                latency_average = float(line.split()[-2])
            # latency stddev = 3.387 ms
            # (only printed with some options)
            if line.startswith("latency stddev"):
                latency_stddev = float(line.split()[-2])

            # Get the TPS without initial connection time. The format
            # of the tps lines changed in pgbench v14, but we accept
            # either format:
            #
            # pgbench v13 and below:
            # tps = 50.219689 (including connections establishing)
            # tps = 50.264435 (excluding connections establishing)
            #
            # pgbench v14:
            # initial connection time = 3.858 ms
            # tps = 309.281539 (without initial connection time)
            if (line.startswith("tps = ") and ("(excluding connections establishing)" in line
                                               or "(without initial connection time)")):
                tps = float(line.split()[2])

        return cls(
            number_of_clients=number_of_clients,
            number_of_threads=number_of_threads,
            number_of_transactions_actually_processed=number_of_transactions_actually_processed,
            latency_average=latency_average,
            latency_stddev=latency_stddev,
            tps=tps,
            run_duration=run_duration,
            run_start_timestamp=run_start_timestamp,
            run_end_timestamp=run_end_timestamp,
        )


@enum.unique
class MetricReport(str, enum.Enum):  # str is a hack to make it json serializable
    # this means that this is a constant test parameter
    # like number of transactions, or number of clients
    TEST_PARAM = 'test_param'
    # reporter can use it to mark test runs with higher values as improvements
    HIGHER_IS_BETTER = 'higher_is_better'
    # the same but for lower values
    LOWER_IS_BETTER = 'lower_is_better'


class NeonBenchmarker:
    """
    An object for recording benchmark results. This is created for each test
    function by the zenbenchmark fixture
    """
    def __init__(self, property_recorder):
        # property recorder here is a pytest fixture provided by junitxml module
        # https://docs.pytest.org/en/6.2.x/reference.html#pytest.junitxml.record_property
        self.property_recorder = property_recorder

    def record(
        self,
        metric_name: str,
        metric_value: float,
        unit: str,
        report: MetricReport,
    ):
        """
        Record a benchmark result.
        """
        # just to namespace the value
        name = f"neon_benchmarker_{metric_name}"
        self.property_recorder(
            name,
            {
                "name": metric_name,
                "value": metric_value,
                "unit": unit,
                "report": str(report),
            },
        )

    @contextmanager
    def record_duration(self, metric_name: str):
        """
        Record a duration. Usage:

        with zenbenchmark.record_duration('foobar_runtime'):
            foobar()   # measure this
        """
        start = timeit.default_timer()
        yield
        end = timeit.default_timer()

        self.record(
            metric_name=metric_name,
            metric_value=end - start,
            unit="s",
            report=MetricReport.LOWER_IS_BETTER,
        )

    def record_pg_bench_result(self, prefix: str, pg_bench_result: PgBenchRunResult):
        self.record(f"{prefix}.number_of_clients",
                    pg_bench_result.number_of_clients,
                    '',
                    MetricReport.TEST_PARAM)
        self.record(f"{prefix}.number_of_threads",
                    pg_bench_result.number_of_threads,
                    '',
                    MetricReport.TEST_PARAM)
        self.record(
            f"{prefix}.number_of_transactions_actually_processed",
            pg_bench_result.number_of_transactions_actually_processed,
            '',
            # that's because this is predefined by test matrix and doesn't change across runs
            report=MetricReport.TEST_PARAM,
        )
        self.record(f"{prefix}.latency_average",
                    pg_bench_result.latency_average,
                    unit="ms",
                    report=MetricReport.LOWER_IS_BETTER)
        if pg_bench_result.latency_stddev is not None:
            self.record(f"{prefix}.latency_stddev",
                        pg_bench_result.latency_stddev,
                        unit="ms",
                        report=MetricReport.LOWER_IS_BETTER)
        self.record(f"{prefix}.tps", pg_bench_result.tps, '', report=MetricReport.HIGHER_IS_BETTER)
        self.record(f"{prefix}.run_duration",
                    pg_bench_result.run_duration,
                    unit="s",
                    report=MetricReport.LOWER_IS_BETTER)
        self.record(f"{prefix}.run_start_timestamp",
                    pg_bench_result.run_start_timestamp,
                    '',
                    MetricReport.TEST_PARAM)
        self.record(f"{prefix}.run_end_timestamp",
                    pg_bench_result.run_end_timestamp,
                    '',
                    MetricReport.TEST_PARAM)

    def get_io_writes(self, pageserver) -> int:
        """
        Fetch the "cumulative # of bytes written" metric from the pageserver
        """
        metric_name = r'libmetrics_disk_io_bytes_total{io_operation="write"}'
        return self.get_int_counter_value(pageserver, metric_name)

    def get_peak_mem(self, pageserver) -> int:
        """
        Fetch the "maxrss" metric from the pageserver
        """
        metric_name = r'libmetrics_maxrss_kb'
        return self.get_int_counter_value(pageserver, metric_name)

    def get_int_counter_value(self, pageserver, metric_name) -> int:
        """Fetch the value of given int counter from pageserver metrics."""
        # TODO: If we start to collect more of the prometheus metrics in the
        # performance test suite like this, we should refactor this to load and
        # parse all the metrics into a more convenient structure in one go.
        #
        # The metric should be an integer, as it's a number of bytes. But in general
        # all prometheus metrics are floats. So to be pedantic, read it as a float
        # and round to integer.
        all_metrics = pageserver.http_client().get_metrics()
        matches = re.search(fr'^{metric_name} (\S+)$', all_metrics, re.MULTILINE)
        assert matches
        return int(round(float(matches.group(1))))

    def get_timeline_size(self, repo_dir: Path, tenantid: uuid.UUID, timelineid: str):
        """
        Calculate the on-disk size of a timeline
        """
        path = "{}/tenants/{}/timelines/{}".format(repo_dir, tenantid.hex, timelineid)

        totalbytes = 0
        for root, dirs, files in os.walk(path):
            for name in files:
                totalbytes += os.path.getsize(os.path.join(root, name))

        return totalbytes

    @contextmanager
    def record_pageserver_writes(self, pageserver, metric_name):
        """
        Record bytes written by the pageserver during a test.
        """
        before = self.get_io_writes(pageserver)
        yield
        after = self.get_io_writes(pageserver)

        self.record(metric_name,
                    round((after - before) / (1024 * 1024)),
                    "MB",
                    report=MetricReport.LOWER_IS_BETTER)


@pytest.fixture(scope="function")
def zenbenchmark(record_property) -> Iterator[NeonBenchmarker]:
    """
    This is a python decorator for benchmark fixtures. It contains functions for
    recording measurements, and prints them out at the end.
    """
    benchmarker = NeonBenchmarker(record_property)
    yield benchmarker


def pytest_addoption(parser):
    parser.addoption(
        "--out-dir",
        dest="out_dir",
        help="Directory to output performance tests results to.",
    )


def get_out_path(target_dir: Path, revision: str) -> Path:
    """
    get output file path
    if running in the CI uses commit revision
    to avoid duplicates uses counter
    """
    # use UTC timestamp as a counter marker to avoid weird behaviour
    # when for example files are deleted
    ts = calendar.timegm(datetime.utcnow().utctimetuple())
    path = target_dir / f"{ts}_{revision}.json"
    assert not path.exists()
    return path


# Hook to print the results at the end
@pytest.hookimpl(hookwrapper=True)
def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus: int, config: Config):
    yield
    revision = os.getenv("GITHUB_SHA", "local")
    platform = os.getenv("PLATFORM", "local")

    terminalreporter.section("Benchmark results", "-")

    result = []
    for test_report in terminalreporter.stats.get("passed", []):
        result_entry = []

        for _, recorded_property in test_report.user_properties:
            terminalreporter.write("{}.{}: ".format(test_report.head_line,
                                                    recorded_property["name"]))
            unit = recorded_property["unit"]
            value = recorded_property["value"]
            if unit == "MB":
                terminalreporter.write("{0:,.0f}".format(value), green=True)
            elif unit in ("s", "ms") and isinstance(value, float):
                terminalreporter.write("{0:,.3f}".format(value), green=True)
            elif isinstance(value, float):
                terminalreporter.write("{0:,.4f}".format(value), green=True)
            else:
                terminalreporter.write(str(value), green=True)
            terminalreporter.line(" {}".format(unit))

            result_entry.append(recorded_property)

        result.append({
            "suit": test_report.nodeid,
            "total_duration": test_report.duration,
            "data": result_entry,
        })

    out_dir = config.getoption("out_dir")
    if out_dir is None:
        warnings.warn("no out dir provided to store performance test results")
        return

    if not result:
        warnings.warn("no results to store (no passed test suites)")
        return

    get_out_path(Path(out_dir), revision=revision).write_text(
        json.dumps({
            "revision": revision, "platform": platform, "result": result
        }, indent=4))
