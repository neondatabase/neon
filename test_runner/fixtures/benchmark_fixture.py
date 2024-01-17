import calendar
import dataclasses
import enum
import json
import os
import re
import timeit
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

# Type-related stuff
from typing import Callable, ClassVar, Dict, Iterator, Optional

import pytest
from _pytest.config import Config
from _pytest.config.argparsing import Parser
from _pytest.terminal import TerminalReporter

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonPageserver
from fixtures.types import TenantId, TimelineId

"""
This file contains fixtures for micro-benchmarks.

To use, declare the `zenbenchmark` fixture in the test function. Run the
bencmark, and then record the result by calling `zenbenchmark.record`. For example:

>>> import timeit
>>> from fixtures.neon_fixtures import NeonEnv
>>> def test_mybench(neon_simple_env: NeonEnv, zenbenchmark):
...     # Initialize the test
...     ...
...     # Run the test, timing how long it takes
...     with zenbenchmark.record_duration('test_query'):
...         cur.execute('SELECT test_query(...)')
...     # Record another measurement
...     zenbenchmark.record('speed_of_light', 300000, 'km/s')

There's no need to import this file to use it. It should be declared as a plugin
inside `conftest.py`, and that makes it available to all tests.

You can measure multiple things in one test, and record each one with a separate
call to `zenbenchmark`. For example, you could time the bulk loading that happens
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
    scale: int

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
        for line in stdout_lines:
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
            if line.startswith("tps = ") and (
                "(excluding connections establishing)" in line
                or "(without initial connection time)" in line
            ):
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
            scale=scale,
        )


@dataclasses.dataclass
class PgBenchInitResult:
    # Taken from https://github.com/postgres/postgres/blob/REL_15_1/src/bin/pgbench/pgbench.c#L5144-L5171
    EXTRACTORS: ClassVar[Dict[str, re.Pattern]] = {  # type: ignore[type-arg]
        "drop_tables": re.compile(r"drop tables (\d+\.\d+) s"),
        "create_tables": re.compile(r"create tables (\d+\.\d+) s"),
        "client_side_generate": re.compile(r"client-side generate (\d+\.\d+) s"),
        "server_side_generate": re.compile(r"server-side generate (\d+\.\d+) s"),
        "vacuum": re.compile(r"vacuum (\d+\.\d+) s"),
        "primary_keys": re.compile(r"primary keys (\d+\.\d+) s"),
        "foreign_keys": re.compile(r"foreign keys (\d+\.\d+) s"),
        "total": re.compile(r"done in (\d+\.\d+) s"),  # Total time printed by pgbench
    }

    total: Optional[float]
    drop_tables: Optional[float]
    create_tables: Optional[float]
    client_side_generate: Optional[float]
    server_side_generate: Optional[float]
    vacuum: Optional[float]
    primary_keys: Optional[float]
    foreign_keys: Optional[float]
    duration: float
    start_timestamp: int
    end_timestamp: int

    @classmethod
    def parse_from_stderr(
        cls,
        stderr: str,
        duration: float,
        start_timestamp: int,
        end_timestamp: int,
    ):
        # Parses pgbench initialize output
        # Example: done in 5.66 s (drop tables 0.05 s, create tables 0.31 s, client-side generate 2.01 s, vacuum 0.53 s, primary keys 0.38 s).

        last_line = stderr.splitlines()[-1]

        timings: Dict[str, Optional[float]] = {}
        last_line_items = re.split(r"\(|\)|,", last_line)
        for item in last_line_items:
            for key, regex in cls.EXTRACTORS.items():
                if (m := regex.match(item.strip())) is not None:
                    if key in timings:
                        raise RuntimeError(
                            f"can't store pgbench results for repeated action `{key}`"
                        )

                    timings[key] = float(m.group(1))

        if not timings or "total" not in timings:
            raise RuntimeError(f"can't parse pgbench initialize results from `{last_line}`")

        return cls(
            total=timings["total"],
            drop_tables=timings.get("drop_tables", 0.0),
            create_tables=timings.get("create_tables", 0.0),
            client_side_generate=timings.get("client_side_generate", 0.0),
            server_side_generate=timings.get("server_side_generate", 0.0),
            vacuum=timings.get("vacuum", 0.0),
            primary_keys=timings.get("primary_keys", 0.0),
            foreign_keys=timings.get("foreign_keys", 0.0),
            duration=duration,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )


@enum.unique
class MetricReport(str, enum.Enum):  # str is a hack to make it json serializable
    # this means that this is a constant test parameter
    # like number of transactions, or number of clients
    TEST_PARAM = "test_param"
    # reporter can use it to mark test runs with higher values as improvements
    HIGHER_IS_BETTER = "higher_is_better"
    # the same but for lower values
    LOWER_IS_BETTER = "lower_is_better"


class NeonBenchmarker:
    """
    An object for recording benchmark results. This is created for each test
    function by the zenbenchmark fixture
    """

    def __init__(self, property_recorder: Callable[[str, object], None]):
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
                "report": report,
            },
        )

    @contextmanager
    def record_duration(self, metric_name: str) -> Iterator[None]:
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
        self.record(
            f"{prefix}.number_of_clients",
            pg_bench_result.number_of_clients,
            "",
            MetricReport.TEST_PARAM,
        )
        self.record(
            f"{prefix}.number_of_threads",
            pg_bench_result.number_of_threads,
            "",
            MetricReport.TEST_PARAM,
        )
        self.record(
            f"{prefix}.number_of_transactions_actually_processed",
            pg_bench_result.number_of_transactions_actually_processed,
            "",
            # that's because this is predefined by test matrix and doesn't change across runs
            report=MetricReport.TEST_PARAM,
        )
        self.record(
            f"{prefix}.latency_average",
            pg_bench_result.latency_average,
            unit="ms",
            report=MetricReport.LOWER_IS_BETTER,
        )
        if pg_bench_result.latency_stddev is not None:
            self.record(
                f"{prefix}.latency_stddev",
                pg_bench_result.latency_stddev,
                unit="ms",
                report=MetricReport.LOWER_IS_BETTER,
            )
        self.record(f"{prefix}.tps", pg_bench_result.tps, "", report=MetricReport.HIGHER_IS_BETTER)
        self.record(
            f"{prefix}.run_duration",
            pg_bench_result.run_duration,
            unit="s",
            report=MetricReport.LOWER_IS_BETTER,
        )
        self.record(
            f"{prefix}.run_start_timestamp",
            pg_bench_result.run_start_timestamp,
            "",
            MetricReport.TEST_PARAM,
        )
        self.record(
            f"{prefix}.run_end_timestamp",
            pg_bench_result.run_end_timestamp,
            "",
            MetricReport.TEST_PARAM,
        )
        self.record(
            f"{prefix}.scale",
            pg_bench_result.scale,
            "",
            MetricReport.TEST_PARAM,
        )

    def record_pg_bench_init_result(self, prefix: str, result: PgBenchInitResult):
        test_params = [
            "start_timestamp",
            "end_timestamp",
        ]
        for test_param in test_params:
            self.record(
                f"{prefix}.{test_param}", getattr(result, test_param), "", MetricReport.TEST_PARAM
            )

        metrics = [
            "duration",
            "drop_tables",
            "create_tables",
            "client_side_generate",
            "server_side_generate",
            "vacuum",
            "primary_keys",
            "foreign_keys",
        ]
        for metric in metrics:
            if (value := getattr(result, metric)) is not None:
                self.record(
                    f"{prefix}.{metric}", value, unit="s", report=MetricReport.LOWER_IS_BETTER
                )

    def get_io_writes(self, pageserver: NeonPageserver) -> int:
        """
        Fetch the "cumulative # of bytes written" metric from the pageserver
        """
        return self.get_int_counter_value(
            pageserver, "libmetrics_disk_io_bytes_total", {"io_operation": "write"}
        )

    def get_peak_mem(self, pageserver: NeonPageserver) -> int:
        """
        Fetch the "maxrss" metric from the pageserver
        """
        return self.get_int_counter_value(pageserver, "libmetrics_maxrss_kb")

    def get_int_counter_value(
        self,
        pageserver: NeonPageserver,
        metric_name: str,
        label_filters: Optional[Dict[str, str]] = None,
    ) -> int:
        """Fetch the value of given int counter from pageserver metrics."""
        all_metrics = pageserver.http_client().get_metrics()
        sample = all_metrics.query_one(metric_name, label_filters)
        return int(round(sample.value))

    def get_timeline_size(
        self, repo_dir: Path, tenant_id: TenantId, timeline_id: TimelineId
    ) -> int:
        """
        Calculate the on-disk size of a timeline
        """
        path = f"{repo_dir}/tenants/{tenant_id}/timelines/{timeline_id}"

        totalbytes = 0
        for root, _dirs, files in os.walk(path):
            for name in files:
                totalbytes += os.path.getsize(os.path.join(root, name))

        return totalbytes

    @contextmanager
    def record_pageserver_writes(
        self, pageserver: NeonPageserver, metric_name: str
    ) -> Iterator[None]:
        """
        Record bytes written by the pageserver during a test.
        """
        before = self.get_io_writes(pageserver)
        yield
        after = self.get_io_writes(pageserver)

        self.record(
            metric_name,
            round((after - before) / (1024 * 1024)),
            "MB",
            report=MetricReport.LOWER_IS_BETTER,
        )


@pytest.fixture(scope="function")
def zenbenchmark(record_property: Callable[[str, object], None]) -> Iterator[NeonBenchmarker]:
    """
    This is a python decorator for benchmark fixtures. It contains functions for
    recording measurements, and prints them out at the end.
    """
    benchmarker = NeonBenchmarker(record_property)
    yield benchmarker


def pytest_addoption(parser: Parser):
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
def pytest_terminal_summary(
    terminalreporter: TerminalReporter, exitstatus: int, config: Config
) -> Iterator[None]:
    yield
    revision = os.getenv("GITHUB_SHA", "local")
    platform = os.getenv("PLATFORM", "local")

    is_header_printed = False

    result = []
    for test_report in terminalreporter.stats.get("passed", []):
        result_entry = []

        for _, recorded_property in test_report.user_properties:
            if not is_header_printed:
                terminalreporter.section("Benchmark results", "-")
                is_header_printed = True

            terminalreporter.write(
                "{}.{}: ".format(test_report.head_line, recorded_property["name"])
            )
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

        result.append(
            {
                "suit": test_report.nodeid,
                "total_duration": test_report.duration,
                "data": result_entry,
            }
        )

    out_dir = config.getoption("out_dir")
    if out_dir is None:
        return

    if not result:
        log.warning("no results to store (no passed test suites)")
        return

    get_out_path(Path(out_dir), revision=revision).write_text(
        json.dumps({"revision": revision, "platform": platform, "result": result}, indent=4)
    )
