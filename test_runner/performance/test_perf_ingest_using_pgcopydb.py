import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import cast
from urllib.parse import urlparse

import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.utils import humantime_to_ms


def setup_environment():
    """Set up necessary environment variables for pgcopydb execution.

    Expects the following variables to be set in the environment:
    - PG_CONFIG: e.g. /tmp/neon/pg_install/v16/bin/pg_config
    - PSQL: e.g. /tmp/neon/pg_install/v16/bin/psql
    - PG_16_LIB_PATH: e.g. /tmp/neon/pg_install/v16/lib
    - PGCOPYDB: e.g. /pgcopydb/bin/pgcopydb
    - PGCOPYDB_LIB_PATH: e.g. /pgcopydb/lib
    - BENCHMARK_INGEST_SOURCE_CONNSTR
    - BENCHMARK_INGEST_TARGET_CONNSTR
    - PERF_TEST_RESULT_CONNSTR
    - TARGET_PROJECT_TYPE

    """
    # Ensure required environment variables are set
    required_env_vars = [
        "PGCOPYDB",
        "PGCOPYDB_LIB_PATH",
        "PG_CONFIG",
        "PSQL",
        "PG_16_LIB_PATH",
        "BENCHMARK_INGEST_SOURCE_CONNSTR",
        "BENCHMARK_INGEST_TARGET_CONNSTR",
        "PERF_TEST_RESULT_CONNSTR",
        "TARGET_PROJECT_TYPE",
    ]
    for var in required_env_vars:
        if not os.getenv(var):
            raise OSError(f"Required environment variable '{var}' is not set.")


def build_pgcopydb_command(pgcopydb_filter_file: Path, test_output_dir: Path):
    """Builds the pgcopydb command to execute using existing environment variables."""
    pgcopydb_executable = os.getenv("PGCOPYDB")
    if not pgcopydb_executable:
        raise OSError("PGCOPYDB environment variable is not set.")

    return [
        pgcopydb_executable,
        "clone",
        "--dir",
        str(test_output_dir),
        "--skip-vacuum",
        "--no-owner",
        "--no-acl",
        "--skip-db-properties",
        "--table-jobs",
        "4",
        "--index-jobs",
        "4",
        "--restore-jobs",
        "4",
        "--split-tables-larger-than",
        "10GB",
        "--skip-extensions",
        "--use-copy-binary",
        "--filters",
        str(pgcopydb_filter_file),
    ]


@pytest.fixture()  # must be function scoped because test_output_dir is function scoped
def pgcopydb_filter_file(test_output_dir: Path) -> Path:
    """Creates the pgcopydb_filter.txt file required by pgcopydb."""
    filter_content = textwrap.dedent("""\
        [include-only-table]
        public.events
        public.emails
        public.email_transmissions
        public.payments
        public.editions
        public.edition_modules
        public.sp_content
        public.email_broadcasts
        public.user_collections
        public.devices
        public.user_accounts
        public.lessons
        public.lesson_users
        public.payment_methods
        public.orders
        public.course_emails
        public.modules
        public.users
        public.module_users
        public.courses
        public.payment_gateway_keys
        public.accounts
        public.roles
        public.payment_gateways
        public.management
        public.event_names
        """)
    filter_path = test_output_dir / "pgcopydb_filter.txt"
    filter_path.write_text(filter_content)
    return filter_path


def get_backpressure_time(connstr):
    """Executes a query to get the backpressure throttling time in seconds."""
    query = "select backpressure_throttling_time()/1000000;"
    psql_path = os.getenv("PSQL")
    if psql_path is None:
        raise OSError("The PSQL environment variable is not set.")
    result = subprocess.run(
        [psql_path, connstr, "-t", "-c", query], capture_output=True, text=True, check=True
    )
    return float(result.stdout.strip())


def run_command_and_log_output(command, log_file_path: Path):
    """
    Runs a command and logs output to both a file and GitHub Actions console.

    Args:
        command (list): The command to execute.
        log_file_path (Path): Path object for the log file where output is written.
    """
    # Define a list of necessary environment variables for pgcopydb
    custom_env_vars = {
        "LD_LIBRARY_PATH": f"{os.getenv('PGCOPYDB_LIB_PATH')}:{os.getenv('PG_16_LIB_PATH')}",
        "PGCOPYDB_SOURCE_PGURI": cast(str, os.getenv("BENCHMARK_INGEST_SOURCE_CONNSTR")),
        "PGCOPYDB_TARGET_PGURI": cast(str, os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR")),
        "PGOPTIONS": "-c maintenance_work_mem=8388608 -c max_parallel_maintenance_workers=7",
    }
    # Combine the current environment with custom variables
    env = os.environ.copy()
    env.update(custom_env_vars)

    with log_file_path.open("w") as log_file:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env
        )

        assert process.stdout is not None, "process.stdout should not be None"

        # Stream output to both log file and console
        for line in process.stdout:
            print(line, end="")  # Stream to GitHub Actions log
            sys.stdout.flush()
            log_file.write(line)  # Write to log file

        process.wait()  # Wait for the process to finish
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)


def parse_log_and_report_metrics(
    zenbenchmark: NeonBenchmarker, log_file_path: Path, backpressure_time_diff: float
):
    """Parses the pgcopydb log file for performance metrics and reports them to the database."""
    metrics = {"backpressure_time": backpressure_time_diff}

    # Define regex patterns to capture metrics
    metric_patterns = {
        "COPY_INDEX_CONSTRAINTS_VACUUM": re.compile(
            r"COPY, INDEX, CONSTRAINTS, VACUUM \(wall clock\).*"
        ),
        "COPY_CUMULATIVE": re.compile(r"COPY \(cumulative\).*"),
        "CREATE_INDEX_CUMULATIVE": re.compile(r"CREATE INDEX \(cumulative\).*"),
        "CONSTRAINTS_CUMULATIVE": re.compile(r"CONSTRAINTS \(cumulative\).*"),
        "FINALIZE_SCHEMA": re.compile(r"Finalize Schema.*"),
        "TOTAL_DURATION": re.compile(r"Total Wall Clock Duration.*"),
    }

    # Parse log file
    with log_file_path.open("r") as log_file:
        for line in log_file:
            for metric_name, pattern in metric_patterns.items():
                if pattern.search(line):
                    # Extract duration and convert it to seconds
                    duration_match = re.search(r"\d+h\d+m|\d+s|\d+ms|\d+\.\d+s", line)
                    if duration_match:
                        duration_str = duration_match.group(0)
                        parts = re.findall(r"\d+[a-zA-Z]+", duration_str)
                        rust_like_humantime = " ".join(parts)
                        duration_seconds = humantime_to_ms(rust_like_humantime) / 1000.0
                        metrics[metric_name] = duration_seconds

    endpoint_id = {"endpoint_id": get_endpoint_id()}
    for metric_name, duration_seconds in metrics.items():
        zenbenchmark.record(
            metric_name, duration_seconds, "s", MetricReport.LOWER_IS_BETTER, endpoint_id
        )


def get_endpoint_id():
    """Extracts and returns the first segment of the hostname from the PostgreSQL URI stored in BENCHMARK_INGEST_TARGET_CONNSTR."""
    connstr = os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR")
    if connstr is None:
        raise OSError("BENCHMARK_INGEST_TARGET_CONNSTR environment variable is not set.")

    # Parse the URI
    parsed_url = urlparse(connstr)

    # Extract the hostname and split to get the first segment
    hostname = parsed_url.hostname
    if hostname is None:
        raise ValueError("Unable to parse hostname from BENCHMARK_INGEST_TARGET_CONNSTR")

    # Split the hostname by dots and take the first segment
    endpoint_id = hostname.split(".")[0]

    return endpoint_id


@pytest.fixture()  # must be function scoped because test_output_dir is function scoped
def log_file_path(test_output_dir):
    """Fixture to provide a temporary log file path."""
    if not os.getenv("TARGET_PROJECT_TYPE"):
        raise OSError("Required environment variable 'TARGET_PROJECT_TYPE' is not set.")
    return (test_output_dir / os.getenv("TARGET_PROJECT_TYPE")).with_suffix(".log")


@pytest.mark.remote_cluster
def test_ingest_performance_using_pgcopydb(
    zenbenchmark: NeonBenchmarker,
    log_file_path: Path,
    pgcopydb_filter_file: Path,
    test_output_dir: Path,
):
    """
    Simulate project migration from another PostgreSQL provider to Neon.

    Measure performance for Neon ingest steps
    - COPY
    - CREATE INDEX
    - CREATE CONSTRAINT
    - VACUUM ANALYZE
    - create foreign keys

    Use pgcopydb to copy data from the source database to the destination database.
    """
    # Set up environment and create filter file
    setup_environment()

    # Get backpressure time before ingest
    backpressure_time_before = get_backpressure_time(os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR"))

    # Build and run the pgcopydb command
    command = build_pgcopydb_command(pgcopydb_filter_file, test_output_dir)
    try:
        run_command_and_log_output(command, log_file_path)
    except subprocess.CalledProcessError as e:
        pytest.fail(f"pgcopydb command failed with error: {e}")

    # Get backpressure time after ingest and calculate the difference
    backpressure_time_after = get_backpressure_time(os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR"))
    backpressure_time_diff = backpressure_time_after - backpressure_time_before

    # Parse log file and report metrics, including backpressure time difference
    parse_log_and_report_metrics(zenbenchmark, log_file_path, backpressure_time_diff)
