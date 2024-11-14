import os
import re
import subprocess
from datetime import datetime
from pathlib import Path

import psycopg2
import pytest
from typing import cast
from urllib.parse import urlparse


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
    - PGCOPYDB_LOG_FILE_NAME
    - COMMIT_HASH

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
        "PGCOPYDB_LOG_FILE_NAME",
        "COMMIT_HASH",
    ]
    for var in required_env_vars:
        if not os.getenv(var):
            raise OSError(f"Required environment variable '{var}' is not set.")

    # Set additional environment variables for the pgcopydb command
    os.environ["LD_LIBRARY_PATH"] = (
        f"{os.getenv('PGCOPYDB_LIB_PATH')}:{os.getenv('PG_16_LIB_PATH')}"
    )
    os.environ["PGCOPYDB_SOURCE_PGURI"] = cast(str, os.getenv("BENCHMARK_INGEST_SOURCE_CONNSTR"))
    os.environ["PGCOPYDB_TARGET_PGURI"] = cast(str, os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR"))
    os.environ["PGOPTIONS"] = (
        "-c maintenance_work_mem=8388608 -c max_parallel_maintenance_workers=7"
    )


def build_pgcopydb_command():
    """Builds the pgcopydb command to execute using existing environment variables."""
    pgcopydb_executable = os.getenv("PGCOPYDB")
    if not pgcopydb_executable:
        raise OSError("PGCOPYDB environment variable is not set.")

    return [
        pgcopydb_executable,
        "clone",
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
        "/tmp/pgcopydb_filter.txt",
    ]


def create_pgcopydb_filter_file():
    """Creates the /tmp/pgcopydb_filter.txt file required by pgcopydb."""
    filter_content = """\
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
"""
    filter_path = Path("/tmp/pgcopydb_filter.txt")
    filter_path.write_text(filter_content)


def get_backpressure_time(connstr):
    """Executes a query to get the backpressure throttling time in seconds."""
    query = "select backpressure_throttling_time()/1000000;"
    psql_path = os.getenv("PSQL")
    if psql_path is None:
        raise EnvironmentError("The PSQL environment variable is not set.")
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
    with log_file_path.open("w") as log_file:
        process = subprocess.Popen(
            command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )

        assert process.stdout is not None, "process.stdout should not be None"

        # Stream output to both log file and console
        for line in process.stdout:
            print(line, end="")  # Stream to GitHub Actions log
            log_file.write(line)  # Write to log file

        process.wait()  # Wait for the process to finish
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, command)


def convert_to_seconds(duration_str):
    """
    Converts a duration string with hours, minutes, seconds, and milliseconds
    (e.g., '2h47m3s044ms') to total seconds as a floating-point number.

    Args:
        duration_str (str): Duration string containing 'h', 'm', 's', 'ms' components.

    Returns:
        float: Total duration in seconds.
    """
    total_seconds = 0.0

    # Regex patterns for hours, minutes, seconds, and milliseconds
    hours_pattern = re.compile(r"([0-9]+)h")
    minutes_pattern = re.compile(r"([0-9]+)m")
    seconds_pattern = re.compile(r"([0-9]+)s")
    milliseconds_pattern = re.compile(r"([0-9]+)ms")

    # Convert and add milliseconds (ms) first to avoid double-counting with 'm'
    ms_match = milliseconds_pattern.search(duration_str)
    if ms_match:
        milliseconds = int(ms_match.group(1))
        total_seconds += milliseconds / 1000.0
        # Remove 'ms' part from the string to prevent double-counting
        duration_str = duration_str.replace(ms_match.group(0), "")

    # Convert and add hours
    h_match = hours_pattern.search(duration_str)
    if h_match:
        hours = int(h_match.group(1))
        total_seconds += hours * 3600

    # Convert and add minutes
    m_match = minutes_pattern.search(duration_str)
    if m_match:
        minutes = int(m_match.group(1))
        total_seconds += minutes * 60

    # Convert and add seconds
    s_match = seconds_pattern.search(duration_str)
    if s_match:
        seconds = int(s_match.group(1))
        total_seconds += seconds

    return total_seconds


def parse_log_and_report_metrics(log_file_path: Path, backpressure_time_diff: float):
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
                        duration_seconds = convert_to_seconds(duration_str)
                        metrics[metric_name] = duration_seconds

    # Report metrics to the database
    report_metrics_to_db(metrics)

def get_endpoint_id():
    """Extracts and returns the first segment of the hostname from the PostgreSQL URI stored in BENCHMARK_INGEST_TARGET_CONNSTR."""
    connstr = os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR")
    if connstr is None:
        raise EnvironmentError("BENCHMARK_INGEST_TARGET_CONNSTR environment variable is not set.")
    
    # Parse the URI
    parsed_url = urlparse(connstr)
    
    # Extract the hostname and split to get the first segment
    hostname = parsed_url.hostname
    if hostname is None:
        raise ValueError("Unable to parse hostname from BENCHMARK_INGEST_TARGET_CONNSTR")
    
    # Split the hostname by dots and take the first segment
    endpoint_id = hostname.split('.')[0]
    
    return endpoint_id


def report_metrics_to_db(metrics):
    """Inserts parsed metrics into the performance database."""
    # Connection string for the performance database
    connstr = os.getenv("PERF_TEST_RESULT_CONNSTR")
    if not connstr:
        raise OSError("PERF_TEST_RESULT_CONNSTR environment variable is not set.")
    commit_hash = os.getenv("COMMIT_HASH")
    if not commit_hash:
        raise OSError("COMMIT_HASH environment variable is not set.")
    endpoint_id = get_endpoint_id()

    # Connect to the database
    with psycopg2.connect(connstr) as conn:
        with conn.cursor() as cur:
            for metric_name, metric_value in metrics.items():
                cur.execute(
                    """
                    INSERT INTO public.perf_test_results (suit, revision, platform, metric_name, metric_value, metric_unit, metric_report_type, recorded_at_timestamp, label_1)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        "pgcopydb_ingest_bench_test",  # Suit
                        commit_hash,  # Revision (example value, replace as needed)
                        "pg16-ingest-bench",  # Platform (example value, replace as needed)
                        metric_name,  # Metric name
                        metric_value,  # Metric value (in seconds)
                        "seconds",  # Metric unit
                        "lower_is_better",  # Metric report type
                        datetime.now(),  # Recorded timestamp
                        endpoint_id,  # Label 1
                    ),
                )
            conn.commit()


@pytest.fixture
def log_file_path(tmp_path):
    """Fixture to provide a temporary log file path."""
    if not os.getenv("PGCOPYDB_LOG_FILE_NAME"):
        raise OSError(
            "Required environment variable 'PGCOPYDB_LOG_FILE_NAME' is not set."
        )
    return tmp_path / os.getenv("PGCOPYDB_LOG_FILE_NAME")

@pytest.mark.remote_cluster
def test_ingest_performance_using_pgcopydb(log_file_path: Path):
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
    create_pgcopydb_filter_file()

    # Get backpressure time before ingest
    backpressure_time_before = get_backpressure_time(os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR"))

    # Build and run the pgcopydb command
    command = build_pgcopydb_command()
    try:
        run_command_and_log_output(command, log_file_path)
    except subprocess.CalledProcessError as e:
        pytest.fail(f"pgcopydb command failed with error: {e}")

    # Get backpressure time after ingest and calculate the difference
    backpressure_time_after = get_backpressure_time(os.getenv("BENCHMARK_INGEST_TARGET_CONNSTR"))
    backpressure_time_diff = backpressure_time_after - backpressure_time_before

    # Parse log file and report metrics, including backpressure time difference
    parse_log_and_report_metrics(log_file_path, backpressure_time_diff)

    # Check log file creation and content
    assert log_file_path.exists(), "Log file should be created"
    assert log_file_path.stat().st_size > 0, "Log file should not be empty"
