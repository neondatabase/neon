#!/usr/bin/env python3
# ruff: noqa
# we exclude the file from ruff because on the github runner we have python 3.9 and ruff
# is running with newer python which suggests changes incompatible with python 3.9
"""
Upload BenchBase TPC-C results from summary.json and results.csv files to perf_test_results database.

This script extracts metrics from BenchBase *.summary.json and *.results.csv files and uploads them
to a PostgreSQL database table for performance tracking and analysis.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2


def load_summary_json(json_file_path):
    """Load summary.json file and return parsed data."""
    try:
        with open(json_file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Summary JSON file not found: {json_file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file {json_file_path}: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading JSON file {json_file_path}: {e}")
        sys.exit(1)


def get_metric_info(metric_name):
    """Get metric unit and report type for a given metric name."""
    metrics_config = {
        "Throughput": {"unit": "req/s", "report_type": "higher_is_better"},
        "Goodput": {"unit": "req/s", "report_type": "higher_is_better"},
        "Measured Requests": {"unit": "requests", "report_type": "higher_is_better"},
        "95th Percentile Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "Maximum Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "Median Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "Minimum Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "25th Percentile Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "90th Percentile Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "99th Percentile Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "75th Percentile Latency": {"unit": "µs", "report_type": "lower_is_better"},
        "Average Latency": {"unit": "µs", "report_type": "lower_is_better"},
    }

    return metrics_config.get(metric_name, {"unit": "", "report_type": "higher_is_better"})


def extract_metrics(summary_data):
    """Extract relevant metrics from summary JSON data."""
    metrics = []

    # Direct top-level metrics
    direct_metrics = {
        "Throughput (requests/second)": "Throughput",
        "Goodput (requests/second)": "Goodput",
        "Measured Requests": "Measured Requests",
    }

    for json_key, clean_name in direct_metrics.items():
        if json_key in summary_data:
            metrics.append((clean_name, summary_data[json_key]))

    # Latency metrics from nested "Latency Distribution" object
    if "Latency Distribution" in summary_data:
        latency_data = summary_data["Latency Distribution"]
        latency_metrics = {
            "95th Percentile Latency (microseconds)": "95th Percentile Latency",
            "Maximum Latency (microseconds)": "Maximum Latency",
            "Median Latency (microseconds)": "Median Latency",
            "Minimum Latency (microseconds)": "Minimum Latency",
            "25th Percentile Latency (microseconds)": "25th Percentile Latency",
            "90th Percentile Latency (microseconds)": "90th Percentile Latency",
            "99th Percentile Latency (microseconds)": "99th Percentile Latency",
            "75th Percentile Latency (microseconds)": "75th Percentile Latency",
            "Average Latency (microseconds)": "Average Latency",
        }

        for json_key, clean_name in latency_metrics.items():
            if json_key in latency_data:
                metrics.append((clean_name, latency_data[json_key]))

    return metrics


def build_labels(summary_data, project_id):
    """Build labels JSON object from summary data and project info."""
    labels = {}

    # Extract required label keys from summary data
    label_keys = [
        "DBMS Type",
        "DBMS Version",
        "Benchmark Type",
        "Final State",
        "isolation",
        "scalefactor",
        "terminals",
    ]

    for key in label_keys:
        if key in summary_data:
            labels[key] = summary_data[key]

    # Add project_id from workflow
    labels["project_id"] = project_id

    return labels


def build_suit_name(scalefactor, terminals, run_type, min_cu, max_cu):
    """Build the suit name according to specification."""
    return f"benchbase-tpc-c-{scalefactor}-{terminals}-{run_type}-{min_cu}-{max_cu}"


def convert_timestamp_to_utc(timestamp_ms):
    """Convert millisecond timestamp to PostgreSQL-compatible UTC timestamp."""
    try:
        dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not convert timestamp {timestamp_ms}: {e}")
        return datetime.now(timezone.utc).isoformat()


def insert_metrics(conn, metrics_data):
    """Insert metrics data into the perf_test_results table."""
    insert_query = """
    INSERT INTO perf_test_results
    (suit, revision, platform, metric_name, metric_value, metric_unit,
     metric_report_type, recorded_at_timestamp, labels)
    VALUES (%(suit)s, %(revision)s, %(platform)s, %(metric_name)s, %(metric_value)s,
            %(metric_unit)s, %(metric_report_type)s, %(recorded_at_timestamp)s, %(labels)s)
    """

    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, metrics_data)
        conn.commit()
        print(f"Successfully inserted {len(metrics_data)} metrics into perf_test_results")

        # Log some sample data for verification
        if metrics_data:
            print(
                f"Sample metric: {metrics_data[0]['metric_name']} = {metrics_data[0]['metric_value']} {metrics_data[0]['metric_unit']}"
            )

    except Exception as e:
        conn.rollback()
        print(f"Error inserting metrics into database: {e}")
        sys.exit(1)


def create_benchbase_results_details_table(conn):
    """Create benchbase_results_details table if it doesn't exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS benchbase_results_details (
        id BIGSERIAL PRIMARY KEY,
        suit TEXT,
        revision CHAR(40),
        platform TEXT,
        recorded_at_timestamp TIMESTAMP WITH TIME ZONE,
        requests_per_second NUMERIC,
        average_latency_ms NUMERIC,
        minimum_latency_ms NUMERIC,
        p25_latency_ms NUMERIC,
        median_latency_ms NUMERIC,
        p75_latency_ms NUMERIC,
        p90_latency_ms NUMERIC,
        p95_latency_ms NUMERIC,
        p99_latency_ms NUMERIC,
        maximum_latency_ms NUMERIC
    );

    CREATE INDEX IF NOT EXISTS benchbase_results_details_recorded_at_timestamp_idx
        ON benchbase_results_details USING BRIN (recorded_at_timestamp);
    CREATE INDEX IF NOT EXISTS benchbase_results_details_suit_idx
        ON benchbase_results_details USING BTREE (suit text_pattern_ops);
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_query)
        conn.commit()
        print("Successfully created/verified benchbase_results_details table")
    except Exception as e:
        conn.rollback()
        print(f"Error creating benchbase_results_details table: {e}")
        sys.exit(1)


def process_csv_results(csv_file_path, start_timestamp_ms, suit, revision, platform):
    """Process CSV results and return data for database insertion."""
    try:
        # Read CSV file
        df = pd.read_csv(csv_file_path)

        # Validate required columns exist
        required_columns = [
            "Time (seconds)",
            "Throughput (requests/second)",
            "Average Latency (millisecond)",
            "Minimum Latency (millisecond)",
            "25th Percentile Latency (millisecond)",
            "Median Latency (millisecond)",
            "75th Percentile Latency (millisecond)",
            "90th Percentile Latency (millisecond)",
            "95th Percentile Latency (millisecond)",
            "99th Percentile Latency (millisecond)",
            "Maximum Latency (millisecond)",
        ]

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns in CSV: {missing_columns}")
            return []

        csv_data = []

        for _, row in df.iterrows():
            # Calculate timestamp: start_timestamp_ms + (time_seconds * 1000)
            time_seconds = row["Time (seconds)"]
            row_timestamp_ms = start_timestamp_ms + (time_seconds * 1000)

            # Convert to UTC timestamp
            row_timestamp = datetime.fromtimestamp(
                row_timestamp_ms / 1000.0, tz=timezone.utc
            ).isoformat()

            csv_row = {
                "suit": suit,
                "revision": revision,
                "platform": platform,
                "recorded_at_timestamp": row_timestamp,
                "requests_per_second": float(row["Throughput (requests/second)"]),
                "average_latency_ms": float(row["Average Latency (millisecond)"]),
                "minimum_latency_ms": float(row["Minimum Latency (millisecond)"]),
                "p25_latency_ms": float(row["25th Percentile Latency (millisecond)"]),
                "median_latency_ms": float(row["Median Latency (millisecond)"]),
                "p75_latency_ms": float(row["75th Percentile Latency (millisecond)"]),
                "p90_latency_ms": float(row["90th Percentile Latency (millisecond)"]),
                "p95_latency_ms": float(row["95th Percentile Latency (millisecond)"]),
                "p99_latency_ms": float(row["99th Percentile Latency (millisecond)"]),
                "maximum_latency_ms": float(row["Maximum Latency (millisecond)"]),
            }
            csv_data.append(csv_row)

        print(f"Processed {len(csv_data)} rows from CSV file")
        return csv_data

    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_file_path}")
        return []
    except Exception as e:
        print(f"Error processing CSV file {csv_file_path}: {e}")
        return []


def insert_csv_results(conn, csv_data):
    """Insert CSV results into benchbase_results_details table."""
    if not csv_data:
        print("No CSV data to insert")
        return

    insert_query = """
    INSERT INTO benchbase_results_details
    (suit, revision, platform, recorded_at_timestamp, requests_per_second,
     average_latency_ms, minimum_latency_ms, p25_latency_ms, median_latency_ms,
     p75_latency_ms, p90_latency_ms, p95_latency_ms, p99_latency_ms, maximum_latency_ms)
    VALUES (%(suit)s, %(revision)s, %(platform)s, %(recorded_at_timestamp)s, %(requests_per_second)s,
            %(average_latency_ms)s, %(minimum_latency_ms)s, %(p25_latency_ms)s, %(median_latency_ms)s,
            %(p75_latency_ms)s, %(p90_latency_ms)s, %(p95_latency_ms)s, %(p99_latency_ms)s, %(maximum_latency_ms)s)
    """

    try:
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, csv_data)
        conn.commit()
        print(
            f"Successfully inserted {len(csv_data)} detailed results into benchbase_results_details"
        )

        # Log some sample data for verification
        if csv_data:
            sample = csv_data[0]
            print(
                f"Sample detail: {sample['requests_per_second']} req/s at {sample['recorded_at_timestamp']}"
            )

    except Exception as e:
        conn.rollback()
        print(f"Error inserting CSV results into database: {e}")
        sys.exit(1)


def main():
    """Main function to parse arguments and upload results."""
    parser = argparse.ArgumentParser(
        description="Upload BenchBase TPC-C results to perf_test_results database"
    )
    parser.add_argument(
        "--summary-json", type=str, required=True, help="Path to the summary.json file"
    )
    parser.add_argument(
        "--run-type",
        type=str,
        required=True,
        choices=["warmup", "opt-rate", "ramp-up"],
        help="Type of benchmark run",
    )
    parser.add_argument("--min-cu", type=float, required=True, help="Minimum compute units")
    parser.add_argument("--max-cu", type=float, required=True, help="Maximum compute units")
    parser.add_argument("--project-id", type=str, required=True, help="Neon project ID")
    parser.add_argument(
        "--revision", type=str, required=True, help="Git commit hash (40 characters)"
    )
    parser.add_argument(
        "--connection-string", type=str, required=True, help="PostgreSQL connection string"
    )
    parser.add_argument(
        "--results-csv",
        type=str,
        required=False,
        help="Path to the results.csv file for detailed metrics upload",
    )

    args = parser.parse_args()

    # Validate inputs
    if not Path(args.summary_json).exists():
        print(f"Error: Summary JSON file does not exist: {args.summary_json}")
        sys.exit(1)

    if len(args.revision) != 40:
        print(f"Warning: Revision should be 40 characters, got {len(args.revision)}")

    # Load and process summary data
    summary_data = load_summary_json(args.summary_json)

    # Extract metrics
    metrics = extract_metrics(summary_data)
    if not metrics:
        print("Warning: No metrics found to upload")
        return

    # Build common data for all metrics
    scalefactor = summary_data.get("scalefactor", "unknown")
    terminals = summary_data.get("terminals", "unknown")
    suit = build_suit_name(scalefactor, terminals, args.run_type, args.min_cu, args.max_cu)
    platform = f"prod-us-east-2-{args.project_id}"
    labels = build_labels(summary_data, args.project_id)

    # Convert timestamp
    current_timestamp_ms = summary_data.get("Current Timestamp (milliseconds)")
    start_timestamp_ms = summary_data.get("Start timestamp (milliseconds)")

    if current_timestamp_ms:
        recorded_at = convert_timestamp_to_utc(current_timestamp_ms)
    else:
        print("Warning: No timestamp found in JSON, using current time")
        recorded_at = datetime.now(timezone.utc).isoformat()

    if not start_timestamp_ms:
        print("Warning: No start timestamp found in JSON, CSV upload may be incorrect")
        start_timestamp_ms = current_timestamp_ms or datetime.now(timezone.utc).timestamp() * 1000

    # Prepare metrics data for database insertion
    metrics_data = []
    for metric_name, metric_value in metrics:
        metric_info = get_metric_info(metric_name)

        row = {
            "suit": suit,
            "revision": args.revision,
            "platform": platform,
            "metric_name": metric_name,
            "metric_value": float(metric_value),  # Ensure numeric type
            "metric_unit": metric_info["unit"],
            "metric_report_type": metric_info["report_type"],
            "recorded_at_timestamp": recorded_at,
            "labels": json.dumps(labels),  # Convert to JSON string for JSONB column
        }
        metrics_data.append(row)

    print(f"Prepared {len(metrics_data)} metrics for upload to database")
    print(f"Suit: {suit}")
    print(f"Platform: {platform}")

    # Connect to database and insert metrics
    try:
        conn = psycopg2.connect(args.connection_string)

        # Insert summary metrics into perf_test_results
        insert_metrics(conn, metrics_data)

        # Process and insert detailed CSV results if provided
        if args.results_csv:
            print(f"Processing detailed CSV results from: {args.results_csv}")

            # Create table if it doesn't exist
            create_benchbase_results_details_table(conn)

            # Process CSV data
            csv_data = process_csv_results(
                args.results_csv, start_timestamp_ms, suit, args.revision, platform
            )

            # Insert CSV data
            if csv_data:
                insert_csv_results(conn, csv_data)
            else:
                print("No CSV data to upload")
        else:
            print("No CSV file provided, skipping detailed results upload")

        conn.close()
        print("Database upload completed successfully")

    except psycopg2.Error as e:
        print(f"Database connection/query error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
