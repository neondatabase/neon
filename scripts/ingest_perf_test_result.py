#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import backoff
import psycopg2
import psycopg2.extras

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS perf_test_results (
    id SERIAL PRIMARY KEY,
    suit TEXT,
    revision CHAR(40),
    platform TEXT,
    metric_name TEXT,
    metric_value NUMERIC,
    metric_unit VARCHAR(10),
    metric_report_type TEXT,
    recorded_at_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    labels JSONB with default '{}'
)
"""


def err(msg):
    print(f"error: {msg}")
    sys.exit(1)


@contextmanager
def get_connection_cursor():
    connstr = os.getenv("DATABASE_URL")
    if not connstr:
        err("DATABASE_URL environment variable is not set")

    @backoff.on_exception(backoff.expo, psycopg2.OperationalError, max_time=150)
    def connect(connstr):
        conn = psycopg2.connect(connstr, connect_timeout=30)
        conn.autocommit = True
        return conn

    conn = connect(connstr)
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        if conn is not None:
            conn.close()


def create_table(cur):
    cur.execute(CREATE_TABLE)


def ingest_perf_test_result(cursor, data_file: Path, recorded_at_timestamp: int) -> int:
    run_data = json.loads(data_file.read_text())
    revision = run_data["revision"]
    platform = run_data["platform"]

    run_result = run_data["result"]
    args_list = []

    for suit_result in run_result:
        suit = suit_result["suit"]
        total_duration = suit_result["total_duration"]

        suit_result["data"].append(
            {
                "name": "total_duration",
                "value": total_duration,
                "unit": "s",
                "report": "lower_is_better",
            }
        )

        for metric in suit_result["data"]:
            values = {
                "suit": suit,
                "revision": revision,
                "platform": platform,
                "metric_name": metric["name"],
                "metric_value": metric["value"],
                "metric_unit": metric["unit"],
                "metric_report_type": metric["report"],
                "recorded_at_timestamp": datetime.utcfromtimestamp(recorded_at_timestamp),
                "labels": json.dumps(metric.get("labels")),
            }
            args_list.append(values)

    psycopg2.extras.execute_values(
        cursor,
        """
        INSERT INTO perf_test_results (
            suit,
            revision,
            platform,
            metric_name,
            metric_value,
            metric_unit,
            metric_report_type,
            recorded_at_timestamp,
            labels
        ) VALUES %s
        """,
        args_list,
        template="""(
            %(suit)s,
            %(revision)s,
            %(platform)s,
            %(metric_name)s,
            %(metric_value)s,
            %(metric_unit)s,
            %(metric_report_type)s,
            %(recorded_at_timestamp)s,
            %(labels)s
        )""",
    )
    return len(args_list)


def main():
    parser = argparse.ArgumentParser(
        description="Perf test result uploader. \
            Database connection string should be provided via DATABASE_URL environment variable",
    )
    parser.add_argument(
        "--ingest",
        type=Path,
        required=True,
        help="Path to perf test result file, or directory with perf test result files",
    )
    parser.add_argument("--initdb", action="store_true", help="Initialuze database")

    args = parser.parse_args()
    with get_connection_cursor() as cur:
        if args.initdb:
            create_table(cur)

        if not args.ingest.exists():
            err(f"ingest path {args.ingest} does not exist")

        if args.ingest:
            if args.ingest.is_dir():
                for item in sorted(args.ingest.iterdir(), key=lambda x: int(x.name.split("_")[0])):
                    recorded_at_timestamp = int(item.name.split("_")[0])
                    ingested = ingest_perf_test_result(cur, item, recorded_at_timestamp)
                    print(f"Ingested {ingested} metric values from {item}")
            else:
                recorded_at_timestamp = int(args.ingest.name.split("_")[0])
                ingested = ingest_perf_test_result(cur, args.ingest, recorded_at_timestamp)
                print(f"Ingested {ingested} metric values from {args.ingest}")


if __name__ == "__main__":
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    main()
