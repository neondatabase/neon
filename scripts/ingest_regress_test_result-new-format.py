#! /usr/bin/env python3

import argparse
import json
import logging
import os
import re
import sys
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Tuple

import backoff
import psycopg2
from psycopg2.extras import execute_values

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS results (
    id           BIGSERIAL PRIMARY KEY,
    parent_suite TEXT NOT NULL,
    suite        TEXT NOT NULL,
    name         TEXT NOT NULL,
    status       TEXT NOT NULL,
    start_ts     TIMESTAMP NOT NULL,
    stop_ts      TIMESTAMP NOT NULL,
    duration_s   INT NOT NULL,
    flaky        BOOLEAN NOT NULL,
    build_type   TEXT NOT NULL,
    pg_version   INT NOT NULL,
    run_id       BIGINT NOT NULL,
    run_attempt  INT NOT NULL,
    reference    TEXT NOT NULL,
    revision     CHAR(40) NOT NULL,
    raw          JSONB COMPRESSION lz4 NOT NULL,
    UNIQUE (parent_suite, suite, name, build_type, pg_version, start_ts, stop_ts, run_id)
);
"""

Row = namedtuple(
    "Row",
    [
        "parent_suite",
        "suite",
        "name",
        "status",
        "start_ts",
        "stop_ts",
        "duration_s",
        "flaky",
        "build_type",
        "pg_version",
        "run_id",
        "run_attempt",
        "reference",
        "revision",
        "raw",
    ],
)

TEST_NAME_RE = re.compile(r"[\[-](?P<build_type>debug|release)-pg(?P<pg_version>\d+)[-\]]")


def err(msg):
    print(f"error: {msg}")
    sys.exit(1)


@contextmanager
def get_connection_cursor(connstr: str):
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


def parse_test_name(test_name: str) -> Tuple[str, int, str]:
    build_type, pg_version = None, None
    if match := TEST_NAME_RE.search(test_name):
        found = match.groupdict()
        build_type = found["build_type"]
        pg_version = int(found["pg_version"])
    else:
        # It's ok, we embed BUILD_TYPE and Postgres Version into the test name only for regress suite and do not for other suites (like performance)
        build_type = "release"
        pg_version = 14

    unparametrized_name = re.sub(rf"{build_type}-pg{pg_version}-?", "", test_name).replace("[]", "")

    return build_type, pg_version, unparametrized_name


def ingest_test_result(
    cur,
    reference: str,
    revision: str,
    run_id: int,
    run_attempt: int,
    test_cases_dir: Path,
):
    rows = []
    for f in test_cases_dir.glob("*.json"):
        test = json.loads(f.read_text())
        # Drop unneded fields from raw data
        raw = test.copy()
        raw.pop("parameterValues")
        raw.pop("labels")
        raw.pop("extra")

        build_type, pg_version, unparametrized_name = parse_test_name(test["name"])
        labels = {label["name"]: label["value"] for label in test["labels"]}
        row = Row(
            parent_suite=labels["parentSuite"],
            suite=labels["suite"],
            name=unparametrized_name,
            status=test["status"],
            start_ts=datetime.fromtimestamp(test["time"]["start"] / 1000, tz=timezone.utc),
            stop_ts=datetime.fromtimestamp(test["time"]["stop"] / 1000, tz=timezone.utc),
            duration_s=round(test["time"]["duration"] / 1000),
            flaky=test["flaky"] or test["retriesStatusChange"],
            build_type=build_type,
            pg_version=pg_version,
            run_id=run_id,
            run_attempt=run_attempt,
            reference=reference,
            revision=revision,
            raw=json.dumps(raw),
        )
        rows.append(tuple(row))

    columns = ",".join(Row._fields)
    query = f"INSERT INTO results ({columns}) VALUES %s ON CONFLICT DO NOTHING"
    execute_values(cur, query, rows)


def main():
    parser = argparse.ArgumentParser(
        description="Regress test result uploader. \
            Database connection string should be provided via DATABASE_URL environment variable",
    )
    parser.add_argument("--initdb", action="store_true", help="Initialuze database")
    parser.add_argument(
        "--reference", type=str, required=True, help="git reference, for example refs/heads/main"
    )
    parser.add_argument("--revision", type=str, required=True, help="git revision")
    parser.add_argument("--run-id", type=int, required=True, help="GitHub Workflow run id")
    parser.add_argument(
        "--run-attempt", type=int, required=True, help="GitHub Workflow run attempt"
    )
    parser.add_argument(
        "--test-cases-dir",
        type=Path,
        required=True,
        help="Path to a dir with extended test cases data",
    )

    connstr = os.getenv("DATABASE_URL", "")
    if not connstr:
        err("DATABASE_URL environment variable is not set")

    args = parser.parse_args()
    with get_connection_cursor(connstr) as cur:
        if args.initdb:
            create_table(cur)

        if not args.test_cases_dir.exists():
            err(f"test-cases dir {args.test_cases_dir} does not exist")

        if not args.test_cases_dir.is_dir():
            err(f"test-cases dir {args.test_cases_dir} it not a directory")

        ingest_test_result(
            cur,
            reference=args.reference,
            revision=args.revision,
            run_id=args.run_id,
            run_attempt=args.run_attempt,
            test_cases_dir=args.test_cases_dir,
        )


if __name__ == "__main__":
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    main()
