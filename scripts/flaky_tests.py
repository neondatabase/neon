#! /usr/bin/env python3

import argparse
import json
import logging
from collections import defaultdict
from typing import DefaultDict, Dict

import psycopg2
import psycopg2.extras

# We call the test "flaky" if it failed at least once on the main branch in the last N=10 days.
FLAKY_TESTS_QUERY = """
    SELECT
        DISTINCT parent_suite, suite, REGEXP_REPLACE(test, '(release|debug)-pg(\\d+)-?', '') as deparametrized_test
    FROM
        (
            SELECT
                reference,
                jsonb_array_elements(data -> 'children') ->> 'name' as parent_suite,
                jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') ->> 'name' as suite,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') ->> 'name' as test,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') ->> 'status' as status,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') ->> 'retriesStatusChange' as retries_status_change,
                to_timestamp((jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'time' ->> 'start')::bigint / 1000)::date as timestamp
            FROM
                regress_test_results
        ) data
    WHERE
        timestamp > CURRENT_DATE - INTERVAL '%s' day
        AND (
            (status IN ('failed', 'broken') AND reference = 'refs/heads/main')
            OR retries_status_change::boolean
        )
    ;
"""


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output

    build_type = args.build_type
    pg_version = args.pg_version

    res: DefaultDict[str, DefaultDict[str, Dict[str, bool]]]
    res = defaultdict(lambda: defaultdict(dict))

    try:
        logging.info("connecting to the database...")
        with psycopg2.connect(connstr, connect_timeout=30) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                logging.info("fetching flaky tests...")
                cur.execute(FLAKY_TESTS_QUERY, (interval_days,))
                rows = cur.fetchall()
    except psycopg2.OperationalError as exc:
        logging.error("cannot fetch flaky tests from the DB due to an error", exc)
        rows = []

    for row in rows:
        # We don't want to automatically rerun tests in a performance suite
        if row["parent_suite"] != "test_runner.regress":
            continue

        deparametrized_test = row["deparametrized_test"]
        dash_if_needed = "" if deparametrized_test.endswith("[]") else "-"
        parametrized_test = deparametrized_test.replace(
            "[",
            f"[{build_type}-pg{pg_version}{dash_if_needed}",
        )
        res[row["parent_suite"]][row["suite"]][parametrized_test] = True

        logging.info(
            f"\t{row['parent_suite'].replace('.', '/')}/{row['suite']}.py::{parametrized_test}"
        )

    logging.info(f"saving results to {output.name}")
    json.dump(res, output, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Detect flaky tests in the last N days")
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        default="flaky.json",
        help="path to output json file (default: flaky.json)",
    )
    parser.add_argument(
        "--days",
        required=False,
        default=10,
        type=int,
        help="how many days to look back for flaky tests (default: 10)",
    )
    parser.add_argument(
        "--build-type",
        required=True,
        type=str,
        help="for which build type to create list of flaky tests (debug or release)",
    )
    parser.add_argument(
        "--pg-version",
        required=True,
        type=int,
        help="for which Postgres version to create list of flaky tests (14, 15, etc.)",
    )
    parser.add_argument(
        "connstr",
        help="connection string to the test results database",
    )
    args = parser.parse_args()

    level = logging.INFO
    logging.basicConfig(
        format="%(message)s",
        level=level,
    )

    main(args)
