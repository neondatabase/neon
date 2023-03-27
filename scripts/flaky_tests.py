#! /usr/bin/env python3

import argparse
import json
import logging
from collections import defaultdict
from typing import DefaultDict

import psycopg2
import psycopg2.extras

# We call the test "flaky" if it changed its status on the same revision in the last N=10 days on main branch.
FLAKY_TESTS_QUERY = """
    SELECT
        DISTINCT ON (parent_suite, suite, name) parent_suite, suite, name
    FROM
        (
            SELECT
                revision,
                jsonb_array_elements(data -> 'children') -> 'name' as parent_suite,
                jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'name' as suite,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'name' as name,
                jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'status' as status,
                to_timestamp((jsonb_array_elements(jsonb_array_elements(jsonb_array_elements(data -> 'children') -> 'children') -> 'children') -> 'time' -> 'start')::bigint / 1000)::date as timestamp
            FROM
                regress_test_results
            WHERE
                reference = 'refs/heads/main'
        ) data
    WHERE
        timestamp > CURRENT_DATE - INTERVAL '%s' day
    GROUP BY
        revision, parent_suite, suite, name
    HAVING
        COUNT(DISTINCT status) > 1
    ;
"""


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output

    res: DefaultDict[str, DefaultDict[str, DefaultDict[str, bool]]]
    res = defaultdict(lambda: defaultdict(lambda: defaultdict(bool)))

    logging.info("connecting to the database...")
    with psycopg2.connect(connstr, connect_timeout=10) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            logging.info("fetching flaky tests...")
            cur.execute(FLAKY_TESTS_QUERY, (interval_days,))
            rows = cur.fetchall()

    for row in rows:
        logging.info(f"\t{row['parent_suite'].replace('.', '/')}/{row['suite']}.py::{row['name']}")
        res[row["parent_suite"]][row["suite"]][row["name"]] = True

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
