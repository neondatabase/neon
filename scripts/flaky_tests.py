#! /usr/bin/env python3

from __future__ import annotations

import argparse
import json
import logging
import os
from collections import defaultdict
from typing import TYPE_CHECKING

import psycopg2
import psycopg2.extras
import toml

if TYPE_CHECKING:
    from typing import Any

FLAKY_TESTS_QUERY = """
    SELECT
        DISTINCT parent_suite, suite, name
    FROM results
    WHERE
        started_at > CURRENT_DATE - INTERVAL '%s' day
        AND (
            (status IN ('failed', 'broken') AND reference = 'refs/heads/main')
            OR flaky
        )
    ;
"""


def main(args: argparse.Namespace):
    connstr = args.connstr
    interval_days = args.days
    output = args.output

    build_type = args.build_type
    pg_version = args.pg_version

    res: defaultdict[str, defaultdict[str, dict[str, bool]]]
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

    # If a test run has non-default PAGESERVER_VIRTUAL_FILE_IO_ENGINE (i.e. not empty, not tokio-epoll-uring),
    # use it to parametrize test name along with build_type and pg_version
    #
    # See test_runner/fixtures/parametrize.py for details
    if (io_engine := os.getenv("PAGESERVER_VIRTUAL_FILE_IO_ENGINE", "")) not in (
        "",
        "tokio-epoll-uring",
    ):
        pageserver_virtual_file_io_engine_parameter = f"-{io_engine}"
    else:
        pageserver_virtual_file_io_engine_parameter = ""

    # re-use existing records of flaky tests from before parametrization by compaction_algorithm
    def get_pageserver_default_tenant_config_compaction_algorithm() -> dict[str, Any] | None:
        """Duplicated from parametrize.py"""
        toml_table = os.getenv("PAGESERVER_DEFAULT_TENANT_CONFIG_COMPACTION_ALGORITHM")
        if toml_table is None:
            return None
        v = toml.loads(toml_table)
        assert isinstance(v, dict)
        return v

    pageserver_default_tenant_config_compaction_algorithm_parameter = ""
    if (
        explicit_default := get_pageserver_default_tenant_config_compaction_algorithm()
    ) is not None:
        pageserver_default_tenant_config_compaction_algorithm_parameter = (
            f"-{explicit_default['kind']}"
        )

    for row in rows:
        # We don't want to automatically rerun tests in a performance suite
        if row["parent_suite"] != "test_runner.regress":
            continue

        if row["name"].endswith("]"):
            parametrized_test = row["name"].replace(
                "[",
                f"[{build_type}-pg{pg_version}{pageserver_virtual_file_io_engine_parameter}{pageserver_default_tenant_config_compaction_algorithm_parameter}-",
            )
        else:
            parametrized_test = f"{row['name']}[{build_type}-pg{pg_version}{pageserver_virtual_file_io_engine_parameter}{pageserver_default_tenant_config_compaction_algorithm_parameter}]"

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
