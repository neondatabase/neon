#!/usr/bin/env python3
import argparse
import logging
import os
import re
import sys
from contextlib import contextmanager
from pathlib import Path

import backoff
import psycopg2

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS regress_test_results (
    id SERIAL PRIMARY KEY,
    reference CHAR(255),
    revision CHAR(40),
    build_type CHAR(16),
    data JSONB
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


def ingest_regress_test_result(
    cursor, reference: str, revision: str, build_type: str, data_file: Path
):
    data = data_file.read_text()
    # In the JSON report we can have lines related to LazyFixture with escaped double-quote
    # It's hard to insert them into jsonb field as is, so replace \" with ' to make it easier for us
    #
    # "<LazyFixture \"vanilla_compare\">" -> "<LazyFixture 'vanilla_compare'>"
    data = re.sub(r'("<LazyFixture )\\"([^\\]+)\\"(>")', r"\g<1>'\g<2>'\g<3>", data)
    values = (
        reference,
        revision,
        build_type,
        data,
    )
    cursor.execute(
        """
        INSERT INTO regress_test_results (
            reference,
            revision,
            build_type,
            data
        ) VALUES (%s, %s, %s, %s)
        """,
        values,
    )


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
    parser.add_argument(
        "--build-type", type=str, required=True, help="build type: release, debug or remote"
    )
    parser.add_argument(
        "--ingest", type=Path, required=True, help="Path to regress test result file"
    )

    args = parser.parse_args()
    with get_connection_cursor() as cur:
        if args.initdb:
            create_table(cur)

        if not args.ingest.exists():
            err(f"ingest path {args.ingest} does not exist")

        ingest_regress_test_result(
            cur,
            reference=args.reference,
            revision=args.revision,
            build_type=args.build_type,
            data_file=args.ingest,
        )


if __name__ == "__main__":
    logging.getLogger("backoff").addHandler(logging.StreamHandler())
    main()
