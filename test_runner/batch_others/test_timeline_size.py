from contextlib import closing
from uuid import UUID
import psycopg2.extras
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

import logging
import fixtures.log_helper  # configures loggers
log = logging.getLogger('root')

def test_timeline_size(
    zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory, pg_bin
):
    # Branch at the point where only 100 rows were inserted
    zenith_cli.run(["branch", "test_timeline_size", "empty"])

    client = pageserver.http_client()
    res = client.branch_detail(UUID(pageserver.initial_tenant), "test_timeline_size")
    assert res["current_logical_size"] == res["current_logical_size_non_incremental"]

    pgmain = postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")

            # Create table, and insert the first 100 rows
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute(
                """
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10) g
            """
            )

            res = client.branch_detail(UUID(pageserver.initial_tenant), "test_timeline_size")
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
            cur.execute("TRUNCATE foo")

            res = client.branch_detail(UUID(pageserver.initial_tenant), "test_timeline_size")
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
