from contextlib import closing
from uuid import UUID
import psycopg2.extras
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


def test_timeline_size(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    # Branch at the point where only 100 rows were inserted
    env.zenith_cli(["branch", "test_timeline_size", "empty"])

    client = env.pageserver.http_client()
    res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size")
    assert res["current_logical_size"] == res["current_logical_size_non_incremental"]

    pgmain = env.postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")

            # Create table, and insert the first 100 rows
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute("""
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10) g
            """)

            res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size")
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
            cur.execute("TRUNCATE foo")

            res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size")
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
