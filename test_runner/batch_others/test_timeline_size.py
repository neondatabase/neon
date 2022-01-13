from contextlib import closing
from uuid import UUID
import psycopg2.extras
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
import time


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


def test_timeline_size_quota(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli(["branch", "test_timeline_size_quota", "empty"])

    client = env.pageserver.http_client()
    res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
    assert res["current_logical_size"] == res["current_logical_size_non_incremental"]

    pgmain = env.postgres.create_start(
        "test_timeline_size_quota",
        config_lines=['zenith.max_cluster_size=25MB'],
    )
    log.info("postgres is running on 'test_timeline_size_quota' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")

            cur.execute("CREATE TABLE foo (t text)")

            res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
            current_logical_size = res["current_logical_size"]
            log.info(f"current_logical_size 1 = {current_logical_size}")

            # Insert many rows. This query must fail because of space limit
            try:
                cur.execute('''
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 100000) g
                ''')

                cur.execute('''
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 100000) g
                ''')

                # double check: get size from pageserver
                client = env.pageserver.http_client()
                res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
                current_logical_size = res["current_logical_size"]
                log.info(f"current_logical_size 2 = {current_logical_size}")

                # If we get here, the timeline size limit failed
                log.error("Query unexpectedly succeeded")
                assert False

            except Exception as err:
                log.info(f"Query failed with: {err}")
                # 53100 is a disk_full errcode
                assert err.pgcode == '53100'

                # double check: get size from pageserver
                client = env.pageserver.http_client()
                res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
                current_logical_size = res["current_logical_size"]
                log.info(f"current_logical_size = {current_logical_size}")

            # drop table to free space
            cur.execute('DROP TABLE foo')

            # create it again and insert some rows. This query must succeed
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute('''
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10000) g
            ''')

            # double check: get size from pageserver
            client = env.pageserver.http_client()
            res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
            current_logical_size = res["current_logical_size"]
            log.info(f"current_logical_size = {current_logical_size}")
            assert current_logical_size < 25 * 1024 * 1024
