from contextlib import closing
from uuid import UUID
import psycopg2.extras
import psycopg2.errors
from fixtures.zenith_fixtures import ZenithEnv, ZenithEnvBuilder, Postgres
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


# wait until write_lag is 0
def wait_for_pageserver_catchup(pgmain: Postgres, polling_interval=1, timeout=60):
    started_at = time.time()

    write_lag = 1
    while write_lag > 0:
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(f"timed out waiting for pageserver to reach pg_current_wal_lsn()")

        with closing(pgmain.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    select  pg_size_pretty(pg_cluster_size()),
                    pg_wal_lsn_diff(pg_current_wal_lsn(),write_lsn) as write_lag,
                    pg_wal_lsn_diff(pg_current_wal_lsn(),sent_lsn) as pending_lag
                    FROM pg_stat_get_wal_senders();
                ''')
                res = cur.fetchone()
                log.info(
                    f"pg_cluster_size = {res[0]}, write_lag = {res[1]}, pending_lag = {res[2]}")
                write_lag = res[1]

        time.sleep(polling_interval)


def test_timeline_size_quota(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init()
    env.zenith_cli(["branch", "test_timeline_size_quota", "main"])

    client = env.pageserver.http_client()
    res = client.branch_detail(UUID(env.initial_tenant), "test_timeline_size_quota")
    assert res["current_logical_size"] == res["current_logical_size_non_incremental"]

    pgmain = env.postgres.create_start(
        "test_timeline_size_quota",
        # Set small limit for the test
        config_lines=['zenith.max_cluster_size=30MB'],
    )
    log.info("postgres is running on 'test_timeline_size_quota' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION zenith")  # TODO move it to zenith_fixtures?

            cur.execute("CREATE TABLE foo (t text)")

            wait_for_pageserver_catchup(pgmain)

            # Insert many rows. This query must fail because of space limit
            try:
                cur.execute('''
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 100000) g
                ''')

                wait_for_pageserver_catchup(pgmain)

                cur.execute('''
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 500000) g
                ''')

                # If we get here, the timeline size limit failed
                log.error("Query unexpectedly succeeded")
                assert False

            except psycopg2.errors.DiskFull as err:
                log.info(f"Query expectedly failed with: {err}")

            # drop table to free space
            cur.execute('DROP TABLE foo')

            wait_for_pageserver_catchup(pgmain)

            # create it again and insert some rows. This query must succeed
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute('''
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10000) g
            ''')

            wait_for_pageserver_catchup(pgmain)

            cur.execute("SELECT * from pg_size_pretty(pg_cluster_size())")
            pg_cluster_size = cur.fetchone()
            log.info(f"pg_cluster_size = {pg_cluster_size}")
