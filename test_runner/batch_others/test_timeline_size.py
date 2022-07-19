from contextlib import closing
import os
from uuid import UUID
import psycopg2.extras
import psycopg2.errors
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, NeonPageserverHttpClient, Postgres, assert_timeline_local
from fixtures.log_helper import log
import time


def test_timeline_size(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch('test_timeline_size', 'empty')

    client = env.pageserver.http_client()
    timeline_details = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
    assert timeline_details['local']['current_logical_size'] == timeline_details['local'][
        'current_logical_size_non_incremental']

    pgmain = env.postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW neon.timeline_id")

            cur.execute("CREATE TABLE foo (t text)")
            cur.execute("""
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10) g
            """)

            res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
            local_details = res['local']
            assert local_details["current_logical_size"] == local_details[
                "current_logical_size_non_incremental"]
            cur.execute("TRUNCATE foo")

            res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
            local_details = res['local']
            assert local_details["current_logical_size"] == local_details[
                "current_logical_size_non_incremental"]


def test_timeline_size_createdropdb(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch('test_timeline_size', 'empty')

    client = env.pageserver.http_client()
    timeline_details = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
    assert timeline_details['local']['current_logical_size'] == timeline_details['local'][
        'current_logical_size_non_incremental']

    pgmain = env.postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW neon.timeline_id")

            res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
            local_details = res['local']
            assert local_details["current_logical_size"] == local_details[
                "current_logical_size_non_incremental"]

            cur.execute('CREATE DATABASE foodb')
            with closing(pgmain.connect(dbname='foodb')) as conn:
                with conn.cursor() as cur2:

                    cur2.execute("CREATE TABLE foo (t text)")
                    cur2.execute("""
                        INSERT INTO foo
                            SELECT 'long string to consume some space' || g
                            FROM generate_series(1, 10) g
                    """)

                    res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
                    local_details = res['local']
                    assert local_details["current_logical_size"] == local_details[
                        "current_logical_size_non_incremental"]

            cur.execute('DROP DATABASE foodb')

            res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
            local_details = res['local']
            assert local_details["current_logical_size"] == local_details[
                "current_logical_size_non_incremental"]


# wait until received_lsn_lag is 0
def wait_for_pageserver_catchup(pgmain: Postgres, polling_interval=1, timeout=60):
    started_at = time.time()

    received_lsn_lag = 1
    while received_lsn_lag > 0:
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(
                f"timed out waiting for pageserver to reach pg_current_wal_flush_lsn()")

        with closing(pgmain.connect()) as conn:
            with conn.cursor() as cur:

                cur.execute('''
                    select  pg_size_pretty(pg_cluster_size()),
                    pg_wal_lsn_diff(pg_current_wal_flush_lsn(),received_lsn) as received_lsn_lag
                    FROM backpressure_lsns();
                ''')
                res = cur.fetchone()
                log.info(f"pg_cluster_size = {res[0]}, received_lsn_lag = {res[1]}")
                received_lsn_lag = res[1]

        time.sleep(polling_interval)


def test_timeline_size_quota(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    new_timeline_id = env.neon_cli.create_branch('test_timeline_size_quota')

    client = env.pageserver.http_client()
    res = assert_timeline_local(client, env.initial_tenant, new_timeline_id)
    assert res['local']["current_logical_size"] == res['local'][
        "current_logical_size_non_incremental"]

    pgmain = env.postgres.create_start(
        "test_timeline_size_quota",
        # Set small limit for the test
        config_lines=['neon.max_cluster_size=30MB'])
    log.info("postgres is running on 'test_timeline_size_quota' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION neon")  # TODO move it to neon_fixtures?

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


def test_timeline_physical_size_init(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch('test_timeline_physical_size_init')
    pg = env.postgres.create_start("test_timeline_physical_size_init")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute("""
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 1000) g
            """)

    # restart the pageserer to force calculating timeline's initial physical size
    env.pageserver.stop()
    env.pageserver.start()

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_compaction(neon_env_builder: NeonEnvBuilder):
    # Override default checkpointer settings to run it more often
    neon_env_builder.pageserver_config_override = "tenant_config={checkpoint_distance = 1048576}"
    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch('test_timeline_physical_size_post_compaction')
    pg = env.postgres.create_start("test_timeline_physical_size_post_compaction")

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute("""
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 100000) g
            """)

    env.pageserver.safe_psql(f"compact {env.initial_tenant.hex} {new_timeline_id.hex}")
    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def assert_physical_size(env: NeonEnv, tenant_id: UUID, timeline_id: UUID):
    """Check the current physical size returned from timeline API
    matches the total physical size of the timeline on disk"""
    client = env.pageserver.http_client()
    res = assert_timeline_local(client, tenant_id, timeline_id)
    timeline_path = f"{env.repo_dir}/tenants/{tenant_id.hex}/timelines/{timeline_id.hex}/"
    assert res["local"]["current_physical_size"] == get_dir_size(timeline_path)


def get_dir_size(path: str) -> int:
    """Get the directory's total size (not including sub-directories)"""
    sz = 0
    for f in os.listdir(path):
        f_path = os.path.join(path, f)
        if os.path.isfile(f_path):
            sz += os.path.getsize(f_path)
    return sz
