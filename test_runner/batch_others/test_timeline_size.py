from contextlib import closing
import math
import random
from uuid import UUID
import re
import psycopg2.extras
import psycopg2.errors
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder, Postgres, assert_timeline_local, wait_for_last_flush_lsn
from fixtures.log_helper import log
import time

from fixtures.utils import get_timeline_dir_size


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

        res = pgmain.safe_psql('''
            SELECT
                pg_size_pretty(pg_cluster_size()),
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(), received_lsn) as received_lsn_lag
            FROM backpressure_lsns();
            ''')[0]
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

    pg.safe_psql_many([
        "CREATE TABLE foo (t text)",
        """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
    ])

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)

    # restart the pageserer to force calculating timeline's initial physical size
    env.pageserver.stop()
    env.pageserver.start()

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_checkpoint(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch('test_timeline_physical_size_post_checkpoint')
    pg = env.postgres.create_start("test_timeline_physical_size_post_checkpoint")

    pg.safe_psql_many([
        "CREATE TABLE foo (t text)",
        """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
    ])

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    env.pageserver.safe_psql(f"checkpoint {env.initial_tenant.hex} {new_timeline_id.hex}")

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_compaction(neon_env_builder: NeonEnvBuilder):
    # Disable background compaction as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = "tenant_config={checkpoint_distance=100000, compaction_period='10m'}"

    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch('test_timeline_physical_size_post_compaction')
    pg = env.postgres.create_start("test_timeline_physical_size_post_compaction")

    pg.safe_psql_many([
        "CREATE TABLE foo (t text)",
        """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
    ])

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    env.pageserver.safe_psql(f"checkpoint {env.initial_tenant.hex} {new_timeline_id.hex}")
    env.pageserver.safe_psql(f"compact {env.initial_tenant.hex} {new_timeline_id.hex}")

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_gc(neon_env_builder: NeonEnvBuilder):
    # Disable background compaction and GC as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = \
        "tenant_config={checkpoint_distance=100000, compaction_period='10m', gc_period='10m', pitr_interval='1s'}"

    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch('test_timeline_physical_size_post_gc')
    pg = env.postgres.create_start("test_timeline_physical_size_post_gc")

    pg.safe_psql_many([
        "CREATE TABLE foo (t text)",
        """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
    ])

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    env.pageserver.safe_psql(f"checkpoint {env.initial_tenant.hex} {new_timeline_id.hex}")

    pg.safe_psql("""
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """)

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    env.pageserver.safe_psql(f"checkpoint {env.initial_tenant.hex} {new_timeline_id.hex}")

    env.pageserver.safe_psql(f"do_gc {env.initial_tenant.hex} {new_timeline_id.hex} 0")

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


# The timeline logical and physical sizes are also exposed as prometheus metrics.
# Test the metrics.
def test_timeline_size_metrics(neon_simple_env: NeonEnv):
    env = neon_simple_env

    new_timeline_id = env.neon_cli.create_branch('test_timeline_size_metrics')
    pg = env.postgres.create_start("test_timeline_size_metrics")

    pg.safe_psql_many([
        "CREATE TABLE foo (t text)",
        """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
    ])

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    env.pageserver.safe_psql(f"checkpoint {env.initial_tenant.hex} {new_timeline_id.hex}")

    # get the metrics and parse the metric for the current timeline's physical size
    metrics = env.pageserver.http_client().get_metrics()
    matches = re.search(
        f'^pageserver_current_physical_size{{tenant_id="{env.initial_tenant.hex}",timeline_id="{new_timeline_id.hex}"}} (\\S+)$',
        metrics,
        re.MULTILINE)
    assert matches
    tl_physical_size_metric = int(matches.group(1))

    # assert that the physical size metric matches the actual physical size on disk
    timeline_path = env.timeline_dir(env.initial_tenant, new_timeline_id)
    assert tl_physical_size_metric == get_timeline_dir_size(timeline_path)

    # Check that the logical size metric is sane, and matches
    matches = re.search(
        f'^pageserver_current_logical_size{{tenant_id="{env.initial_tenant.hex}",timeline_id="{new_timeline_id.hex}"}} (\\S+)$',
        metrics,
        re.MULTILINE)
    assert matches
    tl_logical_size_metric = int(matches.group(1))

    # An empty database is around 8 MB. There at least 3 databases, 'postgres',
    # 'template0', 'template1'. So the total size should be about 32 MB. This isn't
    # very accurate and can change with different PostgreSQL versions, so allow a
    # couple of MB of slack.
    assert math.isclose(tl_logical_size_metric, 32 * 1024 * 1024, abs_tol=2 * 1024 * 1024)

    # The sum of the sizes of all databases, as seen by pg_database_size(), should also
    # be close. Again allow some slack, the logical size metric includes some things like
    # the SLRUs that are not included in pg_database_size().
    dbsize_sum = pg.safe_psql("select sum(pg_database_size(oid)) from pg_database")[0][0]
    assert math.isclose(dbsize_sum, tl_logical_size_metric, abs_tol=2 * 1024 * 1024)


def test_tenant_physical_size(neon_simple_env: NeonEnv):
    random.seed(100)

    env = neon_simple_env
    client = env.pageserver.http_client()

    tenant, timeline = env.neon_cli.create_tenant()

    def get_timeline_physical_size(timeline: UUID):
        res = client.timeline_detail(tenant, timeline)
        return res['local']['current_physical_size_non_incremental']

    timeline_total_size = get_timeline_physical_size(timeline)
    for i in range(10):
        n_rows = random.randint(100, 1000)

        timeline = env.neon_cli.create_branch(f"test_tenant_physical_size_{i}", tenant_id=tenant)
        pg = env.postgres.create_start(f"test_tenant_physical_size_{i}", tenant_id=tenant)

        pg.safe_psql_many([
            "CREATE TABLE foo (t text)",
            f"INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, {n_rows}) g",
        ])

        wait_for_last_flush_lsn(env, pg, tenant, timeline)
        env.pageserver.safe_psql(f"checkpoint {tenant.hex} {timeline.hex}")

        timeline_total_size += get_timeline_physical_size(timeline)

        pg.stop()

    tenant_physical_size = int(client.tenant_status(tenant_id=tenant)['current_physical_size'])
    assert tenant_physical_size == timeline_total_size


def assert_physical_size(env: NeonEnv, tenant_id: UUID, timeline_id: UUID):
    """Check the current physical size returned from timeline API
    matches the total physical size of the timeline on disk"""
    client = env.pageserver.http_client()
    res = assert_timeline_local(client, tenant_id, timeline_id)
    timeline_path = env.timeline_dir(tenant_id, timeline_id)
    assert res["local"]["current_physical_size"] == res["local"][
        "current_physical_size_non_incremental"]
    assert res["local"]["current_physical_size"] == get_timeline_dir_size(timeline_path)
