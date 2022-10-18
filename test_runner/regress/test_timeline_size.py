import math
import random
import re
import time
from contextlib import closing
from pathlib import Path

import psycopg2.errors
import psycopg2.extras
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    NeonPageserverHttpClient,
    PgBin,
    PortDistributor,
    Postgres,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.types import TenantId, TimelineId
from fixtures.utils import get_timeline_dir_size


def test_timeline_size(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch("test_timeline_size", "empty")

    client = env.pageserver.http_client()
    wait_for_timeline_size_init(client, tenant=env.initial_tenant, timeline=new_timeline_id)

    pgmain = env.postgres.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute(
                """
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10) g
            """
            )

            res = client.timeline_detail(
                env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
            )
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
            cur.execute("TRUNCATE foo")

            res = client.timeline_detail(
                env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
            )
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]


def test_timeline_size_createdropdb(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch("test_timeline_size_createdropdb", "empty")

    client = env.pageserver.http_client()
    wait_for_timeline_size_init(client, tenant=env.initial_tenant, timeline=new_timeline_id)
    timeline_details = client.timeline_detail(
        env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
    )

    pgmain = env.postgres.create_start("test_timeline_size_createdropdb")
    log.info("postgres is running on 'test_timeline_size_createdropdb' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            res = client.timeline_detail(
                env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
            )
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]
            assert (
                timeline_details["current_logical_size_non_incremental"]
                == res["current_logical_size_non_incremental"]
            ), "no writes should not change the incremental logical size"

            cur.execute("CREATE DATABASE foodb")
            with closing(pgmain.connect(dbname="foodb")) as conn:
                with conn.cursor() as cur2:

                    cur2.execute("CREATE TABLE foo (t text)")
                    cur2.execute(
                        """
                        INSERT INTO foo
                            SELECT 'long string to consume some space' || g
                            FROM generate_series(1, 10) g
                    """
                    )

                    res = client.timeline_detail(
                        env.initial_tenant,
                        new_timeline_id,
                        include_non_incremental_logical_size=True,
                    )
                    assert (
                        res["current_logical_size"] == res["current_logical_size_non_incremental"]
                    )

            cur.execute("DROP DATABASE foodb")

            res = client.timeline_detail(
                env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
            )
            assert res["current_logical_size"] == res["current_logical_size_non_incremental"]


# wait until received_lsn_lag is 0
def wait_for_pageserver_catchup(pgmain: Postgres, polling_interval=1, timeout=60):
    started_at = time.time()

    received_lsn_lag = 1
    while received_lsn_lag > 0:
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(
                "timed out waiting for pageserver to reach pg_current_wal_flush_lsn()"
            )

        res = pgmain.safe_psql(
            """
            SELECT
                pg_size_pretty(pg_cluster_size()),
                pg_wal_lsn_diff(pg_current_wal_flush_lsn(), received_lsn) as received_lsn_lag
            FROM backpressure_lsns();
            """
        )[0]
        log.info(f"pg_cluster_size = {res[0]}, received_lsn_lag = {res[1]}")
        received_lsn_lag = res[1]

        time.sleep(polling_interval)


def test_timeline_size_quota(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()
    new_timeline_id = env.neon_cli.create_branch("test_timeline_size_quota")

    wait_for_timeline_size_init(client, tenant=env.initial_tenant, timeline=new_timeline_id)

    pgmain = env.postgres.create_start(
        "test_timeline_size_quota",
        # Set small limit for the test
        config_lines=["neon.max_cluster_size=30MB"],
    )
    log.info("postgres is running on 'test_timeline_size_quota' branch")

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION neon")  # TODO move it to neon_fixtures?

            cur.execute("CREATE TABLE foo (t text)")

            wait_for_pageserver_catchup(pgmain)

            # Insert many rows. This query must fail because of space limit
            try:
                cur.execute(
                    """
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 100000) g
                """
                )

                wait_for_pageserver_catchup(pgmain)

                cur.execute(
                    """
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 500000) g
                """
                )

                # If we get here, the timeline size limit failed
                log.error("Query unexpectedly succeeded")
                assert False

            except psycopg2.errors.DiskFull as err:
                log.info(f"Query expectedly failed with: {err}")

            # drop table to free space
            cur.execute("DROP TABLE foo")

            wait_for_pageserver_catchup(pgmain)

            # create it again and insert some rows. This query must succeed
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute(
                """
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10000) g
            """
            )

            wait_for_pageserver_catchup(pgmain)

            cur.execute("SELECT * from pg_size_pretty(pg_cluster_size())")
            pg_cluster_size = cur.fetchone()
            log.info(f"pg_cluster_size = {pg_cluster_size}")

    new_res = client.timeline_detail(
        env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
    )
    assert (
        new_res["current_logical_size"] == new_res["current_logical_size_non_incremental"]
    ), "after the WAL is streamed, current_logical_size is expected to be calculated and to be equal its non-incremental value"


def test_timeline_physical_size_init(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_init")
    pg = env.postgres.create_start("test_timeline_physical_size_init")

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)

    # restart the pageserer to force calculating timeline's initial physical size
    env.pageserver.stop()
    env.pageserver.start()

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_checkpoint(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()
    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_checkpoint")
    pg = env.postgres.create_start("test_timeline_physical_size_post_checkpoint")

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_compaction(neon_env_builder: NeonEnvBuilder):
    # Disable background compaction as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = (
        "tenant_config={checkpoint_distance=100000, compaction_period='10m'}"
    )

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_compaction")
    pg = env.postgres.create_start("test_timeline_physical_size_post_compaction")

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_compact(env.initial_tenant, new_timeline_id)

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


def test_timeline_physical_size_post_gc(neon_env_builder: NeonEnvBuilder):
    # Disable background compaction and GC as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = "tenant_config={checkpoint_distance=100000, compaction_period='10m', gc_period='10m', pitr_interval='1s'}"

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_gc")
    pg = env.postgres.create_start("test_timeline_physical_size_post_gc")

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    pg.safe_psql(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    pageserver_http.timeline_gc(env.initial_tenant, new_timeline_id, gc_horizon=None)

    assert_physical_size(env, env.initial_tenant, new_timeline_id)


# The timeline logical and physical sizes are also exposed as prometheus metrics.
# Test the metrics.
def test_timeline_size_metrics(
    neon_simple_env: NeonEnv,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    pg_version: str,
):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_size_metrics")
    pg = env.postgres.create_start("test_timeline_size_metrics")

    pg.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, pg, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    # get the metrics and parse the metric for the current timeline's physical size
    metrics = env.pageserver.http_client().get_metrics()
    matches = re.search(
        f'^pageserver_current_physical_size{{tenant_id="{env.initial_tenant}",timeline_id="{new_timeline_id}"}} (\\S+)$',
        metrics,
        re.MULTILINE,
    )
    assert matches
    tl_physical_size_metric = int(matches.group(1))

    # assert that the physical size metric matches the actual physical size on disk
    timeline_path = env.timeline_dir(env.initial_tenant, new_timeline_id)
    assert tl_physical_size_metric == get_timeline_dir_size(timeline_path)

    # Check that the logical size metric is sane, and matches
    matches = re.search(
        f'^pageserver_current_logical_size{{tenant_id="{env.initial_tenant}",timeline_id="{new_timeline_id}"}} (\\S+)$',
        metrics,
        re.MULTILINE,
    )
    assert matches
    tl_logical_size_metric = int(matches.group(1))

    pgdatadir = test_output_dir / "pgdata-vanilla"
    pg_bin = PgBin(test_output_dir, pg_version)
    port = port_distributor.get_port()
    with VanillaPostgres(pgdatadir, pg_bin, port) as vanilla_pg:
        vanilla_pg.configure([f"port={port}"])
        vanilla_pg.start()

        # Create database based on template0 because we can't connect to template0
        vanilla_pg.safe_psql("CREATE TABLE foo (t text)")
        vanilla_pg.safe_psql(
            """INSERT INTO foo
                                SELECT 'long string to consume some space' || g
                                FROM generate_series(1, 100000) g"""
        )
        vanilla_size_sum = vanilla_pg.safe_psql(
            "select sum(pg_database_size(oid)) from pg_database"
        )[0][0]

    # Compare the size with Vanilla postgres.
    # Allow some slack, because the logical size metric includes some things like
    # the SLRUs that are not included in pg_database_size().
    assert math.isclose(tl_logical_size_metric, vanilla_size_sum, abs_tol=2 * 1024 * 1024)

    # The sum of the sizes of all databases, as seen by pg_database_size(), should also
    # be close. Again allow some slack, the logical size metric includes some things like
    # the SLRUs that are not included in pg_database_size().
    dbsize_sum = pg.safe_psql("select sum(pg_database_size(oid)) from pg_database")[0][0]
    assert math.isclose(dbsize_sum, tl_logical_size_metric, abs_tol=2 * 1024 * 1024)


def test_tenant_physical_size(neon_simple_env: NeonEnv):
    random.seed(100)

    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()
    client = env.pageserver.http_client()

    tenant, timeline = env.neon_cli.create_tenant()

    def get_timeline_physical_size(timeline: TimelineId):
        res = client.timeline_detail(tenant, timeline, include_non_incremental_physical_size=True)
        return res["current_physical_size_non_incremental"]

    timeline_total_size = get_timeline_physical_size(timeline)
    for i in range(10):
        n_rows = random.randint(100, 1000)

        timeline = env.neon_cli.create_branch(f"test_tenant_physical_size_{i}", tenant_id=tenant)
        pg = env.postgres.create_start(f"test_tenant_physical_size_{i}", tenant_id=tenant)

        pg.safe_psql_many(
            [
                "CREATE TABLE foo (t text)",
                f"INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, {n_rows}) g",
            ]
        )

        wait_for_last_flush_lsn(env, pg, tenant, timeline)
        pageserver_http.timeline_checkpoint(tenant, timeline)

        timeline_total_size += get_timeline_physical_size(timeline)

        pg.stop()

    tenant_physical_size = int(client.tenant_status(tenant_id=tenant)["current_physical_size"])
    assert tenant_physical_size == timeline_total_size


def assert_physical_size(env: NeonEnv, tenant_id: TenantId, timeline_id: TimelineId):
    """Check the current physical size returned from timeline API
    matches the total physical size of the timeline on disk"""
    client = env.pageserver.http_client()
    res = client.timeline_detail(tenant_id, timeline_id, include_non_incremental_physical_size=True)
    timeline_path = env.timeline_dir(tenant_id, timeline_id)
    assert res["current_physical_size"] == res["current_physical_size_non_incremental"]
    assert res["current_physical_size"] == get_timeline_dir_size(timeline_path)


# Timeline logical size initialization is an asynchronous background task that runs once,
# try a few times to ensure it's activated properly
def wait_for_timeline_size_init(
    client: NeonPageserverHttpClient, tenant: TenantId, timeline: TimelineId
):
    for i in range(10):
        timeline_details = client.timeline_detail(
            tenant, timeline, include_non_incremental_logical_size=True
        )
        current_logical_size = timeline_details["current_logical_size"]
        non_incremental = timeline_details["current_logical_size_non_incremental"]
        if current_logical_size == non_incremental:
            return
        log.info(
            f"waiting for current_logical_size of a timeline to be calculated, iteration {i}: {current_logical_size} vs {non_incremental}"
        )
        time.sleep(1)
    raise Exception(
        f"timed out while waiting for current_logical_size of a timeline to reach its non-incremental value, details: {timeline_details}"
    )
