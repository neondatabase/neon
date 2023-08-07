import math
import queue
import random
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Optional

import psycopg2.errors
import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    assert_tenant_state,
    timeline_delete_wait_completed,
    wait_for_upload_queue_empty,
    wait_until_tenant_active,
)
from fixtures.pg_version import PgVersion
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import RemoteStorageKind
from fixtures.types import TenantId, TimelineId
from fixtures.utils import get_timeline_dir_size, wait_until


def test_timeline_size(neon_simple_env: NeonEnv):
    env = neon_simple_env
    new_timeline_id = env.neon_cli.create_branch("test_timeline_size", "empty")

    client = env.pageserver.http_client()
    wait_for_timeline_size_init(client, tenant=env.initial_tenant, timeline=new_timeline_id)

    endpoint_main = env.endpoints.create_start("test_timeline_size")
    log.info("postgres is running on 'test_timeline_size' branch")

    with closing(endpoint_main.connect()) as conn:
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

    endpoint_main = env.endpoints.create_start("test_timeline_size_createdropdb")
    log.info("postgres is running on 'test_timeline_size_createdropdb' branch")

    with closing(endpoint_main.connect()) as conn:
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
            with closing(endpoint_main.connect(dbname="foodb")) as conn:
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
def wait_for_pageserver_catchup(endpoint_main: Endpoint, polling_interval=1, timeout=60):
    started_at = time.time()

    received_lsn_lag = 1
    while received_lsn_lag > 0:
        elapsed = time.time() - started_at
        if elapsed > timeout:
            raise RuntimeError(
                "timed out waiting for pageserver to reach pg_current_wal_flush_lsn()"
            )

        res = endpoint_main.safe_psql(
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

    endpoint_main = env.endpoints.create_start(
        "test_timeline_size_quota",
        # Set small limit for the test
        config_lines=["neon.max_cluster_size=30MB"],
    )
    log.info("postgres is running on 'test_timeline_size_quota' branch")

    with closing(endpoint_main.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION neon")  # TODO move it to neon_fixtures?

            cur.execute("CREATE TABLE foo (t text)")

            wait_for_pageserver_catchup(endpoint_main)

            # Insert many rows. This query must fail because of space limit
            try:
                cur.execute(
                    """
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 100000) g
                """
                )

                wait_for_pageserver_catchup(endpoint_main)

                cur.execute(
                    """
                    INSERT INTO foo
                        SELECT 'long string to consume some space' || g
                        FROM generate_series(1, 500000) g
                """
                )

                # If we get here, the timeline size limit failed
                log.error("Query unexpectedly succeeded")
                raise AssertionError()

            except psycopg2.errors.DiskFull as err:
                log.info(f"Query expectedly failed with: {err}")

            # drop table to free space
            cur.execute("DROP TABLE foo")

            wait_for_pageserver_catchup(endpoint_main)

            # create it again and insert some rows. This query must succeed
            cur.execute("CREATE TABLE foo (t text)")
            cur.execute(
                """
                INSERT INTO foo
                    SELECT 'long string to consume some space' || g
                    FROM generate_series(1, 10000) g
            """
            )

            wait_for_pageserver_catchup(endpoint_main)

            cur.execute("SELECT * from pg_size_pretty(pg_cluster_size())")
            pg_cluster_size = cur.fetchone()
            log.info(f"pg_cluster_size = {pg_cluster_size}")

    new_res = client.timeline_detail(
        env.initial_tenant, new_timeline_id, include_non_incremental_logical_size=True
    )
    assert (
        new_res["current_logical_size"] == new_res["current_logical_size_non_incremental"]
    ), "after the WAL is streamed, current_logical_size is expected to be calculated and to be equal its non-incremental value"


@pytest.mark.parametrize("deletion_method", ["tenant_detach", "timeline_delete"])
def test_timeline_initial_logical_size_calculation_cancellation(
    neon_env_builder: NeonEnvBuilder, deletion_method: str
):
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # load in some data
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (x INTEGER)",
            "INSERT INTO foo SELECT g FROM generate_series(1, 10000) g",
        ]
    )
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop()

    # restart with failpoint inside initial size calculation task
    env.pageserver.stop()
    env.pageserver.start(
        extra_env_vars={"FAILPOINTS": "timeline-calculate-logical-size-pause=pause"}
    )

    wait_until_tenant_active(client, tenant_id)

    # kick off initial size calculation task (the response we get here is the estimated size)
    def assert_size_calculation_not_done():
        details = client.timeline_detail(
            tenant_id, timeline_id, include_non_incremental_logical_size=True
        )
        assert details["current_logical_size"] != details["current_logical_size_non_incremental"]

    assert_size_calculation_not_done()
    # ensure we're really stuck
    time.sleep(5)
    assert_size_calculation_not_done()

    log.info(
        f"try to delete the timeline using {deletion_method}, this should cancel size computation tasks and wait for them to finish"
    )
    delete_timeline_success: queue.Queue[bool] = queue.Queue(maxsize=1)

    def delete_timeline_thread_fn():
        try:
            if deletion_method == "tenant_detach":
                client.tenant_detach(tenant_id)
            elif deletion_method == "timeline_delete":
                timeline_delete_wait_completed(client, tenant_id, timeline_id)
            delete_timeline_success.put(True)
        except PageserverApiException:
            delete_timeline_success.put(False)
            raise

    delete_timeline_thread = threading.Thread(target=delete_timeline_thread_fn)
    delete_timeline_thread.start()
    # give it some time to settle in the state where it waits for size computation task
    time.sleep(5)
    if not delete_timeline_success.empty():
        raise AssertionError(
            f"test is broken, the {deletion_method} should be stuck waiting for size computation task, got result {delete_timeline_success.get()}"
        )

    log.info(
        "resume the size calculation. The failpoint checks that the timeline directory still exists."
    )
    client.configure_failpoints(("timeline-calculate-logical-size-check-dir-exists", "return"))
    client.configure_failpoints(("timeline-calculate-logical-size-pause", "off"))

    log.info("wait for delete timeline thread to finish and assert that it succeeded")
    assert delete_timeline_success.get()

    # if the implementation is incorrect, the teardown would complain about an error log
    # message emitted by the code behind failpoint "timeline-calculate-logical-size-check-dir-exists"


@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_timeline_physical_size_init(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: Optional[RemoteStorageKind]
):
    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_timeline_physical_size_init"
        )

    env = neon_env_builder.init_start()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_init")
    endpoint = env.endpoints.create_start("test_timeline_physical_size_init")

    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)

    # restart the pageserer to force calculating timeline's initial physical size
    env.pageserver.stop()
    env.pageserver.start()

    # Wait for the tenant to be loaded
    client = env.pageserver.http_client()
    wait_until(
        number_of_iterations=5,
        interval=1,
        func=lambda: assert_tenant_state(client, env.initial_tenant, "Active"),
    )

    assert_physical_size_invariants(
        get_physical_size_values(env, env.initial_tenant, new_timeline_id, remote_storage_kind),
        remote_storage_kind,
    )


@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_timeline_physical_size_post_checkpoint(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: Optional[RemoteStorageKind]
):
    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_timeline_physical_size_init"
        )

    env = neon_env_builder.init_start()

    pageserver_http = env.pageserver.http_client()
    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_checkpoint")
    endpoint = env.endpoints.create_start("test_timeline_physical_size_post_checkpoint")

    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 1000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    assert_physical_size_invariants(
        get_physical_size_values(env, env.initial_tenant, new_timeline_id, remote_storage_kind),
        remote_storage_kind,
    )


@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_timeline_physical_size_post_compaction(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: Optional[RemoteStorageKind]
):
    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_timeline_physical_size_init"
        )

    # Disable background compaction as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = (
        "tenant_config={checkpoint_distance=100000, compaction_period='10m'}"
    )

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_compaction")
    endpoint = env.endpoints.create_start("test_timeline_physical_size_post_compaction")

    # We don't want autovacuum to run on the table, while we are calculating the
    # physical size, because that could cause a new layer to be created and a
    # mismatch between the incremental and non-incremental size. (If that still
    # happens, because of some other background activity or autovacuum on other
    # tables, we could simply retry the size calculations. It's unlikely that
    # that would happen more than once.)
    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)

    # shutdown safekeepers to prevent new data from coming in
    endpoint.stop()  # We can't gracefully stop after safekeepers die
    for sk in env.safekeepers:
        sk.stop()

    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_compact(env.initial_tenant, new_timeline_id)

    if remote_storage_kind is not None:
        wait_for_upload_queue_empty(pageserver_http, env.initial_tenant, new_timeline_id)

    assert_physical_size_invariants(
        get_physical_size_values(env, env.initial_tenant, new_timeline_id, remote_storage_kind),
        remote_storage_kind,
    )


@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_timeline_physical_size_post_gc(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: Optional[RemoteStorageKind]
):
    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_timeline_physical_size_init"
        )

    # Disable background compaction and GC as we don't want it to happen after `get_physical_size` request
    # and before checking the expected size on disk, which makes the assertion failed
    neon_env_builder.pageserver_config_override = "tenant_config={checkpoint_distance=100000, compaction_period='0s', gc_period='0s', pitr_interval='1s'}"

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_physical_size_post_gc")
    endpoint = env.endpoints.create_start("test_timeline_physical_size_post_gc")

    # Like in test_timeline_physical_size_post_compaction, disable autovacuum
    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    endpoint.safe_psql(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
    """
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_gc(env.initial_tenant, new_timeline_id, gc_horizon=None)

    if remote_storage_kind is not None:
        wait_for_upload_queue_empty(pageserver_http, env.initial_tenant, new_timeline_id)

    assert_physical_size_invariants(
        get_physical_size_values(env, env.initial_tenant, new_timeline_id, remote_storage_kind),
        remote_storage_kind,
    )


# The timeline logical and physical sizes are also exposed as prometheus metrics.
# Test the metrics.
def test_timeline_size_metrics(
    neon_simple_env: NeonEnv,
    test_output_dir: Path,
    port_distributor: PortDistributor,
    pg_distrib_dir: Path,
    pg_version: PgVersion,
):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    new_timeline_id = env.neon_cli.create_branch("test_timeline_size_metrics")
    endpoint = env.endpoints.create_start("test_timeline_size_metrics")

    endpoint.safe_psql_many(
        [
            "CREATE TABLE foo (t text)",
            """INSERT INTO foo
           SELECT 'long string to consume some space' || g
           FROM generate_series(1, 100000) g""",
        ]
    )

    wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, new_timeline_id)
    pageserver_http.timeline_checkpoint(env.initial_tenant, new_timeline_id)

    # get the metrics and parse the metric for the current timeline's physical size
    metrics = env.pageserver.http_client().get_metrics()
    tl_physical_size_metric = metrics.query_one(
        name="pageserver_resident_physical_size",
        filter={
            "tenant_id": str(env.initial_tenant),
            "timeline_id": str(new_timeline_id),
        },
    ).value

    # assert that the physical size metric matches the actual physical size on disk
    timeline_path = env.timeline_dir(env.initial_tenant, new_timeline_id)
    assert tl_physical_size_metric == get_timeline_dir_size(timeline_path)

    # Check that the logical size metric is sane, and matches
    tl_logical_size_metric = metrics.query_one(
        name="pageserver_current_logical_size",
        filter={
            "tenant_id": str(env.initial_tenant),
            "timeline_id": str(new_timeline_id),
        },
    ).value

    pgdatadir = test_output_dir / "pgdata-vanilla"
    pg_bin = PgBin(test_output_dir, pg_distrib_dir, pg_version)
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
    dbsize_sum = endpoint.safe_psql("select sum(pg_database_size(oid)) from pg_database")[0][0]
    assert math.isclose(dbsize_sum, tl_logical_size_metric, abs_tol=2 * 1024 * 1024)


@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_tenant_physical_size(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: Optional[RemoteStorageKind]
):
    random.seed(100)

    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_timeline_physical_size_init"
        )

    env = neon_env_builder.init_start()

    pageserver_http = env.pageserver.http_client()
    client = env.pageserver.http_client()

    tenant, timeline = env.neon_cli.create_tenant()
    if remote_storage_kind is not None:
        wait_for_upload_queue_empty(pageserver_http, tenant, timeline)

    def get_timeline_resident_physical_size(timeline: TimelineId):
        sizes = get_physical_size_values(env, tenant, timeline, remote_storage_kind)
        assert_physical_size_invariants(sizes, remote_storage_kind)
        return sizes.prometheus_resident_physical

    timeline_total_resident_physical_size = get_timeline_resident_physical_size(timeline)
    for i in range(10):
        n_rows = random.randint(100, 1000)

        timeline = env.neon_cli.create_branch(f"test_tenant_physical_size_{i}", tenant_id=tenant)
        endpoint = env.endpoints.create_start(f"test_tenant_physical_size_{i}", tenant_id=tenant)

        endpoint.safe_psql_many(
            [
                "CREATE TABLE foo (t text)",
                f"INSERT INTO foo SELECT 'long string to consume some space' || g FROM generate_series(1, {n_rows}) g",
            ]
        )

        wait_for_last_flush_lsn(env, endpoint, tenant, timeline)
        pageserver_http.timeline_checkpoint(tenant, timeline)

        if remote_storage_kind is not None:
            wait_for_upload_queue_empty(pageserver_http, tenant, timeline)

        timeline_total_resident_physical_size += get_timeline_resident_physical_size(timeline)

        endpoint.stop()

    # ensure that tenant_status current_physical size reports sum of timeline current_physical_size
    tenant_current_physical_size = int(
        client.tenant_status(tenant_id=tenant)["current_physical_size"]
    )
    assert tenant_current_physical_size == sum(
        [tl["current_physical_size"] for tl in client.timeline_list(tenant_id=tenant)]
    )
    # since we don't do layer eviction, current_physical_size is identical to resident physical size
    assert timeline_total_resident_physical_size == tenant_current_physical_size


class TimelinePhysicalSizeValues:
    api_current_physical: int
    prometheus_resident_physical: float
    prometheus_remote_physical: Optional[float] = None
    python_timelinedir_layerfiles_physical: int
    layer_map_file_size_sum: int


def get_physical_size_values(
    env: NeonEnv,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    remote_storage_kind: Optional[RemoteStorageKind],
) -> TimelinePhysicalSizeValues:
    res = TimelinePhysicalSizeValues()

    client = env.pageserver.http_client()

    res.layer_map_file_size_sum = sum(
        layer.layer_file_size or 0
        for layer in client.layer_map_info(tenant_id, timeline_id).historic_layers
    )

    metrics = client.get_metrics()
    metrics_filter = {"tenant_id": str(tenant_id), "timeline_id": str(timeline_id)}
    res.prometheus_resident_physical = metrics.query_one(
        "pageserver_resident_physical_size", metrics_filter
    ).value
    if remote_storage_kind is not None:
        res.prometheus_remote_physical = metrics.query_one(
            "pageserver_remote_physical_size", metrics_filter
        ).value
    else:
        res.prometheus_remote_physical = None

    detail = client.timeline_detail(
        tenant_id, timeline_id, include_timeline_dir_layer_file_size_sum=True
    )
    res.api_current_physical = detail["current_physical_size"]

    timeline_path = env.timeline_dir(tenant_id, timeline_id)
    res.python_timelinedir_layerfiles_physical = get_timeline_dir_size(timeline_path)

    return res


def assert_physical_size_invariants(
    sizes: TimelinePhysicalSizeValues, remote_storage_kind: Optional[RemoteStorageKind]
):
    # resident phyiscal size is defined as
    assert sizes.python_timelinedir_layerfiles_physical == sizes.prometheus_resident_physical
    assert sizes.python_timelinedir_layerfiles_physical == sizes.layer_map_file_size_sum

    # we don't do layer eviction, so, all layers are resident
    assert sizes.api_current_physical == sizes.prometheus_resident_physical
    if remote_storage_kind is not None:
        assert sizes.prometheus_resident_physical == sizes.prometheus_remote_physical
        # XXX would be nice to assert layer file physical storage utilization here as well, but we can only do that for LocalFS
    else:
        assert sizes.prometheus_remote_physical is None


# Timeline logical size initialization is an asynchronous background task that runs once,
# try a few times to ensure it's activated properly
def wait_for_timeline_size_init(
    client: PageserverHttpClient, tenant: TenantId, timeline: TimelineId
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
