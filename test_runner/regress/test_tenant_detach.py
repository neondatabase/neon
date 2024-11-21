from __future__ import annotations

import asyncio
import random
import time
from enum import StrEnum
from threading import Thread

import asyncpg
import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    Endpoint,
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_state,
)
from fixtures.remote_storage import (
    RemoteStorageKind,
)
from fixtures.utils import query_scalar, wait_until
from prometheus_client.samples import Sample

# In tests that overlap endpoint activity with tenant attach/detach, there are
# a variety of warnings that the page service may emit when it cannot acquire
# an active tenant to serve a request
PERMIT_PAGE_SERVICE_ERRORS = [
    ".*page_service.*Tenant .* not found",
    ".*page_service.*Tenant .* is not active",
    ".*page_service.*cancelled",
    ".*page_service.*will not become active.*",
]


def do_gc_target(
    pageserver_http: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    """Hack to unblock main, see https://github.com/neondatabase/neon/issues/2211"""
    try:
        log.info("sending gc http request")
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)
    except Exception as e:
        log.error("do_gc failed: %s", e)
    finally:
        log.info("gc http thread returning")


class ReattachMode(StrEnum):
    REATTACH_EXPLICIT = "explicit"
    REATTACH_RESET = "reset"
    REATTACH_RESET_DROP = "reset_drop"


# Basic detach and re-attach test
@pytest.mark.parametrize(
    "mode",
    [ReattachMode.REATTACH_EXPLICIT, ReattachMode.REATTACH_RESET, ReattachMode.REATTACH_RESET_DROP],
)
def test_tenant_reattach(neon_env_builder: NeonEnvBuilder, mode: str):
    # Exercise retry code path by making all uploads and downloads fail for the
    # first time. The retries print INFO-messages to the log; we will check
    # that they are present after the test.
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # create new nenant
    tenant_id, timeline_id = env.create_tenant()

    env.pageserver.allowed_errors.extend(PERMIT_PAGE_SERVICE_ERRORS)

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # Wait for the all data to be processed by the pageserver and uploaded in remote storage
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)

    # Check that we had to retry the uploads
    env.pageserver.assert_log_contains(
        ".*failed to perform remote task UploadLayer.*, will retry.*"
    )
    env.pageserver.assert_log_contains(
        ".*failed to perform remote task UploadMetadata.*, will retry.*"
    )

    ps_metrics = pageserver_http.get_metrics()
    tenant_metric_filter = {
        "tenant_id": str(tenant_id),
        "timeline_id": str(timeline_id),
    }
    pageserver_last_record_lsn_before_detach = int(
        ps_metrics.query_one("pageserver_last_record_lsn", filter=tenant_metric_filter).value
    )

    if mode == ReattachMode.REATTACH_EXPLICIT:
        # Explicitly detach then attach the tenant as two separate API calls
        env.pageserver.tenant_detach(tenant_id)
        env.pageserver.tenant_attach(tenant_id)
    elif mode in (ReattachMode.REATTACH_RESET, ReattachMode.REATTACH_RESET_DROP):
        # Use the reset API to detach/attach in one shot
        pageserver_http.tenant_reset(tenant_id, mode == ReattachMode.REATTACH_RESET_DROP)
    else:
        raise NotImplementedError(mode)

    time.sleep(1)  # for metrics propagation

    ps_metrics = pageserver_http.get_metrics()
    pageserver_last_record_lsn = int(
        ps_metrics.query_one("pageserver_last_record_lsn", filter=tenant_metric_filter).value
    )

    assert pageserver_last_record_lsn_before_detach == pageserver_last_record_lsn

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        with endpoint.cursor() as cur:
            assert query_scalar(cur, "SELECT count(*) FROM t") == 100000

        # Check that we had to retry the downloads
        assert env.pageserver.log_contains(".*list identifiers.*failed, will retry.*")
        assert env.pageserver.log_contains(".*download.*failed, will retry.*")


num_connections = 10
num_rows = 100000

# Detach and re-attach tenant, while compute is busy running queries.
#
# Some of the queries may fail, in the window that the tenant has been
# detached but not yet re-attached. But Postgres itself should keep
# running, and when we retry the queries, they should start working
# after the attach has finished.


# FIXME:
#
# This is pretty unstable at the moment. I've seen it fail with a warning like this:
#
# AssertionError: assert not ['2023-01-05T13:09:40.708303Z  WARN remote_upload{tenant=c3fc41f6cf29a7626b90316e3518cd4b timeline=7978246f85faa71ab03...1282b/000000000000000000000000000000000000-FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF__0000000001716699-0000000001736681"\n']
#
# (https://neon-github-public-dev.s3.amazonaws.com/reports/pr-3232/debug/3846817847/index.html#suites/f9eba3cfdb71aa6e2b54f6466222829b/470fc62b5db7d7d7/)
# I believe that failure happened because there is a race condition
# between detach and starting remote upload tasks:
#
# 1. detach_timeline calls task_mgr::shutdown_tasks(), sending shutdown
#    signal to all in-progress tasks associated with the tenant.
# 2. Just after shutdown_tasks() has collected the list of tasks,
#    a new remote-upload task is spawned.
#
# See https://github.com/neondatabase/neon/issues/3273
#
#
# I also saw this failure:
#
# test_runner/regress/test_tenant_detach.py:194: in test_tenant_reattach_while_busy
#     asyncio.run(reattach_while_busy(env, pg, pageserver_http, tenant_id))
# /home/nonroot/.pyenv/versions/3.9.2/lib/python3.9/asyncio/runners.py:44: in run
#     return loop.run_until_complete(main)
# /home/nonroot/.pyenv/versions/3.9.2/lib/python3.9/asyncio/base_events.py:642: in run_until_complete
#     return future.result()
# test_runner/regress/test_tenant_detach.py:151: in reattach_while_busy
#     assert updates_finished == updates_to_perform
# E   assert 5010 == 10010
# E     +5010
# E     -10010
#
# I don't know what's causing that...
@pytest.mark.skip(reason="fixme")
def test_tenant_reattach_while_busy(
    neon_env_builder: NeonEnvBuilder,
):
    updates_started = 0
    updates_finished = 0
    updates_to_perform = 0

    env = neon_env_builder.init_start()

    # Run random UPDATEs on test table. On failure, try again.
    async def update_table(pg_conn: asyncpg.Connection):
        nonlocal updates_started, updates_finished, updates_to_perform

        while updates_started < updates_to_perform or updates_to_perform == 0:
            updates_started += 1
            id = random.randrange(1, num_rows)

            # Loop to retry until the UPDATE succeeds
            while True:
                try:
                    await pg_conn.fetchrow(f"UPDATE t SET counter = counter + 1 WHERE id = {id}")
                    updates_finished += 1
                    if updates_finished % 1000 == 0:
                        log.info(f"update {updates_finished} / {updates_to_perform}")
                    break
                except asyncpg.PostgresError as e:
                    # Received error from Postgres. Log it, sleep a little, and continue
                    log.info(f"UPDATE error: {e}")
                    await asyncio.sleep(0.1)

    async def sleep_and_reattach(pageserver_http: PageserverHttpClient, tenant_id: TenantId):
        nonlocal updates_started, updates_finished, updates_to_perform

        # Wait until we have performed some updates
        wait_until(20, 0.5, lambda: updates_finished > 500)

        log.info("Detaching tenant")
        pageserver_http.tenant_detach(tenant_id)
        await asyncio.sleep(1)
        log.info("Re-attaching tenant")
        env.pageserver.tenant_attach(tenant_id)
        log.info("Re-attach finished")

        # Continue with 5000 more updates
        updates_to_perform = updates_started + 5000

    # async guts of test_tenant_reattach_while_bysy test
    async def reattach_while_busy(
        env: NeonEnv, endpoint: Endpoint, pageserver_http: PageserverHttpClient, tenant_id: TenantId
    ):
        nonlocal updates_to_perform, updates_finished
        workers = []
        for _ in range(num_connections):
            pg_conn = await endpoint.connect_async()
            workers.append(asyncio.create_task(update_table(pg_conn)))

        workers.append(asyncio.create_task(sleep_and_reattach(pageserver_http, tenant_id)))
        await asyncio.gather(*workers)

        assert updates_finished == updates_to_perform

    pageserver_http = env.pageserver.http_client()

    # create new nenant
    tenant_id, timeline_id = env.create_tenant(
        # Create layers aggressively
        conf={"checkpoint_distance": "100000"}
    )

    # Attempts to connect from compute to pageserver while the tenant is
    # temporarily detached produces these errors in the pageserver log.
    env.pageserver.allowed_errors.extend(PERMIT_PAGE_SERVICE_ERRORS)

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

    cur = endpoint.connect().cursor()

    cur.execute("CREATE TABLE t(id int primary key, counter int)")
    cur.execute(f"INSERT INTO t SELECT generate_series(1,{num_rows}), 0")

    # Run the test
    asyncio.run(reattach_while_busy(env, endpoint, pageserver_http, tenant_id))

    # Verify table contents
    assert query_scalar(cur, "SELECT count(*) FROM t") == num_rows
    assert query_scalar(cur, "SELECT sum(counter) FROM t") == updates_to_perform


def test_tenant_detach_smoke(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start(initial_tenant_conf={"lsn_lease_length": "0s"})
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(PERMIT_PAGE_SERVICE_ERRORS)

    # create new nenant
    tenant_id, timeline_id = env.initial_tenant, env.initial_timeline

    # assert tenant exists on disk
    assert env.pageserver.tenant_dir(tenant_id).exists()

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    endpoint.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    # gc should not try to even start on a timeline that doesn't exist
    with pytest.raises(
        expected_exception=PageserverApiException, match="NotFound: Timeline not found"
    ):
        bogus_timeline_id = TimelineId.generate()
        pageserver_http.timeline_gc(tenant_id, bogus_timeline_id, 0)

    env.pageserver.allowed_errors.extend(
        [
            # the error will be printed to the log too
            ".*NotFound: Timeline not found.*",
            # Timelines get stopped during detach, ignore the gc calls that error, witnessing that
            ".*InternalServerError\\(timeline is Stopping.*",
        ]
    )

    # Detach while running manual GC.
    # It should wait for manual GC to finish because it runs in a task associated with the tenant.
    pageserver_http.configure_failpoints(
        ("gc_iteration_internal_after_getting_gc_timelines", "return(2000)")
    )
    gc_thread = Thread(target=lambda: do_gc_target(pageserver_http, tenant_id, timeline_id))
    gc_thread.start()
    time.sleep(5)
    # By now the gc task is spawned but in sleep for another second due to the failpoint.

    log.info("detaching tenant")
    pageserver_http.tenant_detach(tenant_id)
    log.info("tenant detached without error")

    log.info("wait for gc thread to return")
    gc_thread.join(timeout=10)
    assert not gc_thread.is_alive()
    log.info("gc thread returned")

    # check that nothing is left on disk for deleted tenant
    assert not env.pageserver.tenant_dir(tenant_id).exists()

    with pytest.raises(
        expected_exception=PageserverApiException, match=f"NotFound: tenant {tenant_id}"
    ):
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)


def test_detach_while_attaching(
    neon_env_builder: NeonEnvBuilder,
):
    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    env.pageserver.allowed_errors.extend(PERMIT_PAGE_SERVICE_ERRORS)

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with endpoint.cursor() as cur:
        cur.execute("CREATE TABLE foo (t text)")
        cur.execute(
            """
            INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
            """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    log.info("waiting for upload")

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)
    log.info("upload is done")

    # Detach it
    pageserver_http.tenant_detach(tenant_id)

    # And re-attach
    pageserver_http.configure_failpoints([("attach-before-activate-sleep", "return(5000)")])

    env.pageserver.tenant_attach(tenant_id)

    # Before it has chance to finish, detach it again
    pageserver_http.tenant_detach(tenant_id)

    # is there a better way to assert that failpoint triggered?
    time.sleep(10)

    # Attach it again. If the GC and compaction loops from the previous attach/detach
    # cycle are still running, things could get really confusing..
    env.pageserver.tenant_attach(tenant_id)

    with endpoint.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM foo")


def test_detach_while_activating(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test cancellation behavior for tenants that are stuck somewhere between
    being attached and reaching Active state.
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    endpoint = env.endpoints.create_start("main")

    pageserver_http = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    env.pageserver.allowed_errors.extend(PERMIT_PAGE_SERVICE_ERRORS)

    data_id = 1
    data_secret = "very secret secret"
    insert_test_data(pageserver_http, tenant_id, timeline_id, data_id, data_secret, endpoint)

    tenants_before_detach = [tenant["id"] for tenant in pageserver_http.tenant_list()]

    # Detach it
    pageserver_http.tenant_detach(tenant_id)

    # And re-attach, but stop attach task_mgr task from completing
    pageserver_http.configure_failpoints([("attach-before-activate-sleep", "return(600000)")])
    env.pageserver.tenant_attach(tenant_id)

    # The tenant is in the Activating state.  This should not block us from
    # shutting it down and detaching it.
    pageserver_http.tenant_detach(tenant_id)

    tenants_after_detach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    assert tenant_id not in tenants_after_detach, "Detached tenant should be missing"
    assert len(tenants_after_detach) + 1 == len(
        tenants_before_detach
    ), "Only ignored tenant should be missing"

    # Subsequently attaching it again should still work
    pageserver_http.configure_failpoints([("attach-before-activate-sleep", "off")])
    env.pageserver.tenant_attach(tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Active", 5)

    endpoint.stop()
    endpoint.start()
    ensure_test_data(data_id, data_secret, endpoint)


def insert_test_data(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    data_id: int,
    data: str,
    endpoint: Endpoint,
):
    with endpoint.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE test(id int primary key, secret text);
            INSERT INTO test VALUES ({data_id}, '{data}');
        """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    # wait until pageserver receives that data
    wait_for_last_record_lsn(pageserver_http, tenant_id, timeline_id, current_lsn)

    # run checkpoint manually to be sure that data landed in remote storage
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    # wait until pageserver successfully uploaded a checkpoint to remote storage
    log.info("waiting for to be ignored tenant data checkpoint upload")
    wait_for_upload(pageserver_http, tenant_id, timeline_id, current_lsn)


def ensure_test_data(data_id: int, data: str, endpoint: Endpoint):
    with endpoint.cursor() as cur:
        assert (
            query_scalar(cur, f"SELECT secret FROM test WHERE id = {data_id};") == data
        ), "Should have timeline data back"


def test_metrics_while_ignoring_broken_tenant_and_reloading(
    neon_env_builder: NeonEnvBuilder,
):
    env = neon_env_builder.init_start()

    client = env.pageserver.http_client()
    env.pageserver.allowed_errors.append(
        r".* Changing Active tenant to Broken state, reason: broken from test"
    )

    def only_int(samples: list[Sample]) -> int | None:
        if len(samples) == 1:
            return int(samples[0].value)
        assert len(samples) == 0
        return None

    wait_until_tenant_state(client, env.initial_tenant, "Active", 10, 0.5)

    client.tenant_break(env.initial_tenant)

    def found_broken():
        m = client.get_metrics()
        active = m.query_all("pageserver_tenant_states_count", {"state": "Active"})
        broken = m.query_all("pageserver_tenant_states_count", {"state": "Broken"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        assert only_int(active) == 0 and only_int(broken) == 1 and only_int(broken_set) == 1

    wait_until(10, 0.5, found_broken)

    client.tenant_detach(env.initial_tenant)

    def found_cleaned_up():
        m = client.get_metrics()
        broken = m.query_all("pageserver_tenant_states_count", {"state": "Broken"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        assert only_int(broken) == 0 and len(broken_set) == 0

    wait_until(10, 0.5, found_cleaned_up)

    env.pageserver.tenant_attach(env.initial_tenant)

    def found_active():
        m = client.get_metrics()
        active = m.query_all("pageserver_tenant_states_count", {"state": "Active"})
        broken_set = m.query_all(
            "pageserver_broken_tenants_count", {"tenant_id": str(env.initial_tenant)}
        )
        assert only_int(active) == 1 and len(broken_set) == 0

    wait_until(10, 0.5, found_active)
