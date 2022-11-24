import time
from threading import Thread

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PageserverApiException,
    PageserverHttpClient,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


def do_gc_target(
    pageserver_http: PageserverHttpClient, tenant_id: TenantId, timeline_id: TimelineId
):
    """Hack to unblock main, see https://github.com/neondatabase/neon/issues/2211"""
    try:
        log.info("sending gc http request")
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)
    except Exception as e:
        log.error("do_gc failed: %s", e)
    finally:
        log.info("gc http thread returning")


def test_tenant_detach_smoke(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.append(".*NotFound\\(Tenant .* not found")

    # first check for non existing tenant
    tenant_id = TenantId.generate()
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"Tenant not found for id {tenant_id}",
    ):
        pageserver_http.tenant_detach(tenant_id)

    # the error will be printed to the log too
    env.pageserver.allowed_errors.append(".*Tenant not found for id.*")

    # create new nenant
    tenant_id, timeline_id = env.neon_cli.create_tenant()

    # assert tenant exists on disk
    assert (env.repo_dir / "tenants" / str(tenant_id)).exists()

    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    # we rely upon autocommit after each statement
    pg.safe_psql_many(
        queries=[
            "CREATE TABLE t(key int primary key, value text)",
            "INSERT INTO t SELECT generate_series(1,100000), 'payload'",
        ]
    )

    # gc should not try to even start on a timeline that doesn't exist
    with pytest.raises(
        expected_exception=PageserverApiException, match="gc target timeline does not exist"
    ):
        bogus_timeline_id = TimelineId.generate()
        pageserver_http.timeline_gc(tenant_id, bogus_timeline_id, 0)

        # the error will be printed to the log too
    env.pageserver.allowed_errors.append(".*gc target timeline does not exist.*")

    # Detach while running manual GC.
    # It should wait for manual GC to finish because it runs in a task associated with the tenant.
    pageserver_http.configure_failpoints(
        ("gc_iteration_internal_after_getting_gc_timelines", "return(2000)")
    )
    gc_thread = Thread(target=lambda: do_gc_target(pageserver_http, tenant_id, timeline_id))
    gc_thread.start()
    time.sleep(1)
    # By now the gc task is spawned but in sleep for another second due to the failpoint.

    log.info("detaching tenant")
    pageserver_http.tenant_detach(tenant_id)
    log.info("tenant detached without error")

    log.info("wait for gc thread to return")
    gc_thread.join(timeout=10)
    assert not gc_thread.is_alive()
    log.info("gc thread returned")

    # check that nothing is left on disk for deleted tenant
    assert not (env.repo_dir / "tenants" / str(tenant_id)).exists()

    with pytest.raises(
        expected_exception=PageserverApiException, match=f"Tenant {tenant_id} not found"
    ):
        pageserver_http.timeline_gc(tenant_id, timeline_id, 0)


#
@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_detach_while_attaching(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_detach_while_attaching",
    )

    ##### First start, insert secret data and upload it to the remote storage
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    client = env.pageserver.http_client()

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    with pg.cursor() as cur:
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
    pageserver_http.configure_failpoints([("attach-before-activate", "return(5000)")])

    pageserver_http.tenant_attach(tenant_id)

    # Before it has chance to finish, detach it again
    pageserver_http.tenant_detach(tenant_id)

    # is there a better way to assert that failpoint triggered?
    time.sleep(10)

    # Attach it again. If the GC and compaction loops from the previous attach/detach
    # cycle are still running, things could get really confusing..
    pageserver_http.tenant_attach(tenant_id)

    with pg.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM foo")
