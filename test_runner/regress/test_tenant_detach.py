import time
from threading import Thread

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PageserverApiException,
    PageserverHttpClient,
    RemoteStorageKind,
)
from fixtures.types import TenantId, TimelineId


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


@pytest.mark.skip(
    reason="""
Commit 'make test_tenant_detach_smoke fail reproducibly' adds failpoint to make this test fail reproducibly.
Fix in https://github.com/neondatabase/neon/pull/2851 will come as part of
https://github.com/neondatabase/neon/pull/2785 .
"""
)
def test_tenant_detach_smoke(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.append(".*NotFound\\(Tenant .* not found in the local state")
    # FIXME: we have a race condition between GC and detach. GC might fail with this
    # error. Similar to https://github.com/neondatabase/neon/issues/2671
    env.pageserver.allowed_errors.append(".*InternalServerError\\(No such file or directory.*")

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
    # It should wait for manual GC to finish (right now it doesn't that's why this test fails sometimes)
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


# TODO kb rename and fix the test style
# @pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.NOOP])
def test_tenant_detach_zzz(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_backup_and_restore",
    )
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    tenant_id, timeline_id = env.neon_cli.create_tenant()
    tenant_dir = env.repo_dir / "tenants" / str(tenant_id)

    tenant_status_before_detach = pageserver_http.tenant_status(tenant_id=tenant_id)
    timeline_info_before_detach = pageserver_http.timeline_detail(
        tenant_id=tenant_id, timeline_id=timeline_id
    )
    files_before_detach = [tenant_path for tenant_path in tenant_dir.glob("**/*")]
    files_before_detach.sort()

    pageserver_http.tenant_detach(tenant_id)

    files_after_detach_with_retain = [tenant_path for tenant_path in tenant_dir.glob("**/*")]
    files_after_detach_with_retain.sort()
    assert (
        files_before_detach == files_after_detach_with_retain
    ), f"Expected after detach with retain to have exactly the same files, but missing {set(files_before_detach) - set(files_after_detach_with_retain)} and have extra {set(files_after_detach_with_retain) - set(files_before_detach)} files"

    pageserver_http.tenant_attach(tenant_id=tenant_id)

    tenant_status_after_reattach = pageserver_http.tenant_status(tenant_id=tenant_id)
    assert (
        tenant_status_before_detach == tenant_status_after_reattach
    ), f"Tenant after reattach expected to have status {tenant_status_before_detach}, but got {tenant_status_after_reattach}"

    timeline_info_after_reattach = pageserver_http.timeline_detail(
        tenant_id=tenant_id, timeline_id=timeline_id
    )
    assert (
        timeline_info_before_detach == timeline_info_after_reattach
    ), f"Tenant timeline after reattach expected to have info {timeline_info_before_detach}, but got {timeline_info_after_reattach}"
