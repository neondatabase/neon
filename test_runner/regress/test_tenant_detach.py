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
    env.pageserver.allowed_errors.append('.*Conflict("Cannot attach.*')

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


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.NOOP, RemoteStorageKind.MOCK_S3])
def test_ignored_tenant_reattach(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_backup_and_restore",
    )
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.append(
        ".*as locally complete, while it doesnt exist in remote index.*"
    )
    env.pageserver.allowed_errors.append(
        '.*Conflict\\("Cannot attach: tenant .*? is not in memory, but has local files.*'
    )
    pageserver_http = env.pageserver.http_client()

    ignored_tenant_id, _ = env.neon_cli.create_tenant()
    tenant_dir = env.repo_dir / "tenants" / str(ignored_tenant_id)
    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_before_ignore.sort()
    timelines_before_ignore = [
        timeline["timeline_id"]
        for timeline in pageserver_http.timeline_list(tenant_id=ignored_tenant_id)
    ]
    files_before_ignore = [tenant_path for tenant_path in tenant_dir.glob("**/*")]

    # ignore the tenant and veirfy it's not present in pageserver replies, with its files still on disk
    pageserver_http.tenant_ignore(ignored_tenant_id)

    files_after_ignore_with_retain = [tenant_path for tenant_path in tenant_dir.glob("**/*")]
    new_files = set(files_after_ignore_with_retain) - set(files_before_ignore)
    disappeared_files = set(files_before_ignore) - set(files_after_ignore_with_retain)
    assert (
        len(disappeared_files) == 0
    ), f"Tenant ignore should not remove files from disk, missing: {disappeared_files}"
    assert (
        len(new_files) == 1
    ), f"Only tenant ignore file should appear on disk but got: {new_files}"

    tenants_after_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    assert ignored_tenant_id not in tenants_after_ignore, "Ignored tenant should be missing"
    assert len(tenants_after_ignore) + 1 == len(
        tenants_before_ignore
    ), "Only ignored tenant should be missing"

    # ensure we cannot attach a timeline with local files on disk
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"Conflict: Cannot attach: tenant {ignored_tenant_id} is not in memory, but has local files",
    ):
        pageserver_http.tenant_attach(tenant_id=ignored_tenant_id)

    # restart the pageserver to ensure we don't load the ignore timeline
    env.pageserver.stop()
    env.pageserver.start()
    tenants_after_restart = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_restart.sort()
    assert (
        tenants_after_restart == tenants_after_ignore
    ), "Ignored tenant should not be reloaded after pageserver restart"

    # now, attach with the local files and expect it works
    pageserver_http.tenant_attach(tenant_id=ignored_tenant_id, allow_local_files=True)
    tenants_after_attach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_attach.sort()
    assert tenants_after_attach == tenants_before_ignore, "Should have all tenants back"

    timelines_after_ignore = [
        timeline["timeline_id"]
        for timeline in pageserver_http.timeline_list(tenant_id=ignored_tenant_id)
    ]
    assert timelines_before_ignore == timelines_after_ignore, "Should have all timelines back"
