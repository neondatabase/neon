import time
from threading import Thread

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PageserverApiException,
    PageserverHttpClient,
    Postgres,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_state,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


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


# Tests that `ignore` and `get` operations' combination is able to remove and restore the tenant in pageserver's memory.
# * writes some data into tenant's timeline
# * ensures it's synced with the remote storage
# * `ignore` the tenant
# * verify that ignored tenant files are generally unchanged, only an ignored mark had appeared
# * verify the ignored tenant is gone from pageserver's memory
# * restart the pageserver and verify that ignored tenant is still not loaded
# * `load` the same tenant
# * ensure that it's status is `Active` and it's present in pageserver's memory with all timelines
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.NOOP, RemoteStorageKind.MOCK_S3])
def test_ignored_tenant_reattach(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_remote_storage_backup_and_restore",
    )
    env = neon_env_builder.init_start()
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

    # restart the pageserver to ensure we don't load the ignore timeline
    env.pageserver.stop()
    env.pageserver.start()
    tenants_after_restart = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_restart.sort()
    assert (
        tenants_after_restart == tenants_after_ignore
    ), "Ignored tenant should not be reloaded after pageserver restart"

    # now, load it from the local files and expect it works
    pageserver_http.tenant_load(tenant_id=ignored_tenant_id)
    wait_until_tenant_state(pageserver_http, ignored_tenant_id, "Active", 5)

    tenants_after_attach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_attach.sort()
    assert tenants_after_attach == tenants_before_ignore, "Should have all tenants back"

    timelines_after_ignore = [
        timeline["timeline_id"]
        for timeline in pageserver_http.timeline_list(tenant_id=ignored_tenant_id)
    ]
    assert timelines_before_ignore == timelines_after_ignore, "Should have all timelines back"


# Tests that it's possible to `load` tenants with missing layers and get them restored:
# * writes some data into tenant's timeline
# * ensures it's synced with the remote storage
# * `ignore` the tenant
# * removes all timeline's local layers
# * `load` the same tenant
# * ensure that it's status is `Active`
# * check that timeline data is restored
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignored_tenant_download_missing_layers(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ignored_tenant_download_and_attach",
    )
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    data_id = 1
    data_secret = "very secret secret"
    insert_test_data(pageserver_http, tenant_id, timeline_id, data_id, data_secret, pg)

    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_before_ignore.sort()
    timelines_before_ignore = [
        timeline["timeline_id"] for timeline in pageserver_http.timeline_list(tenant_id=tenant_id)
    ]

    # ignore the tenant and remove its layers
    pageserver_http.tenant_ignore(tenant_id)
    tenant_timeline_dir = env.repo_dir / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    layers_removed = False
    for dir_entry in tenant_timeline_dir.iterdir():
        if dir_entry.name.startswith("00000"):
            # Looks like a layer file. Remove it
            dir_entry.unlink()
            layers_removed = True
    assert layers_removed, f"Found no layers for tenant {tenant_timeline_dir}"

    # now, load it from the local files and expect it to work due to remote storage restoration
    pageserver_http.tenant_load(tenant_id=tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Active", 5)

    tenants_after_attach = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    tenants_after_attach.sort()
    assert tenants_after_attach == tenants_before_ignore, "Should have all tenants back"

    timelines_after_ignore = [
        timeline["timeline_id"] for timeline in pageserver_http.timeline_list(tenant_id=tenant_id)
    ]
    assert timelines_before_ignore == timelines_after_ignore, "Should have all timelines back"

    pg.stop()
    pg.start()
    ensure_test_data(data_id, data_secret, pg)


# Tests that it's possible to `load` broken tenants:
# * `ignore` a tenant
# * removes its `metadata` file locally
# * `load` the same tenant
# * ensure that it's status is `Broken`
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignored_tenant_stays_broken_without_metadata(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ignored_tenant_stays_broken_without_metadata",
    )
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    # ignore the tenant and remove its metadata
    pageserver_http.tenant_ignore(tenant_id)
    tenant_timeline_dir = env.repo_dir / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    metadata_removed = False
    for dir_entry in tenant_timeline_dir.iterdir():
        if dir_entry.name == "metadata":
            # Looks like a layer file. Remove it
            dir_entry.unlink()
            metadata_removed = True
    assert metadata_removed, f"Failed to find metadata file in {tenant_timeline_dir}"

    env.pageserver.allowed_errors.append(".*could not load tenant .*?: failed to load metadata.*")

    # now, load it from the local files and expect it to be broken due to inability to load tenant files into memory
    pageserver_http.tenant_load(tenant_id=tenant_id)
    wait_until_tenant_state(pageserver_http, tenant_id, "Broken", 5)


# Tests that attach is never working on a tenant, ignored or not, as long as it's not absent locally
# Similarly, tests that it's not possible to schedule a `load` for tenat that's not ignored.
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_load_attach_negatives(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_load_attach_negatives",
    )
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])

    env.pageserver.allowed_errors.append(".*tenant .*? already exists, state:.*")
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"tenant {tenant_id} already exists, state: Active",
    ):
        pageserver_http.tenant_load(tenant_id)

    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"tenant {tenant_id} already exists, state: Active",
    ):
        pageserver_http.tenant_attach(tenant_id)

    pageserver_http.tenant_ignore(tenant_id)

    env.pageserver.allowed_errors.append(
        ".*Cannot attach tenant .*?, local tenant directory already exists.*"
    )
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"Cannot attach tenant {tenant_id}, local tenant directory already exists",
    ):
        pageserver_http.tenant_attach(tenant_id)


@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_ignore_while_attaching(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_ignore_while_attaching",
    )

    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    pageserver_http = env.pageserver.http_client()

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    data_id = 1
    data_secret = "very secret secret"
    insert_test_data(pageserver_http, tenant_id, timeline_id, data_id, data_secret, pg)

    tenants_before_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]

    # Detach it
    pageserver_http.tenant_detach(tenant_id)
    # And re-attach, but stop attach task_mgr task from completing
    pageserver_http.configure_failpoints([("attach-before-activate", "return(5000)")])
    pageserver_http.tenant_attach(tenant_id)
    # Run ignore on the task, thereby cancelling the attach.
    # XXX This should take priority over attach, i.e., it should cancel the attach task.
    # But neither the failpoint, nor the proper storage_sync2 download functions,
    # are sensitive to task_mgr::shutdown.
    # This problem is tracked in https://github.com/neondatabase/neon/issues/2996 .
    # So, for now, effectively, this ignore here will block until attach task completes.
    pageserver_http.tenant_ignore(tenant_id)

    # Cannot attach it due to some local files existing
    env.pageserver.allowed_errors.append(
        ".*Cannot attach tenant .*?, local tenant directory already exists.*"
    )
    with pytest.raises(
        expected_exception=PageserverApiException,
        match=f"Cannot attach tenant {tenant_id}, local tenant directory already exists",
    ):
        pageserver_http.tenant_attach(tenant_id)

    tenants_after_ignore = [tenant["id"] for tenant in pageserver_http.tenant_list()]
    assert tenant_id not in tenants_after_ignore, "Ignored tenant should be missing"
    assert len(tenants_after_ignore) + 1 == len(
        tenants_before_ignore
    ), "Only ignored tenant should be missing"

    # But can load it from local files, that will restore attach.
    pageserver_http.tenant_load(tenant_id)

    wait_until_tenant_state(pageserver_http, tenant_id, "Active", 5)

    pg.stop()
    pg.start()
    ensure_test_data(data_id, data_secret, pg)


def insert_test_data(
    pageserver_http: PageserverHttpClient,
    tenant_id: TenantId,
    timeline_id: TimelineId,
    data_id: int,
    data: str,
    pg: Postgres,
):
    with pg.cursor() as cur:
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


def ensure_test_data(data_id: int, data: str, pg: Postgres):
    with pg.cursor() as cur:
        assert (
            query_scalar(cur, f"SELECT secret FROM test WHERE id = {data_id};") == data
        ), "Should have timeline data back"
