import concurrent.futures
import enum
import os
import shutil
from threading import Thread

import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    StorageScrubber,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    assert_prefix_empty,
    assert_prefix_not_empty,
    poll_for_remote_storage_iterations,
    tenant_delete_wait_completed,
    wait_for_upload,
    wait_tenant_status_404,
    wait_until_tenant_active,
    wait_until_tenant_state,
)
from fixtures.remote_storage import RemoteStorageKind, available_s3_storages, s3_storage
from fixtures.utils import run_pg_bench_small, wait_until
from requests.exceptions import ReadTimeout


def test_tenant_delete_smoke(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [
            # The deletion queue will complain when it encounters simulated S3 errors
            ".*deletion executor: DeleteObjects request failed.*",
            # lucky race with stopping from flushing a layer we fail to schedule any uploads
            ".*layer flush task.+: could not flush frozen layer: update_metadata_file",
        ]
    )

    ps_http = env.pageserver.http_client()

    # first try to delete non existing tenant
    tenant_id = TenantId.generate()
    env.pageserver.allowed_errors.append(".*NotFound.*")
    env.pageserver.allowed_errors.append(".*simulated failure.*")

    # Check that deleting a non-existent tenant gives the expected result: this is a loop because we
    # may need to retry on some remote storage errors injected by the test harness
    while True:
        try:
            ps_http.tenant_delete(tenant_id=tenant_id)
        except PageserverApiException as e:
            if e.status_code == 500:
                # This test uses failure injection, which can produce 500s as the pageserver expects
                # the object store to always be available, and the ListObjects during deletion is generally
                # an infallible operation
                assert "simulated failure of remote operation" in e.message
            elif e.status_code == 404:
                # This is our expected result: trying to erase a non-existent tenant gives us 404
                assert "NotFound" in e.message
                break
            else:
                raise

    env.neon_cli.create_tenant(
        tenant_id=tenant_id,
        conf=MANY_SMALL_LAYERS_TENANT_CONFIG,
    )

    # Default tenant and the one we created
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 2

    # create two timelines one being the parent of another
    parent = None
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_branch(
            timeline, tenant_id=tenant_id, ancestor_branch_name=parent
        )
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

            assert_prefix_not_empty(
                neon_env_builder.pageserver_remote_storage,
                prefix="/".join(
                    (
                        "tenants",
                        str(tenant_id),
                    )
                ),
            )

        parent = timeline

    # Upload a heatmap so that we exercise deletion of that too
    ps_http.tenant_heatmap_upload(tenant_id)

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 2
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1

    tenant_path = env.pageserver.tenant_dir(tenant_id)
    assert not tenant_path.exists()

    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    # Deletion updates the tenant count: the one default tenant remains
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1


class Check(enum.Enum):
    RETRY_WITHOUT_RESTART = enum.auto()
    RETRY_WITH_RESTART = enum.auto()


FAILPOINTS = [
    "tenant-delete-before-shutdown",
    "tenant-delete-before-create-remote-mark",
    "tenant-delete-before-create-local-mark",
    "tenant-delete-before-background",
    "tenant-delete-before-polling-ongoing-deletions",
    "tenant-delete-before-cleanup-remaining-fs-traces",
    "tenant-delete-before-remove-timelines-dir",
    "tenant-delete-before-remove-deleted-mark",
    "tenant-delete-before-remove-tenant-dir",
    # Some failpoints from timeline deletion
    "timeline-delete-before-index-deleted-at",
    "timeline-delete-before-rm",
    "timeline-delete-before-index-delete",
]

FAILPOINTS_BEFORE_BACKGROUND = [
    "timeline-delete-before-schedule",
    "tenant-delete-before-shutdown",
    "tenant-delete-before-create-remote-mark",
    "tenant-delete-before-create-local-mark",
    "tenant-delete-before-background",
]


def combinations():
    result = []

    remotes = available_s3_storages()

    for remote_storage_kind in remotes:
        for delete_failpoint in FAILPOINTS:
            # Simulate failures for only one type of remote storage
            # to avoid log pollution and make tests run faster
            if remote_storage_kind is RemoteStorageKind.MOCK_S3:
                simulate_failures = True
            else:
                simulate_failures = False
            result.append((remote_storage_kind, delete_failpoint, simulate_failures))
    return result


@pytest.mark.parametrize("check", list(Check))
@pytest.mark.parametrize("remote_storage_kind, failpoint, simulate_failures", combinations())
def test_delete_tenant_exercise_crash_safety_failpoints(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    failpoint: str,
    simulate_failures: bool,
    check: Check,
    pg_bin: PgBin,
):
    if simulate_failures:
        neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)

    tenant_id = env.initial_tenant

    env.pageserver.allowed_errors.extend(
        [
            # From deletion polling
            f".*NotFound: tenant {env.initial_tenant}.*",
            # allow errors caused by failpoints
            f".*failpoint: {failpoint}",
            # It appears when we stopped flush loop during deletion (attempt) and then pageserver is stopped
            ".*shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
            # We may leave some upload tasks in the queue. They're likely deletes.
            # For uploads we explicitly wait with `last_flush_lsn_upload` below.
            # So by ignoring these instead of waiting for empty upload queue
            # we execute more distinct code paths.
            '.*stopping left-over name="remote upload".*',
            # an on-demand is cancelled by shutdown
            ".*initial size calculation failed: downloading failed, possibly for shutdown",
        ]
    )

    if simulate_failures:
        env.pageserver.allowed_errors.append(
            # The deletion queue will complain when it encounters simulated S3 errors
            ".*deletion executor: DeleteObjects request failed.*",
        )

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete", tenant_id=tenant_id)
    with env.endpoints.create_start("delete", tenant_id=tenant_id) as endpoint:
        # generate enough layers
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

        assert_prefix_not_empty(
            neon_env_builder.pageserver_remote_storage,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                )
            ),
        )

    ps_http.configure_failpoints((failpoint, "return"))

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    # These failpoints are earlier than background task is spawned.
    # so they result in api request failure.
    if failpoint in FAILPOINTS_BEFORE_BACKGROUND:
        with pytest.raises(PageserverApiException, match=failpoint):
            ps_http.tenant_delete(tenant_id)

    else:
        ps_http.tenant_delete(tenant_id)
        tenant_info = wait_until_tenant_state(
            pageserver_http=ps_http,
            tenant_id=tenant_id,
            expected_state="Broken",
            iterations=iterations,
        )

        reason = tenant_info["state"]["data"]["reason"]
        log.info(f"tenant broken: {reason}")

        # failpoint may not be the only error in the stack
        assert reason.endswith(f"failpoint: {failpoint}"), reason

    if check is Check.RETRY_WITH_RESTART:
        env.pageserver.restart()

        if failpoint in (
            "tenant-delete-before-shutdown",
            "tenant-delete-before-create-remote-mark",
        ):
            wait_until_tenant_active(
                ps_http, tenant_id=tenant_id, iterations=iterations, period=0.25
            )
            tenant_delete_wait_completed(ps_http, tenant_id, iterations=iterations)
        else:
            # Pageserver should've resumed deletion after restart.
            wait_tenant_status_404(ps_http, tenant_id, iterations=iterations + 10)
    elif check is Check.RETRY_WITHOUT_RESTART:
        # this should succeed
        # this also checks that delete can be retried even when tenant is in Broken state
        ps_http.configure_failpoints((failpoint, "off"))

        tenant_delete_wait_completed(ps_http, tenant_id, iterations=iterations)

    tenant_dir = env.pageserver.tenant_dir(tenant_id)
    # Check local is empty
    assert not tenant_dir.exists()

    # Check remote is empty
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
        allowed_postfix="initdb.tar.zst",
    )


def test_tenant_delete_is_resumed_on_attach(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)
    env.pageserver.allowed_errors.append(
        # lucky race with stopping from flushing a layer we fail to schedule any uploads
        ".*layer flush task.+: could not flush frozen layer: update_metadata_file"
    )

    tenant_id = env.initial_tenant

    ps_http = env.pageserver.http_client()
    # create two timelines
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_timeline(timeline, tenant_id=tenant_id)
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

    # sanity check, data should be there
    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    # failpoint before we remove index_part from s3
    failpoint = "timeline-delete-before-index-delete"
    ps_http.configure_failpoints((failpoint, "return"))

    env.pageserver.allowed_errors.extend(
        (
            # allow errors caused by failpoints
            f".*failpoint: {failpoint}",
            # From deletion polling
            f".*NotFound: tenant {env.initial_tenant}.*",
            # It appears when we stopped flush loop during deletion (attempt) and then pageserver is stopped
            ".*shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
            # error from http response is also logged
            ".*InternalServerError\\(Tenant is marked as deleted on remote storage.*",
            '.*shutdown_pageserver{exit_code=0}: stopping left-over name="remote upload".*',
        )
    )

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    ps_http.tenant_delete(tenant_id)

    tenant_info = wait_until_tenant_state(
        pageserver_http=ps_http,
        tenant_id=tenant_id,
        expected_state="Broken",
        iterations=iterations,
    )

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    reason = tenant_info["state"]["data"]["reason"]
    # failpoint may not be the only error in the stack
    assert reason.endswith(f"failpoint: {failpoint}"), reason

    # now we stop pageserver and remove local tenant state
    env.endpoints.stop_all()
    env.pageserver.stop()

    dir_to_clear = env.pageserver.tenant_dir()
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    env.pageserver.start()

    # now we call attach
    env.pageserver.tenant_attach(tenant_id=tenant_id)

    # delete should be resumed
    wait_tenant_status_404(ps_http, tenant_id, iterations)

    # we shouldn've created tenant dir on disk
    tenant_path = env.pageserver.tenant_dir(tenant_id)
    assert not tenant_path.exists()

    ps_http.deletion_queue_flush(execute=True)
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )


def test_long_timeline_create_cancelled_by_tenant_delete(neon_env_builder: NeonEnvBuilder):
    """Reproduction of 2023-11-23 stuck tenants investigation"""

    # do not use default tenant/timeline creation because it would output the failpoint log message too early
    env = neon_env_builder.init_configs()
    env.start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(
        [
            # happens with the cancellation bailing flushing loop earlier, leaving disk_consistent_lsn at zero
            ".*Timeline got dropped without initializing, cleaning its files",
            # the response hit_pausable_failpoint_and_later_fail
            f".*Error processing HTTP request: InternalServerError\\(new timeline {env.initial_tenant}/{env.initial_timeline} has invalid disk_consistent_lsn",
        ]
    )

    env.pageserver.tenant_create(env.initial_tenant)

    failpoint = "flush-layer-cancel-after-writing-layer-out-pausable"
    pageserver_http.configure_failpoints((failpoint, "pause"))

    def hit_pausable_failpoint_and_later_fail():
        with pytest.raises(PageserverApiException, match="NotFound: tenant"):
            pageserver_http.timeline_create(
                env.pg_version, env.initial_tenant, env.initial_timeline
            )

    def start_deletion():
        pageserver_http.tenant_delete(env.initial_tenant)

    def has_hit_failpoint():
        assert env.pageserver.log_contains(f"at failpoint {failpoint}") is not None

    def deletion_has_started_waiting_for_timelines():
        assert env.pageserver.log_contains("Waiting for timelines...") is not None

    def tenant_is_deleted():
        try:
            pageserver_http.tenant_status(env.initial_tenant)
        except PageserverApiException as e:
            assert e.status_code == 404
        else:
            raise RuntimeError("tenant was still accessible")

    creation = Thread(target=hit_pausable_failpoint_and_later_fail)
    creation.start()

    deletion = None

    try:
        wait_until(10, 1, has_hit_failpoint)

        # it should start ok, sync up with the stuck creation, then hang waiting for the timeline
        # to shut down.
        deletion = Thread(target=start_deletion)
        deletion.start()

        wait_until(10, 1, deletion_has_started_waiting_for_timelines)

        pageserver_http.configure_failpoints((failpoint, "off"))

        creation.join()
        deletion.join()

        wait_until(10, 1, tenant_is_deleted)
    finally:
        creation.join()
        if deletion is not None:
            deletion.join()


def test_tenant_delete_concurrent(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    """
    Validate that concurrent delete requests to the same tenant behave correctly:
    exactly one should execute: the rest should give 202 responses but not start
    another deletion.

    This is a reproducer for https://github.com/neondatabase/neon/issues/5936
    """
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.MOCK_S3)
    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)
    ps_http = env.pageserver.http_client()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Populate some data
    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

    env.pageserver.allowed_errors.extend(
        [
            # lucky race with stopping from flushing a layer we fail to schedule any uploads
            ".*layer flush task.+: could not flush frozen layer: update_metadata_file",
        ]
    )

    BEFORE_REMOVE_FAILPOINT = "tenant-delete-before-map-remove"
    BEFORE_RUN_FAILPOINT = "tenant-delete-before-run"

    # We will let the initial delete run until right before it would remove
    # the tenant's TenantSlot.  This pauses it in a state where the tenant
    # is visible in Stopping state, and concurrent requests should fail with 4xx.
    ps_http.configure_failpoints((BEFORE_REMOVE_FAILPOINT, "pause"))

    def delete_tenant():
        return ps_http.tenant_delete(tenant_id)

    def hit_remove_failpoint():
        return env.pageserver.assert_log_contains(f"at failpoint {BEFORE_REMOVE_FAILPOINT}")[1]

    def hit_run_failpoint():
        env.pageserver.assert_log_contains(f"at failpoint {BEFORE_RUN_FAILPOINT}")

    with concurrent.futures.ThreadPoolExecutor() as executor:
        background_200_req = executor.submit(delete_tenant)
        assert background_200_req.result(timeout=10).status_code == 202

        # Wait until the first request completes its work and is blocked on removing
        # the TenantSlot from tenant manager.
        log_cursor = wait_until(100, 0.1, hit_remove_failpoint)
        assert log_cursor is not None

        # Start another request: this should succeed without actually entering the deletion code
        ps_http.tenant_delete(tenant_id)
        assert not env.pageserver.log_contains(
            f"at failpoint {BEFORE_RUN_FAILPOINT}", offset=log_cursor
        )

        # Start another background request, which will pause after acquiring a TenantSlotGuard
        # but before completing.
        ps_http.configure_failpoints((BEFORE_RUN_FAILPOINT, "pause"))
        background_4xx_req = executor.submit(delete_tenant)
        wait_until(100, 0.1, hit_run_failpoint)

        # The TenantSlot is still present while the original request is hung before
        # final removal
        assert (
            ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1
        )

        # Permit the original request to run to success
        ps_http.configure_failpoints((BEFORE_REMOVE_FAILPOINT, "off"))

        # Permit the duplicate background request to run to completion and fail.
        ps_http.configure_failpoints((BEFORE_RUN_FAILPOINT, "off"))
        background_4xx_req.result(timeout=10)
        assert not env.pageserver.log_contains(
            f"at failpoint {BEFORE_RUN_FAILPOINT}", offset=log_cursor
        )

    # Physical deletion should have happened
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    # Zero tenants remain (we deleted the default tenant)
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 0
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "inprogress"}) == 0


def test_tenant_delete_races_timeline_creation(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    """
    Validate that timeline creation executed in parallel with deletion works correctly.

    This is a reproducer for https://github.com/neondatabase/neon/issues/6255
    """
    # The remote storage kind doesn't really matter but we use it for iterations calculation below
    # (and there is no way to reconstruct the used remote storage kind)
    remote_storage_kind = RemoteStorageKind.MOCK_S3
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)
    ps_http = env.pageserver.http_client()
    tenant_id = env.initial_tenant

    # When timeline creation is cancelled by tenant deletion, it is during Tenant::shutdown(), and
    # acting on a shutdown tenant generates a 503 response (if caller retried they would later) get
    # a 404 after the tenant is fully deleted.
    CANCELLED_ERROR = (
        ".*POST.*Cancelled request finished successfully status=503 Service Unavailable"
    )

    # This can occur sometimes.
    CONFLICT_MESSAGE = ".*Precondition failed: Invalid state Stopping. Expected Active or Broken.*"

    env.pageserver.allowed_errors.extend(
        [
            # lucky race with stopping from flushing a layer we fail to schedule any uploads
            ".*layer flush task.+: could not flush frozen layer: update_metadata_file",
            # We need the http connection close for successful reproduction
            ".*POST.*/timeline.* request was dropped before completing",
            # Timeline creation runs into this error
            CANCELLED_ERROR,
            # Timeline deletion can run into this error during deletion
            CONFLICT_MESSAGE,
            ".*tenant_delete_handler.*still waiting, taking longer than expected.*",
        ]
    )

    BEFORE_INITDB_UPLOAD_FAILPOINT = "before-initdb-upload"
    DELETE_BEFORE_CLEANUP_FAILPOINT = "tenant-delete-before-cleanup-remaining-fs-traces-pausable"

    # Wait just before the initdb upload
    ps_http.configure_failpoints((BEFORE_INITDB_UPLOAD_FAILPOINT, "pause"))

    def timeline_create():
        try:
            ps_http.timeline_create(env.pg_version, tenant_id, TimelineId.generate(), timeout=1)
            raise RuntimeError("creation succeeded even though it shouldn't")
        except ReadTimeout:
            pass

    Thread(target=timeline_create).start()

    def hit_initdb_upload_failpoint():
        env.pageserver.assert_log_contains(f"at failpoint {BEFORE_INITDB_UPLOAD_FAILPOINT}")

    wait_until(100, 0.1, hit_initdb_upload_failpoint)

    def creation_connection_timed_out():
        env.pageserver.assert_log_contains(
            "POST.*/timeline.* request was dropped before completing"
        )

    # Wait so that we hit the timeout and the connection is dropped
    # (But timeline creation still continues)
    wait_until(100, 0.1, creation_connection_timed_out)

    ps_http.configure_failpoints((DELETE_BEFORE_CLEANUP_FAILPOINT, "pause"))

    def tenant_delete():
        def tenant_delete_inner():
            ps_http.tenant_delete(tenant_id)

        wait_until(100, 0.5, tenant_delete_inner)

    Thread(target=tenant_delete).start()

    def deletion_arrived():
        env.pageserver.assert_log_contains(
            f"cfg failpoint: {DELETE_BEFORE_CLEANUP_FAILPOINT} pause"
        )

    wait_until(100, 0.1, deletion_arrived)

    ps_http.configure_failpoints((DELETE_BEFORE_CLEANUP_FAILPOINT, "off"))

    # Disable the failpoint and wait for deletion to finish
    ps_http.configure_failpoints((BEFORE_INITDB_UPLOAD_FAILPOINT, "off"))

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    tenant_delete_wait_completed(ps_http, tenant_id, iterations, ignore_errors=True)

    # Physical deletion should have happened
    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(
            (
                "tenants",
                str(tenant_id),
            )
        ),
    )

    # Ensure that creation cancelled and deletion didn't end up in broken state or encountered the leftover temp file
    env.pageserver.assert_log_contains(CANCELLED_ERROR)
    assert not env.pageserver.log_contains(
        ".*ERROR.*delete_tenant.*Timelines directory is not empty after all timelines deletion"
    )

    # Zero tenants remain (we deleted the default tenant)
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 0


def test_tenant_delete_scrubber(pg_bin: PgBin, neon_env_builder: NeonEnvBuilder):
    """
    Validate that creating and then deleting the tenant both survives the scrubber,
    and that one can run the scrubber without problems.
    """

    remote_storage_kind = RemoteStorageKind.MOCK_S3
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    scrubber = StorageScrubber(neon_env_builder)
    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)

    ps_http = env.pageserver.http_client()
    # create a tenant separate from the main tenant so that we have one remaining
    # after we deleted it, as the scrubber treats empty buckets as an error.
    (tenant_id, timeline_id) = env.neon_cli.create_tenant()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    ps_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(ps_http, tenant_id, timeline_id, last_flush_lsn)
    env.stop()

    result = scrubber.scan_metadata()
    assert result["with_warnings"] == []

    env.start()
    ps_http = env.pageserver.http_client()
    iterations = poll_for_remote_storage_iterations(remote_storage_kind)
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)
    env.stop()

    scrubber.scan_metadata()
    assert result["with_warnings"] == []
