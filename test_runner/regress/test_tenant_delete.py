from __future__ import annotations

import json
from threading import Thread

import pytest
from fixtures.common_types import Lsn, TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    assert_prefix_empty,
    assert_prefix_not_empty,
    many_small_layers_tenant_config,
    wait_for_upload,
)
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.utils import run_pg_bench_small, wait_until
from fixtures.workload import Workload
from requests.exceptions import ReadTimeout
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def error_tolerant_delete(ps_http, tenant_id):
    """
    For tests that inject 500 errors, we must retry repeatedly when issuing deletions
    """
    while True:
        try:
            ps_http.tenant_delete(tenant_id=tenant_id)
        except PageserverApiException as e:
            if e.status_code == 500:
                # This test uses failure injection, which can produce 500s as the pageserver expects
                # the object store to always be available, and the ListObjects during deletion is generally
                # an infallible operation.  This can show up as a clear simulated error, or as a general
                # error during delete_objects()
                assert (
                    "simulated failure of remote operation" in e.message
                    or "failed to delete" in e.message
                )
            else:
                raise
        else:
            # Success, drop out
            break


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
    env.pageserver.allowed_errors.extend(
        [".*NotFound.*", ".*simulated failure.*", ".*failed to delete .+ objects.*"]
    )

    # Check that deleting a non-existent tenant gives the expected result: this is a loop because we
    # may need to retry on some remote storage errors injected by the test harness
    error_tolerant_delete(ps_http, tenant_id)

    env.create_tenant(
        tenant_id=tenant_id,
        conf=many_small_layers_tenant_config(),
    )

    # Default tenant and the one we created
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 2

    # create two timelines one being the parent of another
    parent = None
    for timeline in ["first", "second"]:
        timeline_id = env.create_branch(timeline, ancestor_branch_name=parent, tenant_id=tenant_id)
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

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 2
    error_tolerant_delete(ps_http, tenant_id)
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
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "inprogress"}) == 0

    env.pageserver.stop()


def test_long_timeline_create_cancelled_by_tenant_delete(neon_env_builder: NeonEnvBuilder):
    """Reproduction of 2023-11-23 stuck tenants investigation"""

    # do not use default tenant/timeline creation because it would output the failpoint log message too early
    env = neon_env_builder.init_configs()
    env.start()
    pageserver_http = env.pageserver.http_client()

    env.pageserver.allowed_errors.extend(
        [
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
        wait_until(has_hit_failpoint)

        # it should start ok, sync up with the stuck creation, then hang waiting for the timeline
        # to shut down.
        deletion = Thread(target=start_deletion)
        deletion.start()

        wait_until(deletion_has_started_waiting_for_timelines)

        pageserver_http.configure_failpoints((failpoint, "off"))

        creation.join()
        deletion.join()

        wait_until(tenant_is_deleted)
    finally:
        creation.join()
        if deletion is not None:
            deletion.join()

    env.pageserver.stop()


def test_tenant_delete_races_timeline_creation(neon_env_builder: NeonEnvBuilder):
    """
    Validate that timeline creation executed in parallel with deletion works correctly.

    This is a reproducer for https://github.com/neondatabase/neon/issues/6255
    """
    # The remote storage kind doesn't really matter but we use it for iterations calculation below
    # (and there is no way to reconstruct the used remote storage kind)
    remote_storage_kind = RemoteStorageKind.MOCK_S3
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start(initial_tenant_conf=many_small_layers_tenant_config())
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

    wait_until(hit_initdb_upload_failpoint)

    def creation_connection_timed_out():
        env.pageserver.assert_log_contains(
            "POST.*/timeline.* request was dropped before completing"
        )

    # Wait so that we hit the timeout and the connection is dropped
    # (But timeline creation still continues)
    wait_until(creation_connection_timed_out)

    ps_http.configure_failpoints((DELETE_BEFORE_CLEANUP_FAILPOINT, "pause"))

    def tenant_delete():
        def tenant_delete_inner():
            ps_http.tenant_delete(tenant_id)

        wait_until(tenant_delete_inner)

    Thread(target=tenant_delete).start()

    def deletion_arrived():
        env.pageserver.assert_log_contains(
            f"cfg failpoint: {DELETE_BEFORE_CLEANUP_FAILPOINT} pause"
        )

    wait_until(deletion_arrived)

    ps_http.configure_failpoints((DELETE_BEFORE_CLEANUP_FAILPOINT, "off"))

    # Disable the failpoint and wait for deletion to finish
    ps_http.configure_failpoints((BEFORE_INITDB_UPLOAD_FAILPOINT, "off"))

    ps_http.tenant_delete(tenant_id)

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

    # We deleted our only tenant, and the scrubber fails if it detects nothing
    neon_env_builder.disable_scrub_on_exit()

    env.pageserver.stop()


def test_tenant_delete_scrubber(pg_bin: PgBin, make_httpserver, neon_env_builder: NeonEnvBuilder):
    """
    Validate that creating and then deleting the tenant both survives the scrubber,
    and that one can run the scrubber without problems.
    """

    remote_storage_kind = RemoteStorageKind.MOCK_S3
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)
    env = neon_env_builder.init_start(initial_tenant_conf=many_small_layers_tenant_config())

    ps_http = env.pageserver.http_client()
    # create a tenant separate from the main tenant so that we have one remaining
    # after we deleted it, as the scrubber treats empty buckets as an error.
    (tenant_id, timeline_id) = env.create_tenant()

    with env.endpoints.create_start("main", tenant_id=tenant_id) as endpoint:
        run_pg_bench_small(pg_bin, endpoint.connstr())
        last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
    ps_http.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(ps_http, tenant_id, timeline_id, last_flush_lsn)
    env.stop()

    healthy, _ = env.storage_scrubber.scan_metadata()
    assert healthy

    timeline_lsns = {
        "tenant_id": f"{tenant_id}",
        "timeline_id": f"{timeline_id}",
        "timeline_start_lsn": f"{last_flush_lsn}",
        "backup_lsn": f"{last_flush_lsn}",
    }

    cloud_admin_url = f"http://{make_httpserver.host}:{make_httpserver.port}/"
    cloud_admin_token = ""

    def get_branches(request: Request):
        # Compare definition with `BranchData` struct
        dummy_data = {
            "id": "test-branch-id",
            "created_at": "",  # TODO
            "updated_at": "",  # TODO
            "name": "testbranchname",
            "project_id": "test-project-id",
            "timeline_id": f"{timeline_id}",
            "default": False,
            "deleted": False,
            "logical_size": 42000,
            "physical_size": 42000,
            "written_size": 42000,
        }
        # This test does all its own compute configuration (by passing explicit pageserver ID to Workload functions),
        # so we send controller notifications to /dev/null to prevent it fighting the test for control of the compute.
        log.info(f"got get_branches request: {request.json}")
        return Response(json.dumps(dummy_data), content_type="application/json", status=200)

    make_httpserver.expect_request("/branches", method="GET").respond_with_handler(get_branches)

    healthy, _ = env.storage_scrubber.scan_metadata_safekeeper(
        timeline_lsns=[timeline_lsns],
        cloud_admin_api_url=cloud_admin_url,
        cloud_admin_api_token=cloud_admin_token,
    )
    assert healthy

    env.start()
    ps_http = env.pageserver.http_client()
    ps_http.tenant_delete(tenant_id)
    env.stop()

    healthy, _ = env.storage_scrubber.scan_metadata()
    assert healthy

    healthy, _ = env.storage_scrubber.scan_metadata_safekeeper(
        timeline_lsns=[timeline_lsns],
        cloud_admin_api_url=cloud_admin_url,
        cloud_admin_api_token=cloud_admin_token,
    )
    assert healthy


def test_tenant_delete_stale_shards(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    """
    Deleting a tenant should also delete any stale (pre-split) shards from remote storage.
    """
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    # Create an unsharded tenant.
    tenant_id, timeline_id = env.create_tenant()

    # Write some data.
    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()
    workload.write_rows(256)
    workload.validate()

    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(("tenants", str(tenant_id))),
    )

    # Upload a heatmap as well.
    env.pageserver.http_client().tenant_heatmap_upload(tenant_id)

    # Split off a few shards, in two rounds.
    env.storage_controller.tenant_shard_split(tenant_id, shard_count=4)
    env.storage_controller.tenant_shard_split(tenant_id, shard_count=16)

    # Delete the tenant. This should also delete data for the unsharded and count=4 parents.
    env.storage_controller.pageserver_api().tenant_delete(tenant_id=tenant_id)

    assert_prefix_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(("tenants", str(tenant_id))),
        delimiter="",  # match partial prefixes, i.e. all shards
    )

    dirs = list(env.pageserver.tenant_dir(None).glob(f"{tenant_id}*"))
    assert dirs == [], f"found tenant directories: {dirs}"

    # The initial tenant created by the test harness should still be there.
    # Only the tenant we deleted should be removed.
    assert_prefix_not_empty(
        neon_env_builder.pageserver_remote_storage,
        prefix="/".join(("tenants", str(env.initial_tenant))),
    )
    dirs = list(env.pageserver.tenant_dir(None).glob(f"{env.initial_tenant}*"))
    assert dirs != [], "missing initial tenant directory"

    env.stop()
