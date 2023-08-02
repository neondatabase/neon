import enum
import os
import queue
import shutil
import threading
from pathlib import Path
from typing import Optional

import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    timeline_delete_wait_completed,
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_timeline_detail_404,
    wait_until_tenant_active,
    wait_until_timeline_state,
)
from fixtures.remote_storage import (
    RemoteStorageKind,
    S3Storage,
    available_remote_storages,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar, wait_until


def test_timeline_delete(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.pageserver.allowed_errors.append(".*Timeline .* was not found.*")
    env.pageserver.allowed_errors.append(".*timeline not found.*")
    env.pageserver.allowed_errors.append(".*Cannot delete timeline which has child timelines.*")
    env.pageserver.allowed_errors.append(".*Precondition failed: Requested tenant is missing.*")

    ps_http = env.pageserver.http_client()

    # first try to delete non existing timeline
    # for existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found"):
        ps_http.timeline_delete(tenant_id=env.initial_tenant, timeline_id=invalid_timeline_id)

    # for non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match="Precondition failed: Requested tenant is missing",
    ) as exc:
        ps_http.timeline_delete(tenant_id=invalid_tenant_id, timeline_id=invalid_timeline_id)

    assert exc.value.status_code == 412

    # construct pair of branches to validate that pageserver prohibits
    # deletion of ancestor timelines when they have child branches
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_delete_parent", "empty")

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_delete_branch1", "test_ancestor_branch_delete_parent"
    )

    timeline_path = (
        env.repo_dir / "tenants" / str(env.initial_tenant) / "timelines" / str(parent_timeline_id)
    )

    with pytest.raises(
        PageserverApiException, match="Cannot delete timeline which has child timelines"
    ) as exc:
        assert timeline_path.exists()

        ps_http.timeline_delete(env.initial_tenant, parent_timeline_id)

    assert exc.value.status_code == 412

    timeline_path = (
        env.repo_dir / "tenants" / str(env.initial_tenant) / "timelines" / str(leaf_timeline_id)
    )
    assert timeline_path.exists()

    # retry deletes when compaction or gc is running in pageserver
    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: timeline_delete_wait_completed(ps_http, env.initial_tenant, leaf_timeline_id),
    )

    assert not timeline_path.exists()

    # check 404
    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)
    assert exc.value.status_code == 404

    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: timeline_delete_wait_completed(
            ps_http, env.initial_tenant, parent_timeline_id
        ),
    )

    # Check that we didn't pick up the timeline again after restart.
    # See https://github.com/neondatabase/neon/issues/3560
    env.pageserver.stop(immediate=True)
    env.pageserver.start()

    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)


class Check(enum.Enum):
    RETRY_WITHOUT_RESTART = enum.auto()
    RETRY_WITH_RESTART = enum.auto()


DELETE_FAILPOINTS = [
    "timeline-delete-before-index-deleted-at",
    "timeline-delete-before-schedule",
    "timeline-delete-before-rm",
    "timeline-delete-during-rm",
    "timeline-delete-after-rm",
    "timeline-delete-before-index-delete",
    "timeline-delete-after-index-delete",
    "timeline-delete-after-rm-metadata",
    "timeline-delete-after-rm-dir",
]


def combinations():
    result = []

    remotes = [RemoteStorageKind.NOOP, RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE"):
        remotes.append(RemoteStorageKind.REAL_S3)

    for remote_storage_kind in remotes:
        for delete_failpoint in DELETE_FAILPOINTS:
            if remote_storage_kind == RemoteStorageKind.NOOP and delete_failpoint in (
                "timeline-delete-before-index-delete",
                "timeline-delete-after-index-delete",
            ):
                # the above failpoints are not relevant for config without remote storage
                continue

            result.append((remote_storage_kind, delete_failpoint))
    return result


# cover the two cases: remote storage configured vs not configured
@pytest.mark.parametrize("remote_storage_kind, failpoint", combinations())
@pytest.mark.parametrize("check", list(Check))
def test_delete_timeline_exercise_crash_safety_failpoints(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    failpoint: str,
    check: Check,
    pg_bin: PgBin,
):
    """
    If there is a failure during deletion in one of the associated failpoints (or crash restart happens at this point) the delete operation
    should be retryable and should be successfully resumed.

    We iterate over failpoints list, changing failpoint to the next one.

    1. Set settings to generate many layers
    2. Create branch.
    3. Insert something
    4. Go with the test.
    5. Iterate over failpoints
    6. Execute delete for each failpoint
    7. Ensure failpoint is hit
    8. Retry or restart without the failpoint and check the result.
    """

    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_delete_timeline_exercise_crash_safety_failpoints"
        )

    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": f"{1024 ** 2}",
            "image_creation_threshold": "100",
        }
    )

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete")
    with env.endpoints.create_start("delete") as endpoint:
        # generate enough layers
        pg_bin.run(["pgbench", "-i", "-I dtGvp", "-s1", endpoint.connstr()])
        if remote_storage_kind is RemoteStorageKind.NOOP:
            wait_for_last_flush_lsn(env, endpoint, env.initial_tenant, timeline_id)
        else:
            last_flush_lsn_upload(env, endpoint, env.initial_tenant, timeline_id)

    env.pageserver.allowed_errors.append(f".*{timeline_id}.*failpoint: {failpoint}")
    # It appears when we stopped flush loop during deletion and then pageserver is stopped
    env.pageserver.allowed_errors.append(
        ".*freeze_and_flush_on_shutdown.*failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited"
    )
    # This happens when we fail before scheduling background operation.
    # Timeline is left in stopping state and retry tries to stop it again.
    env.pageserver.allowed_errors.append(
        ".*Ignoring new state, equal to the existing one: Stopping"
    )
    # This happens when we retry delete requests for broken timelines
    env.pageserver.allowed_errors.append(".*Ignoring state update Stopping for broken timeline")
    # This happens when timeline remains are cleaned up during loading
    env.pageserver.allowed_errors.append(".*Timeline dir entry become invalid.*")
    # In one of the branches we poll for tenant to become active. Polls can generate this log message:
    env.pageserver.allowed_errors.append(f".*Tenant {env.initial_tenant} is not active*")

    ps_http.configure_failpoints((failpoint, "return"))

    iterations = 20 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 4

    # These failpoints are earlier than background task is spawned.
    # so they result in api request failure.
    if failpoint in (
        "timeline-delete-before-index-deleted-at",
        "timeline-delete-before-schedule",
    ):
        with pytest.raises(PageserverApiException, match=failpoint):
            ps_http.timeline_delete(env.initial_tenant, timeline_id)

    else:
        ps_http.timeline_delete(env.initial_tenant, timeline_id)
        timeline_info = wait_until_timeline_state(
            pageserver_http=ps_http,
            tenant_id=env.initial_tenant,
            timeline_id=timeline_id,
            expected_state="Broken",
            iterations=iterations,
        )

        reason = timeline_info["state"]["Broken"]["reason"]
        log.info(f"timeline broken: {reason}")

        # failpoint may not be the only error in the stack
        assert reason.endswith(f"failpoint: {failpoint}"), reason

    if check is Check.RETRY_WITH_RESTART:
        env.pageserver.stop()
        env.pageserver.start()

        wait_until_tenant_active(ps_http, env.initial_tenant, iterations=iterations)

        if failpoint == "timeline-delete-before-index-deleted-at":
            # We crashed before persisting this to remote storage, need to retry delete request
            timeline_delete_wait_completed(ps_http, env.initial_tenant, timeline_id)
        else:
            # Pageserver should've resumed deletion after restart.
            wait_timeline_detail_404(
                ps_http, env.initial_tenant, timeline_id, iterations=iterations
            )
    elif check is Check.RETRY_WITHOUT_RESTART:
        # this should succeed
        # this also checks that delete can be retried even when timeline is in Broken state
        ps_http.configure_failpoints((failpoint, "off"))

        timeline_delete_wait_completed(
            ps_http, env.initial_tenant, timeline_id, iterations=iterations
        )

    # Check remote is impty
    if remote_storage_kind is RemoteStorageKind.MOCK_S3:
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    timeline_dir = env.timeline_dir(env.initial_tenant, timeline_id)
    # Check local is empty
    assert not timeline_dir.exists()
    # Check no delete mark present
    assert not (timeline_dir.parent / f"{timeline_id}.___deleted").exists()


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
@pytest.mark.parametrize("fill_branch", [True, False])
def test_timeline_resurrection_on_attach(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    fill_branch: bool,
):
    """
    After deleting a timeline it should never appear again.
    This test ensures that this invariant holds for detach+attach.
    Original issue: https://github.com/neondatabase/neon/issues/3560
    """

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_timeline_resurrection_on_attach",
    )

    ##### First start, insert data and upload it to the remote storage
    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()
    pg = env.endpoints.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    main_timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    with pg.cursor() as cur:
        cur.execute("CREATE TABLE f (i integer);")
        cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

        # wait until pageserver receives that data
        wait_for_last_record_lsn(ps_http, tenant_id, main_timeline_id, current_lsn)

        # run checkpoint manually to be sure that data landed in remote storage
        ps_http.timeline_checkpoint(tenant_id, main_timeline_id)

        # wait until pageserver successfully uploaded a checkpoint to remote storage
        log.info("waiting for checkpoint upload")
        wait_for_upload(ps_http, tenant_id, main_timeline_id, current_lsn)
        log.info("upload of checkpoint is done")

    branch_timeline_id = env.neon_cli.create_branch("new", "main")

    # Two variants of this test:
    # - In fill_branch=True, the deleted branch has layer files.
    # - In fill_branch=False, it doesn't, it just has the metadata file.
    # A broken implementation is conceivable that tries to "optimize" handling of empty branches, e.g.,
    # by skipping IndexPart uploads if the layer file set doesn't change. That would be wrong, catch those.
    if fill_branch:
        with env.endpoints.create_start("new") as new_pg:
            with new_pg.cursor() as cur:
                cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
                current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

                # wait until pageserver receives that data
                wait_for_last_record_lsn(ps_http, tenant_id, branch_timeline_id, current_lsn)

                # run checkpoint manually to be sure that data landed in remote storage
                ps_http.timeline_checkpoint(tenant_id, branch_timeline_id)

                # wait until pageserver successfully uploaded a checkpoint to remote storage
                log.info("waiting for checkpoint upload")
                wait_for_upload(ps_http, tenant_id, branch_timeline_id, current_lsn)
                log.info("upload of checkpoint is done")
    else:
        pass

    # delete new timeline
    timeline_delete_wait_completed(ps_http, tenant_id=tenant_id, timeline_id=branch_timeline_id)

    ##### Stop the pageserver instance, erase all its data
    env.endpoints.stop_all()
    env.pageserver.stop()

    dir_to_clear = Path(env.repo_dir) / "tenants"
    shutil.rmtree(dir_to_clear)
    os.mkdir(dir_to_clear)

    ##### Second start, restore the data and ensure that we see only timeline that wasnt deleted
    env.pageserver.start()

    ps_http.tenant_attach(tenant_id=tenant_id)

    wait_until_tenant_active(ps_http, tenant_id=tenant_id, iterations=10, period=0.5)

    timelines = ps_http.timeline_list(tenant_id=tenant_id)
    assert {TimelineId(tl["timeline_id"]) for tl in timelines} == {
        main_timeline_id
    }, "the deleted timeline should not have been resurrected"
    assert all([tl["state"] == "Active" for tl in timelines])


def assert_prefix_empty(neon_env_builder: NeonEnvBuilder, prefix: Optional[str] = None):
    # For local_fs we need to properly handle empty directories, which we currently dont, so for simplicity stick to s3 api.
    assert neon_env_builder.remote_storage_kind in (
        RemoteStorageKind.MOCK_S3,
        RemoteStorageKind.REAL_S3,
    )
    # For mypy
    assert isinstance(neon_env_builder.remote_storage, S3Storage)

    # Note that this doesnt use pagination, so list is not guaranteed to be exhaustive.
    assert neon_env_builder.remote_storage_client is not None
    response = neon_env_builder.remote_storage_client.list_objects_v2(
        Bucket=neon_env_builder.remote_storage.bucket_name,
        Prefix=prefix or neon_env_builder.remote_storage.prefix_in_bucket or "",
    )
    objects = response.get("Contents")
    assert (
        response["KeyCount"] == 0
    ), f"remote dir with prefix {prefix} is not empty after deletion: {objects}"


def test_timeline_delete_fail_before_local_delete(neon_env_builder: NeonEnvBuilder):
    """
    When deleting a timeline, if we succeed in setting the deleted flag remotely
    but fail to delete the local state, restarting the pageserver should resume
    the deletion of the local state.
    """

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_timeline_delete_fail_before_local_delete",
    )

    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(".*failpoint: timeline-delete-before-rm")
    env.pageserver.allowed_errors.append(
        ".*Ignoring new state, equal to the existing one: Stopping"
    )
    # this happens, because the stuck timeline is visible to shutdown
    env.pageserver.allowed_errors.append(
        ".*freeze_and_flush_on_shutdown.+: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited"
    )

    ps_http = env.pageserver.http_client()
    ps_http.configure_failpoints(("timeline-delete-before-rm", "return"))

    # construct pair of branches
    intermediate_timeline_id = env.neon_cli.create_branch(
        "test_timeline_delete_fail_before_local_delete"
    )

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_timeline_delete_fail_before_local_delete1",
        "test_timeline_delete_fail_before_local_delete",
    )

    leaf_timeline_path = (
        env.repo_dir / "tenants" / str(env.initial_tenant) / "timelines" / str(leaf_timeline_id)
    )

    ps_http.timeline_delete(env.initial_tenant, leaf_timeline_id)
    timeline_info = wait_until_timeline_state(
        pageserver_http=ps_http,
        tenant_id=env.initial_tenant,
        timeline_id=leaf_timeline_id,
        expected_state="Broken",
        iterations=2,  # effectively try immediately and retry once in one second
    )

    assert timeline_info["state"]["Broken"]["reason"] == "failpoint: timeline-delete-before-rm"

    assert leaf_timeline_path.exists(), "the failpoint didn't work"

    env.pageserver.stop()
    env.pageserver.start()

    # Wait for tenant to finish loading.
    wait_until_tenant_active(ps_http, tenant_id=env.initial_tenant, iterations=10, period=1)

    try:
        data = ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)
        log.debug(f"detail {data}")
    except PageserverApiException as e:
        log.debug(e)
        if e.status_code != 404:
            raise
    else:
        raise Exception("detail succeeded (it should return 404)")

    assert (
        not leaf_timeline_path.exists()
    ), "timeline load procedure should have resumed the deletion interrupted by the failpoint"
    timelines = ps_http.timeline_list(env.initial_tenant)
    assert {TimelineId(tl["timeline_id"]) for tl in timelines} == {
        intermediate_timeline_id,
        env.initial_timeline,
    }, "other timelines should not have been affected"
    assert all([tl["state"] == "Active" for tl in timelines])

    assert_prefix_empty(
        neon_env_builder,
        prefix="/".join(
            (
                "tenants",
                str(env.initial_tenant),
                "timelines",
                str(leaf_timeline_id),
            )
        ),
    )

    assert env.initial_timeline is not None

    for timeline_id in (intermediate_timeline_id, env.initial_timeline):
        timeline_delete_wait_completed(
            ps_http, tenant_id=env.initial_tenant, timeline_id=timeline_id
        )

        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    # for some reason the check above doesnt immediately take effect for the below.
    # Assume it is mock server incosistency and check twice.
    wait_until(
        2,
        0.5,
        lambda: assert_prefix_empty(neon_env_builder),
    )


@pytest.mark.parametrize(
    "stuck_failpoint",
    ["persist_deleted_index_part", "in_progress_delete"],
)
def test_concurrent_timeline_delete_stuck_on(
    neon_env_builder: NeonEnvBuilder, stuck_failpoint: str
):
    """
    If delete is stuck console will eventually retry deletion.
    So we need to be sure that these requests wont interleave with each other.
    In this tests we check two places where we can spend a lot of time.
    This is a regression test because there was a bug when DeletionGuard wasnt propagated
    to the background task.

    Ensure that when retry comes if we're still stuck request will get an immediate error response,
    signalling to console that it should retry later.
    """

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name=f"concurrent_timeline_delete_stuck_on_{stuck_failpoint}",
    )

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client()

    # make the first call sleep practically forever
    ps_http.configure_failpoints((stuck_failpoint, "pause"))

    def first_call(result_queue):
        try:
            log.info("first call start")
            timeline_delete_wait_completed(
                ps_http, env.initial_tenant, child_timeline_id, timeout=20
            )
            log.info("first call success")
            result_queue.put("success")
        except Exception:
            log.exception("first call failed")
            result_queue.put("failure, see log for stack trace")

    first_call_result: queue.Queue[str] = queue.Queue()
    first_call_thread = threading.Thread(target=first_call, args=(first_call_result,))
    first_call_thread.start()

    try:

        def first_call_hit_failpoint():
            assert env.pageserver.log_contains(
                f".*{child_timeline_id}.*at failpoint {stuck_failpoint}"
            )

        wait_until(50, 0.1, first_call_hit_failpoint)

        # make the second call and assert behavior
        log.info("second call start")
        error_msg_re = "Timeline deletion is already in progress"
        with pytest.raises(PageserverApiException, match=error_msg_re) as second_call_err:
            ps_http.timeline_delete(env.initial_tenant, child_timeline_id)
        assert second_call_err.value.status_code == 409
        env.pageserver.allowed_errors.append(f".*{child_timeline_id}.*{error_msg_re}.*")
        # the second call will try to transition the timeline into Stopping state as well
        env.pageserver.allowed_errors.append(
            f".*{child_timeline_id}.*Ignoring new state, equal to the existing one: Stopping"
        )
        log.info("second call failed as expected")

        # ensure it is not 404 and stopping
        detail = ps_http.timeline_detail(env.initial_tenant, child_timeline_id)
        assert detail["state"] == "Stopping"

        # by now we know that the second call failed, let's ensure the first call will finish
        ps_http.configure_failpoints((stuck_failpoint, "off"))

        result = first_call_result.get()
        assert result == "success"

    finally:
        log.info("joining first call thread")
        # in any case, make sure the lifetime of the thread is bounded to this test
        first_call_thread.join()


def test_delete_timeline_client_hangup(neon_env_builder: NeonEnvBuilder):
    """
    If the client hangs up before we start the index part upload but after deletion is scheduled
    we mark it
    deleted in local memory, a subsequent delete_timeline call should be able to do

    another delete timeline operation.

    This tests cancel safety up to the given failpoint.
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_delete_timeline_client_hangup",
    )

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client()

    failpoint_name = "persist_deleted_index_part"
    ps_http.configure_failpoints((failpoint_name, "pause"))

    with pytest.raises(requests.exceptions.Timeout):
        ps_http.timeline_delete(env.initial_tenant, child_timeline_id, timeout=2)

    env.pageserver.allowed_errors.append(
        f".*{child_timeline_id}.*Timeline deletion is already in progress.*"
    )
    with pytest.raises(PageserverApiException, match="Timeline deletion is already in progress"):
        ps_http.timeline_delete(env.initial_tenant, child_timeline_id, timeout=2)

    # make sure the timeout was due to the failpoint
    at_failpoint_log_message = f".*{child_timeline_id}.*at failpoint {failpoint_name}.*"

    def hit_failpoint():
        assert env.pageserver.log_contains(at_failpoint_log_message)

    wait_until(50, 0.1, hit_failpoint)

    # we log this error if a client hangs up
    # might as well use it as another indicator that the test works
    hangup_log_message = f".*DELETE.*{child_timeline_id}.*request was dropped before completing"
    env.pageserver.allowed_errors.append(hangup_log_message)

    def got_hangup_log_message():
        assert env.pageserver.log_contains(hangup_log_message)

    wait_until(50, 0.1, got_hangup_log_message)

    # check that the timeline is still present
    ps_http.timeline_detail(env.initial_tenant, child_timeline_id)

    # ok, disable the failpoint to let the deletion finish
    ps_http.configure_failpoints((failpoint_name, "off"))

    def first_request_finished():
        message = f".*DELETE.*{child_timeline_id}.*Cancelled request finished"
        assert env.pageserver.log_contains(message)

    wait_until(50, 0.1, first_request_finished)

    # check that the timeline is gone
    wait_timeline_detail_404(ps_http, env.initial_tenant, child_timeline_id, iterations=2)


@pytest.mark.parametrize(
    "remote_storage_kind",
    list(
        filter(
            lambda s: s in (RemoteStorageKind.MOCK_S3, RemoteStorageKind.REAL_S3),
            available_remote_storages(),
        )
    ),
)
def test_timeline_delete_works_for_remote_smoke(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_timeline_delete_works_for_remote_smoke",
    )

    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()
    pg = env.endpoints.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    main_timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    assert tenant_id == env.initial_tenant
    assert main_timeline_id == env.initial_timeline

    assert env.initial_timeline is not None
    timeline_ids = [env.initial_timeline]
    for i in range(2):
        branch_timeline_id = env.neon_cli.create_branch(f"new{i}", "main")
        pg = env.endpoints.create_start(f"new{i}")

        with pg.cursor() as cur:
            cur.execute("CREATE TABLE f (i integer);")
            cur.execute("INSERT INTO f VALUES (generate_series(1,1000));")
            current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

            # wait until pageserver receives that data
            wait_for_last_record_lsn(ps_http, tenant_id, branch_timeline_id, current_lsn)

            # run checkpoint manually to be sure that data landed in remote storage
            ps_http.timeline_checkpoint(tenant_id, branch_timeline_id)

            # wait until pageserver successfully uploaded a checkpoint to remote storage
            log.info("waiting for checkpoint upload")
            wait_for_upload(ps_http, tenant_id, branch_timeline_id, current_lsn)
            log.info("upload of checkpoint is done")
            timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

        timeline_ids.append(timeline_id)

    for timeline_id in reversed(timeline_ids):
        # note that we need to finish previous deletion before scheduling next one
        # otherwise we can get an "HasChildren" error if deletion is not fast enough (real_s3)
        timeline_delete_wait_completed(ps_http, tenant_id=tenant_id, timeline_id=timeline_id)

        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(env.initial_tenant),
                    "timelines",
                    str(timeline_id),
                )
            ),
        )

    # for some reason the check above doesnt immediately take effect for the below.
    # Assume it is mock server inconsistency and check twice.
    wait_until(
        2,
        0.5,
        lambda: assert_prefix_empty(neon_env_builder),
    )
