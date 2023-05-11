import os
import queue
import shutil
import threading
from pathlib import Path

import pytest
import requests
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    RemoteStorageKind,
    available_remote_storages,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    wait_for_last_record_lsn,
    wait_for_upload,
    wait_until_tenant_active,
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

    assert exc.value.status_code == 400

    timeline_path = (
        env.repo_dir / "tenants" / str(env.initial_tenant) / "timelines" / str(leaf_timeline_id)
    )
    assert timeline_path.exists()

    # retry deletes when compaction or gc is running in pageserver
    wait_until(
        number_of_iterations=3,
        interval=0.2,
        func=lambda: ps_http.timeline_delete(env.initial_tenant, leaf_timeline_id),
    )

    assert not timeline_path.exists()

    # check 404
    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)

        # FIXME leaves tenant without timelines, should we prevent deletion of root timeline?
        wait_until(
            number_of_iterations=3,
            interval=0.2,
            func=lambda: ps_http.timeline_delete(env.initial_tenant, parent_timeline_id),
        )

    assert exc.value.status_code == 404

    # Check that we didn't pick up the timeline again after restart.
    # See https://github.com/neondatabase/neon/issues/3560
    env.pageserver.stop(immediate=True)
    env.pageserver.start()

    with pytest.raises(
        PageserverApiException,
        match=f"Timeline {env.initial_tenant}/{leaf_timeline_id} was not found",
    ) as exc:
        ps_http.timeline_detail(env.initial_tenant, leaf_timeline_id)


# cover the two cases: remote storage configured vs not configured
@pytest.mark.parametrize("remote_storage_kind", [None, RemoteStorageKind.LOCAL_FS])
def test_delete_timeline_post_rm_failure(
    neon_env_builder: NeonEnvBuilder, remote_storage_kind: RemoteStorageKind
):
    """
    If there is a failure after removing the timeline directory, the delete operation
    should be retryable.
    """

    if remote_storage_kind is not None:
        neon_env_builder.enable_remote_storage(
            remote_storage_kind, "test_delete_timeline_post_rm_failure"
        )

    env = neon_env_builder.init_start()
    assert env.initial_timeline

    ps_http = env.pageserver.http_client()

    failpoint_name = "timeline-delete-after-rm"
    ps_http.configure_failpoints((failpoint_name, "return"))

    with pytest.raises(PageserverApiException, match=f"failpoint: {failpoint_name}"):
        ps_http.timeline_delete(env.initial_tenant, env.initial_timeline)

    at_failpoint_log_message = f".*{env.initial_timeline}.*at failpoint {failpoint_name}.*"
    env.pageserver.allowed_errors.append(at_failpoint_log_message)
    env.pageserver.allowed_errors.append(
        f".*DELETE.*{env.initial_timeline}.*InternalServerError.*{failpoint_name}"
    )

    # retry without failpoint, it should succeed
    ps_http.configure_failpoints((failpoint_name, "off"))

    # this should succeed
    ps_http.timeline_delete(env.initial_tenant, env.initial_timeline, timeout=2)
    # the second call will try to transition the timeline into Stopping state, but it's already in that state
    env.pageserver.allowed_errors.append(
        f".*{env.initial_timeline}.*Ignoring new state, equal to the existing one: Stopping"
    )
    env.pageserver.allowed_errors.append(
        f".*{env.initial_timeline}.*timeline directory not found, proceeding anyway.*"
    )


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
    ps_http.timeline_delete(tenant_id=tenant_id, timeline_id=branch_timeline_id)

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


def test_timeline_delete_fail_before_local_delete(neon_env_builder: NeonEnvBuilder):
    """
    When deleting a timeline, if we succeed in setting the deleted flag remotely
    but fail to delete the local state, restarting the pageserver should resume
    the deletion of the local state.
    (Deletion of the state in S3 is not implemented yet.)
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
    env.pageserver.allowed_errors.append(
        ".*during shutdown: cannot flush frozen layers when flush_loop is not running, state is Exited"
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

    with pytest.raises(
        PageserverApiException,
        match="failpoint: timeline-delete-before-rm",
    ):
        ps_http.timeline_delete(env.initial_tenant, leaf_timeline_id)

    assert leaf_timeline_path.exists(), "the failpoint didn't work"

    env.pageserver.stop()
    env.pageserver.start()

    # Wait for tenant to finish loading.
    wait_until_tenant_active(ps_http, tenant_id=env.initial_tenant, iterations=10, period=0.5)

    assert (
        not leaf_timeline_path.exists()
    ), "timeline load procedure should have resumed the deletion interrupted by the failpoint"
    timelines = ps_http.timeline_list(env.initial_tenant)
    assert {TimelineId(tl["timeline_id"]) for tl in timelines} == {
        intermediate_timeline_id,
        env.initial_timeline,
    }, "other timelines should not have been affected"
    assert all([tl["state"] == "Active" for tl in timelines])


def test_concurrent_timeline_delete_if_first_stuck_at_index_upload(
    neon_env_builder: NeonEnvBuilder,
):
    """
    If we're stuck uploading the index file with the is_delete flag,
    eventually console will hand up and retry.
    If we're still stuck at the retry time, ensure that the retry
    eventually completes with the same status.
    """

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_concurrent_timeline_delete_if_first_stuck_at_index_upload",
    )

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client()

    failpoint_name = "persist_index_part_with_deleted_flag_after_set_before_upload_pause"
    ps_http.configure_failpoints((failpoint_name, "pause"))

    def delete_timeline_call(name, result_queue, barrier):
        if barrier:
            barrier.wait()
        try:
            log.info(f"{name} call start")
            ps_http.timeline_delete(env.initial_tenant, child_timeline_id, timeout=10)
            log.info(f"{name} call success")
            result_queue.put("success")
        except Exception:
            log.exception(f"{name} call failed")
            result_queue.put("failure, see log for stack trace")

    delete_results: queue.Queue[str] = queue.Queue()
    first_call_thread = threading.Thread(
        target=delete_timeline_call,
        args=(
            "1st",
            delete_results,
            None,
        ),
    )
    first_call_thread.start()

    second_call_thread = None

    try:

        def first_call_hit_failpoint():
            assert env.pageserver.log_contains(
                f".*{child_timeline_id}.*at failpoint {failpoint_name}"
            )

        wait_until(50, 0.1, first_call_hit_failpoint)

        barrier = threading.Barrier(2)
        second_call_thread = threading.Thread(
            target=delete_timeline_call,
            args=(
                "2nd",
                delete_results,
                barrier,
            ),
        )
        second_call_thread.start()

        barrier.wait()

        # release the pause
        ps_http.configure_failpoints((failpoint_name, "off"))

        # both should had succeeded: the second call will coalesce with the already-ongoing first call
        result = delete_results.get()
        assert result == "success"
        result = delete_results.get()
        assert result == "success"

        # the second call will try to transition the timeline into Stopping state, but it's already in that state
        # (the transition to Stopping state is not part of the request coalescing, because Tenant and Timeline states are a mess already)
        env.pageserver.allowed_errors.append(
            f".*{child_timeline_id}.*Ignoring new state, equal to the existing one: Stopping"
        )

        def second_call_attempt():
            assert env.pageserver.log_contains(
                f".*{child_timeline_id}.*Ignoring new state, equal to the existing one: Stopping"
            )

        wait_until(50, 0.1, second_call_attempt)
    finally:
        log.info("joining 1st thread")
        # in any case, make sure the lifetime of the thread is bounded to this test
        first_call_thread.join()

        if second_call_thread:
            log.info("joining 2nd thread")
            second_call_thread.join()


def test_delete_timeline_client_hangup(neon_env_builder: NeonEnvBuilder):
    """
    Make sure the timeline_delete runs to completion even if first request is cancelled because of a timeout.
    """
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
        test_name="test_delete_timeline_client_hangup",
    )

    env = neon_env_builder.init_start()

    child_timeline_id = env.neon_cli.create_branch("child", "main")

    ps_http = env.pageserver.http_client()

    failpoint_name = "persist_index_part_with_deleted_flag_after_set_before_upload_pause"
    ps_http.configure_failpoints((failpoint_name, "pause"))

    with pytest.raises(requests.exceptions.Timeout):
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

    # after disabling the failpoint pause, the original attempt should complete eventually
    ps_http.configure_failpoints((failpoint_name, "off"))

    def timeline_goes_away():
        try:
            ps_http.timeline_detail(env.initial_tenant, child_timeline_id)
            assert False, "expected a 404"
        except PageserverApiException as e:
            if e.status_code != 404:
                raise e
            else:
                # 404 received, timeline delete is now complete
                pass

    wait_until(50, 0.5, timeline_goes_away)
