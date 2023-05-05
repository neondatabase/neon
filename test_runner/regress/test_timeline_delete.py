import pytest
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    RemoteStorageKind,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.types import TenantId, TimelineId
from fixtures.utils import wait_until


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

    # Check that we didnt pick up the timeline again after restart.
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


# TODO Test that we correctly handle GC of files that are stuck in upload queue.
