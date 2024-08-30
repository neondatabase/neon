import pytest
from fixtures.common_types import TenantId, TimelineArchivalState, TimelineId
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverApiException


@pytest.mark.parametrize("shard_count", [0, 4])
def test_timeline_archive(neon_env_builder: NeonEnvBuilder, shard_count: int):
    unsharded = shard_count == 0
    if unsharded:
        env = neon_env_builder.init_start()
        # If we run the unsharded version, talk to the pageserver directly
        ps_http = env.pageserver.http_client()
    else:
        neon_env_builder.num_pageservers = shard_count
        env = neon_env_builder.init_start(initial_tenant_shard_count=shard_count)
        # If we run the unsharded version, talk to the storage controller
        ps_http = env.storage_controller.pageserver_api()

    # first try to archive a non existing timeline for an existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found") as exc:
        ps_http.timeline_archival_config(
            tenant_id=env.initial_tenant,
            timeline_id=invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # for a non existing tenant:
    invalid_tenant_id = TenantId.generate()
    if unsharded:
        not_found_pattern = f"NotFound: tenant {invalid_tenant_id}"
    else:
        not_found_pattern = "NotFound: Tenant not found"
    with pytest.raises(
        PageserverApiException,
        match=not_found_pattern,
    ) as exc:
        ps_http.timeline_archival_config(
            tenant_id=invalid_tenant_id,
            timeline_id=invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # construct a pair of branches to validate that pageserver prohibits
    # archival of ancestor timelines when they have non-archived child branches
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_archive_parent")

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_archive_branch1", "test_ancestor_branch_archive_parent"
    )

    with pytest.raises(
        PageserverApiException,
        match="Cannot archive timeline which has non-archived child timelines",
    ) as exc:
        ps_http.timeline_archival_config(
            tenant_id=env.initial_tenant,
            timeline_id=parent_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 412 if unsharded else 409

    if shard_count != 0:
        leaf_detail = ps_http.timeline_detail(
            tenant_id=env.initial_tenant,
            timeline_id=leaf_timeline_id,
        )
        assert leaf_detail["is_archived"] is False

    # Test that archiving the leaf timeline and then the parent works
    ps_http.timeline_archival_config(
        tenant_id=env.initial_tenant,
        timeline_id=leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    if shard_count != 0:
        leaf_detail = ps_http.timeline_detail(
            tenant_id=env.initial_tenant,
            timeline_id=leaf_timeline_id,
        )
        assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        tenant_id=env.initial_tenant,
        timeline_id=parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    # Test that the leaf can't be unarchived
    with pytest.raises(
        PageserverApiException,
        match="ancestor is archived",
    ) as exc:
        ps_http.timeline_archival_config(
            tenant_id=env.initial_tenant,
            timeline_id=leaf_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )

    # Unarchive works for the leaf if the parent gets unarchived first
    ps_http.timeline_archival_config(
        tenant_id=env.initial_tenant,
        timeline_id=parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )

    ps_http.timeline_archival_config(
        tenant_id=env.initial_tenant,
        timeline_id=leaf_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
