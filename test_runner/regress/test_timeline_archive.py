from __future__ import annotations

import pytest
from fixtures.common_types import TenantId, TimelineArchivalState, TimelineId
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.utils import wait_until


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
            env.initial_tenant,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # for a non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match="NotFound: [tT]enant",
    ) as exc:
        ps_http.timeline_archival_config(
            invalid_tenant_id,
            invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # construct a pair of branches to validate that pageserver prohibits
    # archival of ancestor timelines when they have non-archived child branches
    parent_timeline_id = env.create_branch("test_ancestor_branch_archive_parent")

    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1",
        ancestor_branch_name="test_ancestor_branch_archive_parent",
    )

    with pytest.raises(
        PageserverApiException,
        match="Cannot archive timeline which has non-archived child timelines",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            parent_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 412

    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        timeline_id=leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is False

    # Test that archiving the leaf timeline and then the parent works
    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    # Test that the leaf can't be unarchived
    with pytest.raises(
        PageserverApiException,
        match="ancestor is archived",
    ) as exc:
        ps_http.timeline_archival_config(
            env.initial_tenant,
            leaf_timeline_id,
            state=TimelineArchivalState.UNARCHIVED,
        )

    # Unarchive works for the leaf if the parent gets unarchived first
    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )

    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )


def test_timeline_offloading(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Create two branches and archive them
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_archive_parent")
    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_archive_branch1", "test_ancestor_branch_archive_parent"
    )

    ps_http.timeline_archival_config(
        env.initial_tenant,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        env.initial_tenant,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        env.initial_tenant,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    def parent_offloaded():
        ps_http.timeline_offload(tenant_id=env.initial_tenant, timeline_id=parent_timeline_id)
        assert env.pageserver.log_contains(
            f".*{parent_timeline_id}.*offloading archived timeline.*"
        )

    def leaf_offloaded():
        ps_http.timeline_offload(tenant_id=env.initial_tenant, timeline_id=leaf_timeline_id)
        assert env.pageserver.log_contains(f".*{leaf_timeline_id}.*offloading archived timeline.*")

    wait_until(30, 1, leaf_offloaded)
    wait_until(30, 1, parent_offloaded)
