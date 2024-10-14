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


@pytest.mark.parametrize("manual_offload", [False, True])
def test_timeline_offloading(neon_env_builder: NeonEnvBuilder, manual_offload: bool):
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()

    # Turn off gc and compaction loops: we want to issue them manually for better reliability
    tenant_id, initial_timeline_id = env.create_tenant(
        conf={
            "gc_period": "0s",
            "compaction_period": "0s" if manual_offload else "1s",
        }
    )

    # Create two branches and archive them
    parent_timeline_id = env.create_branch("test_ancestor_branch_archive_parent", tenant_id)
    leaf_timeline_id = env.create_branch(
        "test_ancestor_branch_archive_branch1", tenant_id, "test_ancestor_branch_archive_parent"
    )

    ps_http.timeline_archival_config(
        tenant_id,
        leaf_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is True

    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.ARCHIVED,
    )

    def timeline_offloaded(timeline_id: TimelineId) -> bool:
        return (
            env.pageserver.log_contains(f".*{timeline_id}.* offloading archived timeline.*")
            is not None
        )

    if manual_offload:
        with pytest.raises(
            PageserverApiException,
            match="no way to offload timeline, can_offload=true, has_no_attached_children=false",
        ):
            # This only tests the (made for testing only) http handler,
            # but still demonstrates the constraints we have.
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)

    def parent_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=parent_timeline_id)
        assert timeline_offloaded(parent_timeline_id)

    def leaf_offloaded():
        if manual_offload:
            ps_http.timeline_offload(tenant_id=tenant_id, timeline_id=leaf_timeline_id)
        assert timeline_offloaded(leaf_timeline_id)

    wait_until(30, 1, leaf_offloaded)
    wait_until(30, 1, parent_offloaded)

    ps_http.timeline_archival_config(
        tenant_id,
        parent_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    ps_http.timeline_archival_config(
        tenant_id,
        leaf_timeline_id,
        state=TimelineArchivalState.UNARCHIVED,
    )
    leaf_detail = ps_http.timeline_detail(
        tenant_id,
        leaf_timeline_id,
    )
    assert leaf_detail["is_archived"] is False

    assert not timeline_offloaded(initial_timeline_id)
