import pytest
from fixtures.common_types import TenantId, TimelineArchivalState, TimelineId
from fixtures.neon_fixtures import (
    NeonEnv,
)
from fixtures.pageserver.http import PageserverApiException


def test_timeline_archive(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.pageserver.allowed_errors.extend(
        [
            ".*Timeline .* was not found.*",
            ".*timeline not found.*",
            ".*Cannot archive timeline which has unarchived child timelines.*",
            ".*Precondition failed: Requested tenant is missing.*",
        ]
    )

    ps_http = env.pageserver.http_client()

    # first try to archive non existing timeline
    # for existing tenant:
    invalid_timeline_id = TimelineId.generate()
    with pytest.raises(PageserverApiException, match="timeline not found") as exc:
        ps_http.timeline_archival_config(
            tenant_id=env.initial_tenant,
            timeline_id=invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # for non existing tenant:
    invalid_tenant_id = TenantId.generate()
    with pytest.raises(
        PageserverApiException,
        match=f"NotFound: tenant {invalid_tenant_id}",
    ) as exc:
        ps_http.timeline_archival_config(
            tenant_id=invalid_tenant_id,
            timeline_id=invalid_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 404

    # construct pair of branches to validate that pageserver prohibits
    # archival of ancestor timelines when they have non-archived child branches
    parent_timeline_id = env.neon_cli.create_branch("test_ancestor_branch_archive_parent", "empty")

    leaf_timeline_id = env.neon_cli.create_branch(
        "test_ancestor_branch_archive_branch1", "test_ancestor_branch_archive_parent"
    )

    timeline_path = env.pageserver.timeline_dir(env.initial_tenant, parent_timeline_id)

    with pytest.raises(
        PageserverApiException,
        match="Cannot archive timeline which has non-archived child timelines",
    ) as exc:
        assert timeline_path.exists()

        ps_http.timeline_archival_config(
            tenant_id=env.initial_tenant,
            timeline_id=parent_timeline_id,
            state=TimelineArchivalState.ARCHIVED,
        )

    assert exc.value.status_code == 412

    # Test timeline_detail
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
        match="Cannot unarchive timeline which has archived ancestor",
    ) as exc:
        assert timeline_path.exists()

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
