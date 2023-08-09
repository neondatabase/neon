import enum
import os

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    assert_prefix_empty,
    tenant_delete_wait_completed,
    wait_tenant_status_404,
    wait_until_tenant_active,
    wait_until_tenant_state,
)
from fixtures.remote_storage import RemoteStorageKind, available_remote_storages
from fixtures.types import TenantId
from fixtures.utils import run_pg_bench_small


@pytest.mark.parametrize(
    "remote_storage_kind", [RemoteStorageKind.NOOP, *available_remote_storages()]
)
def test_tenant_delete_smoke(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_bin: PgBin,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_tenant_delete_smoke",
    )

    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()

    # first try to delete non existing tenant
    tenant_id = TenantId.generate()
    env.pageserver.allowed_errors.append(f".*NotFound: tenant {tenant_id}.*")
    with pytest.raises(PageserverApiException, match=f"NotFound: tenant {tenant_id}"):
        ps_http.tenant_delete(tenant_id=tenant_id)

    env.neon_cli.create_tenant(
        tenant_id=tenant_id,
        conf=MANY_SMALL_LAYERS_TENANT_CONFIG,
    )

    # create two timelines one being the parent of another
    parent = None
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_branch(
            timeline, tenant_id=tenant_id, ancestor_branch_name=parent
        )
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

        parent = timeline

    iterations = 20 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 4
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)

    tenant_path = env.tenant_dir(tenant_id=tenant_id)
    assert not tenant_path.exists()

    if remote_storage_kind in [RemoteStorageKind.MOCK_S3, RemoteStorageKind.REAL_S3]:
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                )
            ),
        )


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
    "timeline-delete-after-rm-dir",
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

    remotes = [RemoteStorageKind.NOOP, RemoteStorageKind.MOCK_S3]
    if os.getenv("ENABLE_REAL_S3_REMOTE_STORAGE"):
        remotes.append(RemoteStorageKind.REAL_S3)

    for remote_storage_kind in remotes:
        for delete_failpoint in FAILPOINTS:
            if remote_storage_kind == RemoteStorageKind.NOOP and delete_failpoint in (
                "timeline-delete-before-index-delete",
            ):
                # the above failpoint are not relevant for config without remote storage
                continue

            result.append((remote_storage_kind, delete_failpoint))
    return result


@pytest.mark.parametrize("remote_storage_kind, failpoint", combinations())
@pytest.mark.parametrize("check", list(Check))
def test_delete_tenant_exercise_crash_safety_failpoints(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    failpoint: str,
    check: Check,
    pg_bin: PgBin,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind, "test_delete_tenant_exercise_crash_safety_failpoints"
    )

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)

    tenant_id = env.initial_tenant

    env.pageserver.allowed_errors.extend(
        [
            # From deletion polling
            f".*NotFound: tenant {env.initial_tenant}.*",
            # allow errors caused by failpoints
            f".*failpoint: {failpoint}",
            # It appears when we stopped flush loop during deletion (attempt) and then pageserver is stopped
            ".*freeze_and_flush_on_shutdown.*failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
            # We may leave some upload tasks in the queue. They're likely deletes.
            # For uploads we explicitly wait with `last_flush_lsn_upload` below.
            # So by ignoring these instead of waiting for empty upload queue
            # we execute more distinct code paths.
            '.*stopping left-over name="remote upload".*',
        ]
    )

    ps_http = env.pageserver.http_client()

    timeline_id = env.neon_cli.create_timeline("delete", tenant_id=tenant_id)
    with env.endpoints.create_start("delete", tenant_id=tenant_id) as endpoint:
        # generate enough layers
        run_pg_bench_small(pg_bin, endpoint.connstr())
        if remote_storage_kind is RemoteStorageKind.NOOP:
            wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
        else:
            last_flush_lsn_upload(env, endpoint, tenant_id, timeline_id)

    ps_http.configure_failpoints((failpoint, "return"))

    iterations = 20 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 4

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
        env.pageserver.stop()
        env.pageserver.start()

        if (
            remote_storage_kind is RemoteStorageKind.NOOP
            and failpoint == "tenant-delete-before-create-local-mark"
        ):
            tenant_delete_wait_completed(ps_http, tenant_id, iterations=iterations)
        elif failpoint in (
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

    # Check remote is impty
    if remote_storage_kind is RemoteStorageKind.MOCK_S3:
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                )
            ),
        )

    tenant_dir = env.tenant_dir(tenant_id)
    # Check local is empty
    assert not tenant_dir.exists()


# TODO test concurrent deletions with "hang" failpoint
# TODO test tenant delete continues after attach
