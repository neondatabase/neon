import enum
import os
import shutil

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
    assert_prefix_not_empty,
    poll_for_remote_storage_iterations,
    tenant_delete_wait_completed,
    wait_tenant_status_404,
    wait_until_tenant_active,
    wait_until_tenant_state,
)
from fixtures.remote_storage import (
    RemoteStorageKind,
    available_remote_storages,
    available_s3_storages,
)
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
    neon_env_builder.pageserver_config_override = "test_remote_failures=1"

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()

    # lucky race with stopping from flushing a layer we fail to schedule any uploads
    env.pageserver.allowed_errors.append(
        ".*layer flush task.+: could not flush frozen layer: update_metadata_file"
    )

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

            if remote_storage_kind in available_s3_storages():
                assert_prefix_not_empty(
                    neon_env_builder,
                    prefix="/".join(
                        (
                            "tenants",
                            str(tenant_id),
                        )
                    ),
                )

        parent = timeline

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    tenant_delete_wait_completed(ps_http, tenant_id, iterations)

    tenant_path = env.pageserver.tenant_dir(tenant_id)
    assert not tenant_path.exists()

    if remote_storage_kind in available_s3_storages():
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
            if remote_storage_kind is RemoteStorageKind.NOOP and delete_failpoint in (
                "timeline-delete-before-index-delete",
            ):
                # the above failpoint are not relevant for config without remote storage
                continue

            # Simulate failures for only one type of remote storage
            # to avoid log pollution and make tests run faster
            if remote_storage_kind is RemoteStorageKind.MOCK_S3:
                simulate_failures = True
            else:
                simulate_failures = False
            result.append((remote_storage_kind, delete_failpoint, simulate_failures))
    return result


@pytest.mark.parametrize("remote_storage_kind, failpoint, simulate_failures", combinations())
@pytest.mark.parametrize("check", list(Check))
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
            ".*shutdown_all_tenants:shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
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

            if remote_storage_kind in available_s3_storages():
                assert_prefix_not_empty(
                    neon_env_builder,
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

    tenant_dir = env.pageserver.tenant_dir(tenant_id)
    # Check local is empty
    assert not tenant_dir.exists()

    # Check remote is empty
    if remote_storage_kind in available_s3_storages():
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                )
            ),
        )


@pytest.mark.parametrize("remote_storage_kind", available_remote_storages())
def test_tenant_delete_is_resumed_on_attach(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
    pg_bin: PgBin,
):
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)

    tenant_id = env.initial_tenant

    ps_http = env.pageserver.http_client()
    # create two timelines
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_timeline(timeline, tenant_id=tenant_id)
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

    # sanity check, data should be there
    if remote_storage_kind in available_s3_storages():
        assert_prefix_not_empty(
            neon_env_builder,
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
            ".*shutdown_all_tenants:shutdown.*tenant_id.*shutdown.*timeline_id.*: failed to freeze and flush: cannot flush frozen layers when flush_loop is not running, state is Exited",
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

    if remote_storage_kind in available_s3_storages():
        assert_prefix_not_empty(
            neon_env_builder,
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
    ps_http.tenant_attach(tenant_id=tenant_id)

    # delete should be resumed
    wait_tenant_status_404(ps_http, tenant_id, iterations)

    # we shouldn've created tenant dir on disk
    tenant_path = env.pageserver.tenant_dir(tenant_id)
    assert not tenant_path.exists()

    if remote_storage_kind in available_s3_storages():
        assert_prefix_empty(
            neon_env_builder,
            prefix="/".join(
                (
                    "tenants",
                    str(tenant_id),
                )
            ),
        )


# TODO test concurrent deletions with "hang" failpoint
