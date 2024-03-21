import time
from datetime import datetime, timezone

from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    last_flush_lsn_upload
)
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    assert_prefix_empty,
    enable_remote_storage_versioning,
    poll_for_remote_storage_iterations,
    tenant_delete_wait_completed,
    wait_for_upload,
)
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.types import Lsn
from fixtures.utils import run_pg_bench_small
from fixtures.log_helper import log


def test_tenant_s3_restore(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    # Mock S3 doesn't have versioning enabled by default, enable it
    # (also do it before there is any writes to the bucket)
    if remote_storage_kind == RemoteStorageKind.MOCK_S3:
        remote_storage = neon_env_builder.pageserver_remote_storage
        assert remote_storage, "remote storage not configured"
        enable_remote_storage_versioning(remote_storage)

    env = neon_env_builder.init_start(initial_tenant_conf=MANY_SMALL_LAYERS_TENANT_CONFIG)
    env.pageserver.allowed_errors.extend(
        [
            # The deletion queue will complain when it encounters simulated S3 errors
            ".*deletion executor: DeleteObjects request failed.*",
            # lucky race with stopping from flushing a layer we fail to schedule any uploads
            ".*layer flush task.+: could not flush frozen layer: update_metadata_file",
        ]
    )

    ps_http = env.pageserver.http_client()

    tenant_id = env.initial_tenant

    # Default tenant and the one we created
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 1

    # create two timelines one being the parent of another, both with non-trivial data
    parent = None
    last_flush_lsns = {}

    timeline_ids = {}
    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as endpoint:
        endpoint.safe_psql(f"CREATE TABLE created_main(id integer);")
        flushed = last_flush_lsn_upload(env, endpoint, env.initial_tenant, env.initial_timeline)
        log.info(f"Timeline main/{env.initial_timeline} last_flush_lsn: {flushed}");
        last_flush_lsns["main"] = flushed
        timeline_ids["main"] = env.initial_timeline

    for branch in ["first", "second"]:
        timeline_id = env.neon_cli.create_branch(
            branch, tenant_id=tenant_id, ancestor_branch_name=parent
        )
        timeline_ids[branch] = timeline_id
        with env.endpoints.create_start(branch, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            endpoint.safe_psql(f"CREATE TABLE created_{branch}(id integer);")
            last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
            last_flush_lsns[branch] = last_flush_lsn

        log.info(f"Timeline {branch}/{timeline_id} last_flush_lsn: {last_flush_lsn}");
        ps_http.timeline_checkpoint(tenant_id, timeline_id)
        wait_for_upload(ps_http, tenant_id, timeline_id, last_flush_lsn)
        parent = branch

    # These sleeps are important because they fend off differences in clocks between us and S3
    time.sleep(4)
    ts_before_deletion = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    assert (
        ps_http.get_metric_value("pageserver_tenant_manager_slots") == 1
    ), "tenant removed before we deletion was issued"
    iterations = poll_for_remote_storage_iterations(remote_storage_kind)
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)
    ps_http.deletion_queue_flush(execute=True)
    assert (
        ps_http.get_metric_value("pageserver_tenant_manager_slots") == 0
    ), "tenant removed before we deletion was issued"
    env.storage_controller.attach_hook_drop(tenant_id)

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

    time.sleep(4)
    ts_after_deletion = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    ps_http.tenant_time_travel_remote_storage(
        tenant_id, timestamp=ts_before_deletion, done_if_after=ts_after_deletion
    )

    generation = env.storage_controller.attach_hook_issue(tenant_id, env.pageserver.id)

    ps_http.tenant_attach(tenant_id, generation=generation)
    env.pageserver.quiesce_tenants()
 
    for branch in ["main", "first", "second"]:
        timeline_id = timeline_ids[branch]
        detail = ps_http.timeline_detail(tenant_id, timeline_id)
        log.info(f"Timeline {branch}/{timeline_id} detail: {detail}")

        # We expect that we restored pageserver state up to last_flush_lsn, because we flushed that to
        # remote storage before we set our recovery timestamp.
        #assert Lsn(detail["last_record_lsn"]) >= last_flush_lsns[branch]

        # Check that we can indeed read from this recovered timeline
        with env.endpoints.create_start(branch, tenant_id=tenant_id) as endpoint:
            endpoint.safe_psql(f"SELECT * FROM created_{branch};")
            last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
            expected_last_flush_lsn = last_flush_lsns[branch]
            # There might be some activity that advances the lsn so we can't use a strict equality check
            assert last_flush_lsn >= expected_last_flush_lsn, "last_flush_lsn too old"

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 1
