import time
from datetime import datetime, timezone

from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.utils import (
    MANY_SMALL_LAYERS_TENANT_CONFIG,
    assert_prefix_empty,
    assert_prefix_not_empty,
    poll_for_remote_storage_iterations,
    tenant_delete_wait_completed,
)
from fixtures.remote_storage import s3_storage
from fixtures.types import TenantId
from fixtures.utils import run_pg_bench_small


def test_tenant_s3_restore(
    neon_env_builder: NeonEnvBuilder,
    pg_bin: PgBin,
):
    remote_storage_kind = s3_storage()
    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [
            # The deletion queue will complain when it encounters simulated S3 errors
            ".*deletion executor: DeleteObjects request failed.*",
            # lucky race with stopping from flushing a layer we fail to schedule any uploads
            ".*layer flush task.+: could not flush frozen layer: update_metadata_file",
        ]
    )

    ps_http = env.pageserver.http_client()

    tenant_id = TenantId.generate()

    env.neon_cli.create_tenant(
        tenant_id=tenant_id,
        conf=MANY_SMALL_LAYERS_TENANT_CONFIG,
    )

    # Default tenant and the one we created
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 2

    # create two timelines one being the parent of another, both with non-trivial data
    parent = None
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_branch(
            timeline, tenant_id=tenant_id, ancestor_branch_name=parent
        )
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

            assert_prefix_not_empty(
                neon_env_builder.pageserver_remote_storage,
                prefix="/".join(
                    (
                        "tenants",
                        str(tenant_id),
                    )
                ),
            )

        parent = timeline

    # These sleeps are important because they fend off differences in clocks between us and S3
    time.sleep(4)
    ts_before_deletion = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    time.sleep(4)

    iterations = poll_for_remote_storage_iterations(remote_storage_kind)

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 2
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 1
    env.attachment_service.attach_hook_drop(tenant_id)

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

    generation = env.attachment_service.attach_hook_issue(tenant_id, env.pageserver.id)

    ps_http.tenant_attach(tenant_id, generation=generation)

    time.sleep(4)

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots") == 2
