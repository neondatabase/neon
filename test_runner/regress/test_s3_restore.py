from __future__ import annotations

import time
from datetime import UTC, datetime

from fixtures.common_types import Lsn
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
)
from fixtures.pageserver.utils import (
    assert_prefix_empty,
    enable_remote_storage_versioning,
    many_small_layers_tenant_config,
    wait_for_upload,
)
from fixtures.remote_storage import RemoteStorageKind, s3_storage
from fixtures.utils import run_pg_bench_small


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

    # change it back after initdb, recovery doesn't work if the two
    # index_part.json uploads happen at same second or too close to each other.
    initial_tenant_conf = many_small_layers_tenant_config()
    del initial_tenant_conf["checkpoint_distance"]

    env = neon_env_builder.init_start(initial_tenant_conf)
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

    # now lets create the small layers
    env.storage_controller.pageserver_api().set_tenant_config(
        tenant_id, many_small_layers_tenant_config()
    )

    # Default tenant and the one we created
    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1

    # create two timelines one being the parent of another, both with non-trivial data
    parent = "main"
    last_flush_lsns = []

    for timeline in ["first", "second"]:
        timeline_id = env.create_branch(timeline, ancestor_branch_name=parent, tenant_id=tenant_id)
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            endpoint.safe_psql(f"CREATE TABLE created_{timeline}(id integer);")
            last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
            last_flush_lsns.append(last_flush_lsn)
        ps_http.timeline_checkpoint(tenant_id, timeline_id)
        wait_for_upload(ps_http, tenant_id, timeline_id, last_flush_lsn)
        log.info(f"{timeline} timeline {timeline_id} {last_flush_lsn=}")
        parent = timeline

    # These sleeps are important because they fend off differences in clocks between us and S3
    time.sleep(4)
    ts_before_deletion = datetime.now(tz=UTC).replace(tzinfo=None)
    time.sleep(4)

    assert (
        ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1
    ), "tenant removed before we deletion was issued"
    ps_http.tenant_delete(tenant_id)
    ps_http.deletion_queue_flush(execute=True)
    assert (
        ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 0
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
    ts_after_deletion = datetime.now(tz=UTC).replace(tzinfo=None)
    time.sleep(4)

    ps_http.tenant_time_travel_remote_storage(
        tenant_id, timestamp=ts_before_deletion, done_if_after=ts_after_deletion
    )

    generation = env.storage_controller.attach_hook_issue(tenant_id, env.pageserver.id)

    ps_http.tenant_attach(tenant_id, generation=generation)
    env.pageserver.quiesce_tenants()

    for tline in ps_http.timeline_list(env.initial_tenant):
        log.info(f"timeline detail: {tline}")

    for i, timeline in enumerate(["first", "second"]):
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            endpoint.safe_psql(f"SELECT * FROM created_{timeline};")
            last_flush_lsn = Lsn(endpoint.safe_psql("SELECT pg_current_wal_flush_lsn()")[0][0])
            expected_last_flush_lsn = last_flush_lsns[i]
            # There might be some activity that advances the lsn so we can't use a strict equality check
            assert last_flush_lsn >= expected_last_flush_lsn, "last_flush_lsn too old"

    assert ps_http.get_metric_value("pageserver_tenant_manager_slots", {"mode": "attached"}) == 1
