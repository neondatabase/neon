import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PgBin,
    RemoteStorageKind,
    available_remote_storages,
    wait_for_last_flush_lsn,
)
from fixtures.pageserver.http import PageserverApiException
from fixtures.pageserver.utils import (
    tenant_delete_wait_completed,
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

    tenant_id = ps_http.tenant_create(
        tenant_id,
        conf={
            "gc_period": "0s",
            "compaction_period": "0s",
            "checkpoint_distance": 1024**2,
            "image_creation_threshold": 100,
        },
    )

    # create two timelines
    for timeline in ["first", "second"]:
        timeline_id = env.neon_cli.create_timeline(timeline, tenant_id=tenant_id)
        with env.endpoints.create_start(timeline, tenant_id=tenant_id) as endpoint:
            run_pg_bench_small(pg_bin, endpoint.connstr())
            wait_for_last_flush_lsn(env, endpoint, tenant=tenant_id, timeline=timeline_id)

    iterations = 20 if remote_storage_kind is RemoteStorageKind.REAL_S3 else 4
    tenant_delete_wait_completed(ps_http, tenant_id, iterations)

    tenant_path = env.tenant_dir(tenant_id=tenant_id)
    assert not tenant_path.exists()
