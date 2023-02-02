import pytest
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    RemoteStorageKind,
    wait_for_last_record_lsn,
    wait_for_upload,
)
from fixtures.types import Lsn, TenantId, TimelineId
from fixtures.utils import query_scalar


# Crates a few layers, ensures that we can evict them (removing locally but keeping track of them anyway)
# and then download them back.
@pytest.mark.parametrize("remote_storage_kind", [RemoteStorageKind.LOCAL_FS])
def test_basic_eviction(
    neon_env_builder: NeonEnvBuilder,
    remote_storage_kind: RemoteStorageKind,
):
    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_download_remote_layers_api",
    )

    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()
    pg = env.postgres.create_start("main")

    tenant_id = TenantId(pg.safe_psql("show neon.tenant_id")[0][0])
    timeline_id = TimelineId(pg.safe_psql("show neon.timeline_id")[0][0])

    # Create a number of layers in the tenant
    with pg.cursor() as cur:
        cur.execute("CREATE TABLE foo (t text)")
        cur.execute(
            """
            INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 5000000) g
            """
        )
        current_lsn = Lsn(query_scalar(cur, "SELECT pg_current_wal_flush_lsn()"))

    wait_for_last_record_lsn(client, tenant_id, timeline_id, current_lsn)
    client.timeline_checkpoint(tenant_id, timeline_id)
    wait_for_upload(client, tenant_id, timeline_id, current_lsn)

    timeline_path = env.repo_dir / "tenants" / str(tenant_id) / "timelines" / str(timeline_id)
    initial_local_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    assert (
        len(initial_local_layers) > 1
    ), f"Should create multiple layers for timeline, but got {initial_local_layers}"

    # Compare layer map dump with the local layers, ensure everything's present locally and matches
    initial_layer_map_info = client.layer_map_info(tenant_id=tenant_id, timeline_id=timeline_id)
    assert (
        not initial_layer_map_info.in_memory_layers
    ), "Should have no in memory layers after flushing"
    assert len(initial_local_layers) == len(
        initial_layer_map_info.historic_layers
    ), "Should have the same layers in memory and on disk"
    for returned_layer in initial_layer_map_info.historic_layers:
        assert (
            returned_layer.kind == "Delta"
        ), f"Did not create and expect image layers, but got {returned_layer}"
        assert (
            not returned_layer.remote
        ), f"All created layers should be present locally, but got {returned_layer}"

        local_layers = list(
            filter(lambda layer: layer.name == returned_layer.layer_file_name, initial_local_layers)
        )
        assert (
            len(local_layers) == 1
        ), f"Did not find returned layer {returned_layer} in local layers {initial_local_layers}"
        local_layer = local_layers[0]
        assert (
            returned_layer.layer_file_size == local_layer.stat().st_size
        ), f"Returned layer {returned_layer} has a different file size than local layer {local_layer}"

    # Detach all layers, ensre they are not in the local FS, but are still dumped as part of the layer map
    for local_layer in initial_local_layers:
        client.evict_layer(
            tenant_id=tenant_id, timeline_id=timeline_id, layer_name=local_layer.name
        )
        assert not any(
            new_local_layer.name == local_layer.name for new_local_layer in timeline_path.glob("*")
        ), f"Did not expect to find {local_layer} layer after evicting"

    empty_layers = list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    assert (
        not empty_layers
    ), f"After evicting all layers, timeline {tenant_id}/{timeline_id} should have no layers locally, but got: {empty_layers}"

    evicted_layer_map_info = client.layer_map_info(tenant_id=tenant_id, timeline_id=timeline_id)
    assert (
        not evicted_layer_map_info.in_memory_layers
    ), "Should have no in memory layers after flushing and evicting"
    assert len(initial_local_layers) == len(
        evicted_layer_map_info.historic_layers
    ), "Should have the same layers in memory and on disk initially"
    for returned_layer in evicted_layer_map_info.historic_layers:
        assert (
            returned_layer.kind == "Delta"
        ), f"Did not create and expect image layers, but got {returned_layer}"
        assert (
            returned_layer.remote
        ), f"All layers should be evicted and not present locally, but got {returned_layer}"
        assert any(
            local_layer.name == returned_layer.layer_file_name
            for local_layer in initial_local_layers
        ), f"Did not find returned layer {returned_layer} in local layers {initial_local_layers}"

    # redownload all evicted layers and ensure the initial state is restored
    for local_layer in initial_local_layers:
        client.download_layer(
            tenant_id=tenant_id, timeline_id=timeline_id, layer_name=local_layer.name
        )
    client.timeline_download_remote_layers(
        tenant_id,
        timeline_id,
        # allow some concurrency to unveil potential concurrency bugs
        max_concurrent_downloads=10,
        errors_ok=False,
        at_least_one_download=False,
    )

    redownloaded_layers = sorted(
        list(filter(lambda path: path.name != "metadata", timeline_path.glob("*")))
    )
    assert (
        redownloaded_layers == initial_local_layers
    ), "Should have the same layers locally after redownloading the evicted layers"
    redownloaded_layer_map_info = client.layer_map_info(
        tenant_id=tenant_id, timeline_id=timeline_id
    )
    assert (
        redownloaded_layer_map_info == initial_layer_map_info
    ), "Should have the same layer map after redownloading the evicted layers"
