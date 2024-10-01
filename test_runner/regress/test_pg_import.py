from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pytest
from fixtures.common_types import TenantId, TenantShardId, TimelineId
from fixtures.neon_fixtures import NeonEnvBuilder, VanillaPostgres
from fixtures.pageserver.http import HistoricLayerInfo, PageserverHttpClient
from fixtures.remote_storage import RemoteStorageKind

num_rows = 1000


@pytest.mark.parametrize("stripe_size", [None, 8, int((1024**3) / 8192)])  # default, tiny, 1GiB
@pytest.mark.parametrize("shard_count", [None, 8])  # unsharded and 8 shards
def test_pgdata_import_smoke(
    vanilla_pg: VanillaPostgres,
    neon_env_builder: NeonEnvBuilder,
    shard_count: Optional[int],
    stripe_size: Optional[int],
):
    # Put data in vanilla pg
    vanilla_pg.start()
    vanilla_pg.safe_psql("create user cloud_admin with password 'postgres' superuser")
    vanilla_pg.safe_psql(
        """create table t as select 'long string to consume some space' || g
     from generate_series(1,300000) g"""
    )
    assert vanilla_pg.safe_psql("select count(*) from t") == [(300000,)]

    vanilla_pg.stop()

    # We have a Postgres data directory to import now
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)
    env = neon_env_builder.init_start()

    tenant_id = TenantId.generate()
    env.storage_controller.tenant_create(
        tenant_id, shard_count=shard_count, shard_stripe_size=stripe_size
    )

    timeline_id = TimelineId.generate()
    env.storage_controller.timeline_import_from_pgdata(tenant_id, vanilla_pg.pgdatadir, timeline_id)

    env.neon_cli.map_branch("imported", tenant_id, timeline_id)

    endpoint = env.endpoints.create_start(branch_name="imported", tenant_id=tenant_id)

    assert endpoint.safe_psql("select count(*) from t") == [(300000,)]

    # test writing after the import
    endpoint.safe_psql("insert into t select g from generate_series(1, 1000) g")
    assert endpoint.safe_psql("select count(*) from t") == [(301000,)]

    endpoint.stop()
    endpoint.start()
    assert endpoint.safe_psql("select count(*) from t") == [(301000,)]

    #
    # validate the layer files in each shard only have the shard-specific data
    #
    shards = env.storage_controller.locate(tenant_id)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futs = []
        for shard in shards:
            shard_ps = env.get_pageserver(shard["node_id"])
            tenant_shard_id = TenantShardId.parse(shard["shard_id"])
            shard_ps_http = shard_ps.http_client()
            shard_layer_map = shard_ps_http.layer_map_info(tenant_shard_id, timeline_id)
            for layer in shard_layer_map.historic_layers:

                def do_layer(
                    shard_ps_http: PageserverHttpClient,
                    tenant_shard_id: TenantShardId,
                    timeline_id: TimelineId,
                    layer: HistoricLayerInfo,
                ):
                    return (
                        layer,
                        shard_ps_http.timeline_layer_scan_disposable_keys(
                            tenant_shard_id, timeline_id, layer.layer_file_name
                        ),
                    )

                futs.append(
                    executor.submit(do_layer, shard_ps_http, tenant_shard_id, timeline_id, layer)
                )
        for fut in futs:
            layer, result = fut.result()
            assert result["disposable_count"] == 0
            assert result["not_disposable_count"] > 0  # sanity check
