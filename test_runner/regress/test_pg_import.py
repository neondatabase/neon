from typing import Optional

import pytest
from fixtures.common_types import TenantId, TimelineId
from fixtures.neon_fixtures import NeonEnvBuilder, VanillaPostgres
from fixtures.remote_storage import RemoteStorageKind

num_rows = 1000


@pytest.mark.parametrize("stripe_size", [None, int((1024**3) / 8192)])  # default and 1GiB
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
