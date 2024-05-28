from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    AuxFileStore,
    NeonEnvBuilder,
    logical_replication_sync,
)


def test_aux_v2_config_switch(neon_env_builder: NeonEnvBuilder, vanilla_pg):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")
    client = env.pageserver.http_client()

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    tenant_config = client.tenant_config(tenant_id).effective_config
    tenant_config["switch_aux_file_policy"] = AuxFileStore.V2
    client.set_tenant_config(tenant_id, tenant_config)
    # aux file v2 is enabled on the write path, so for now, it should be unset (or null)
    assert (
        client.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)["last_aux_file_policy"]
        is None
    )

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));"
    )
    cur.execute("create publication pub1 for table t, replication_example")

    # now start subscriber, aux files will be created at this point. TODO: find better ways of testing aux files (i.e., neon_test_utils)
    # instead of going through the full logical replication process.
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    vanilla_pg.safe_psql(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120), testcolumn1 int, testcolumn2 int, testcolumn3 int);"
    )
    connstr = endpoint.connstr().replace("'", "''")
    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)
    vanilla_pg.stop()
    endpoint.stop()

    with env.pageserver.http_client() as client:
        # aux file v2 flag should be enabled at this point
        assert (
            client.timeline_detail(tenant_id, timeline_id)["last_aux_file_policy"]
            == AuxFileStore.V2
        )
    with env.pageserver.http_client() as client:
        tenant_config = client.tenant_config(tenant_id).effective_config
        tenant_config["switch_aux_file_policy"] = "V1"
        client.set_tenant_config(tenant_id, tenant_config)
        # the flag should still be enabled
        assert (
            client.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)[
                "last_aux_file_policy"
            ]
            == AuxFileStore.V2
        )
    env.pageserver.restart()
    with env.pageserver.http_client() as client:
        # aux file v2 flag should be persisted
        assert (
            client.timeline_detail(tenant_id=tenant_id, timeline_id=timeline_id)[
                "last_aux_file_policy"
            ]
            == AuxFileStore.V2
        )
