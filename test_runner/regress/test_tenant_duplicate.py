import time
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    last_flush_lsn_upload,
)
from fixtures.remote_storage import (
    RemoteStorageKind,
)
from fixtures.types import TenantId
from fixtures.log_helper import log


def test_tenant_duplicate(
    neon_env_builder: NeonEnvBuilder,
):
    neon_env_builder.enable_pageserver_remote_storage(RemoteStorageKind.LOCAL_FS)

    env = neon_env_builder.init_start()

    with env.endpoints.create_start("main", tenant_id=env.initial_tenant) as ep_main:
        ep_main.safe_psql("CREATE TABLE foo (i int);")
        ep_main.safe_psql("INSERT INTO foo VALUES (1), (2), (3);")
        last_flush_lsn = last_flush_lsn_upload(
            env, ep_main, env.initial_tenant, env.initial_timeline
        )

    new_tenant_id = TenantId.generate()
    # timeline id remains unchanged with tenant_duplicate
    # TODO: implement a remapping scheme so timeline ids remain globally unique
    new_timeline_id = env.initial_timeline

    log.info(f"Duplicate tenant/timeline will be: {new_tenant_id}/{new_timeline_id}")

    ps_http = env.pageserver.http_client()

    ps_http.tenant_duplicate(env.initial_tenant, new_tenant_id)

    ps_http.tenant_delete(env.initial_tenant)

    env.neon_cli.map_branch("duplicate", new_tenant_id, new_timeline_id)

    # start read-only replicate and validate
    with env.endpoints.create_start(
        "duplicate", tenant_id=new_tenant_id, lsn=last_flush_lsn
    ) as ep_dup:
        with ep_dup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM foo ORDER BY i;")
                cur.fetchall() == [(1,), (2,), (3,)]

    # ensure restarting PS works
    env.pageserver.stop()
    env.pageserver.start()

