import queue
import threading
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.types import TenantId


def test_pageserver_startup_many_tenants(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()

    #  below doesn't work because summaries contain tenant and timeline ids and we check for them

    tenant_id, timeline_id = env.initial_tenant, env.initial_timeline
    pshttp = env.pageserver.http_client()
    ep = env.endpoints.create_start("main")
    ep.safe_psql("create table foo(b text)")
    for i in range(0, 8):
        ep.safe_psql("insert into foo(b) values ('some text')")
        # pg_bin.run_capture(["pgbench", "-i", "-s1", ep.connstr()])
        wait_for_last_flush_lsn(env, ep, tenant_id, timeline_id)
        pshttp.timeline_checkpoint(tenant_id, timeline_id)
    ep.stop_and_destroy()

    env.pageserver.stop()
    for sk in env.safekeepers:
        sk.stop()

    tenant_dir = env.repo_dir / "tenants" / str(env.initial_tenant)

    for i in range(0, 20_000):
        import shutil

        shutil.copytree(tenant_dir, tenant_dir.parent / str(TenantId.generate()))
