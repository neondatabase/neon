import queue
import threading
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, wait_for_last_flush_lsn
from fixtures.types import TenantId

"""
553  sudo mkfs.ext4 /dev/nvme1n1
555  mkdir test_output
556  sudo mount /dev/nvme1n1 test_output
557  htop
559  ./scripts/pysync
560  NEON_BIN=/home/admin/neon/target/release DEFAULT_PG_VERSION=15 ./scripts/pytest --preserve-database-files --timeout=0 ./test_runner/performance/test_pageserver_startup_many_tenants.py
561  sudo chown -R admin:admin test_output

cargo build_testing --release

562  NEON_BIN=$PWD/target/release DEFAULT_PG_VERSION=15 ./scripts/pytest --preserve-database-files --timeout=0 ./test_runner/performance/test_pageserver_startup_many_tenants.py

cd test_output/test_pageserver_startup_many_tenants/repo

sudo env  NEON_REPO_DIR=$PWD prlimit --nofile=300000:300000  ../../../target/release/neon_local start
# watch initial load complete, then background jobs start. That's the interesting part.
sudo env  NEON_REPO_DIR=$PWD prlimit --nofile=300000:300000  ../../../target/release/neon_local stop
# usually pageserver won't be responsive, kill with
sudo pkill -9 pageserver
"""
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

    tenant_dir = env.repo_dir / "pageserver_1" / "tenants" / str(env.initial_tenant)

    for i in range(0, 20_000):
        import shutil

        shutil.copytree(tenant_dir, tenant_dir.parent / str(TenantId.generate()))
