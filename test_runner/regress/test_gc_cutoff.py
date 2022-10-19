import pytest
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin
from performance.test_perf_pgbench import get_scales_matrix


# Test gc_cuttoff
#
# This test set fail point after at the end of GC and checks
# that pageserver normally restarts after it
@pytest.mark.parametrize("scale", get_scales_matrix(10))
def test_gc_cutoff(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin, scale: int):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # Use aggressive GC and checkpoint settings, so that we also exercise GC during the test
    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            "gc_period": "10 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "1 s",
        }
    )
    pg = env.postgres.create_start("main", tenant_id=tenant_id)
    connstr = pg.connstr()
    pg_bin.run_capture(["pgbench", "-i", f"-s{scale}", connstr])

    pageserver_http.configure_failpoints(("gc-before-save-metadata", "return"))

    for i in range(5):
        try:
            pg_bin.run_capture(["pgbench", "-T100", connstr])
        except Exception:
            env.pageserver.stop()
            env.pageserver.start()
            pageserver_http.configure_failpoints(("gc-before-save-metadata", "return"))
