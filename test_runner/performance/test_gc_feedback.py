import pytest
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.timeout(10000)
def test_gc_feedback(neon_env_builder: NeonEnvBuilder, zenbenchmark: NeonBenchmarker):
    """
    Test that GC is able to collect all old layers even if them are forming
    "stairs" and there are not three delta layers since last image layer.

    Information about image layers needed to collect old layers should
    be propagated by GC to compaction task which should take in in account
    when make a decision which new image layers needs to be created.
    """
    env = neon_env_builder.init_start()
    client = env.pageserver.http_client()

    tenant_id, _ = env.neon_cli.create_tenant(
        conf={
            # disable default GC and compaction
            "gc_period": "1000 m",
            "compaction_period": "0 s",
            "gc_horizon": f"{1024 ** 2}",
            "checkpoint_distance": f"{1024 ** 2}",
            "compaction_target_size": f"{1024 ** 2}",
            # set PITR interval to be small, so we can do GC
            "pitr_interval": "10 s",
            # "compaction_threshold": "3",
            # "image_creation_threshold": "2",
        }
    )
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    timeline_id = endpoint.safe_psql("show neon.timeline_id")[0][0]
    n_steps = 10
    n_update_iters = 100
    step_size = 10000
    with endpoint.cursor() as cur:
        cur.execute("SET statement_timeout='1000s'")
        cur.execute(
            "CREATE TABLE t(step bigint, count bigint default 0, payload text default repeat(' ', 100))  with (fillfactor=50)"
        )
        cur.execute("CREATE INDEX ON t(step)")
        # In each step, we insert 'step_size' new rows, and update the newly inserted rows
        # 'n_update_iters' times. This creates a lot of churn and generates lots of WAL at the end of the table,
        # without modifying the earlier parts of the table.
        for step in range(n_steps):
            cur.execute(f"INSERT INTO t (step) SELECT {step} FROM generate_series(1, {step_size})")
            for _ in range(n_update_iters):
                cur.execute(f"UPDATE t set count=count+1 where step = {step}")
                cur.execute("vacuum t")

            # cur.execute("select pg_table_size('t')")
            # logical_size = cur.fetchone()[0]
            logical_size = client.timeline_detail(tenant_id, timeline_id)["current_logical_size"]
            log.info(f"Logical storage size  {logical_size}")

            client.timeline_checkpoint(tenant_id, timeline_id)

            # Do compaction and GC
            client.timeline_gc(tenant_id, timeline_id, 0)
            client.timeline_compact(tenant_id, timeline_id)
            # One more iteration to check that no excessive image layers are generated
            client.timeline_gc(tenant_id, timeline_id, 0)
            client.timeline_compact(tenant_id, timeline_id)

            physical_size = client.timeline_detail(tenant_id, timeline_id)["current_physical_size"]
            log.info(f"Physical storage size {physical_size}")

    MB = 1024 * 1024
    zenbenchmark.record("logical_size", logical_size // MB, "Mb", MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record("physical_size", physical_size // MB, "Mb", MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record(
        "physical/logical ratio", physical_size / logical_size, "", MetricReport.LOWER_IS_BETTER
    )
