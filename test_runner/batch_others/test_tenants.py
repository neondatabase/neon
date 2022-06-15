from contextlib import closing
from datetime import datetime
import os
import pytest
import time
from uuid import UUID

from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.utils import lsn_to_hex
from fixtures.benchmark_fixture import MetricReport


@pytest.mark.parametrize('with_safekeepers', [False, True])
def test_tenants_normal_work(neon_env_builder: NeonEnvBuilder, with_safekeepers: bool):
    if with_safekeepers:
        neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline(f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
                                 tenant_id=tenant_1)
    env.neon_cli.create_timeline(f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
                                 tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start(
        f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
        tenant_id=tenant_1,
    )
    pg_tenant2 = env.postgres.create_start(
        f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
        tenant_id=tenant_2,
    )

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000, )


def test_tenant_threads(neon_env_builder, zenbenchmark):
    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    def get_num_threads() -> int:
        metrics = env.pageserver.http_client().get_metrics()
        parsed = parse_metrics(metrics)
        threads = parsed.query_one("process_threads").value
        return threads

    threads_before = get_num_threads()
    zenbenchmark.record("threads_before", threads_before, "", report=MetricReport.LOWER_IS_BETTER)

    tenants = env.pageserver.http_client().tenant_list()
    num_tenants = len(tenants)
    num_active = len([t for t in tenants if t["state"] == "Active"])
    zenbenchmark.record("tenants_before", num_tenants, "", report=MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record("active_before", num_active, "", report=MetricReport.LOWER_IS_BETTER)

    for i in range(20):
        print(f"creating tenant {i}")
        name = f"test_tenant_threads_{i}"
        tenant, _ = env.neon_cli.create_tenant()


        timeline = env.neon_cli.create_timeline(name, tenant_id=tenant)
        pg = env.postgres.create_start(name, tenant_id=tenant)
        pg.safe_psql("select 1;")
        pg.stop()
        env.pageserver.http_client().timeline_detach(tenant, timeline)

        remaining_timelines = [
            UUID(r["timeline_id"])
            for r in env.pageserver.http_client().timeline_list(tenant)
        ]
        for t in remaining_timelines:
            env.pageserver.http_client().timeline_detach(tenant, t)

    time.sleep(5)

    threads_after = get_num_threads()
    zenbenchmark.record("threads_before", threads_after, "", report=MetricReport.LOWER_IS_BETTER)

    tenants = env.pageserver.http_client().tenant_list()
    num_tenants = len(tenants)
    num_active = len([t for t in tenants if t["state"] == "Active"])
    zenbenchmark.record("tenants_after", num_tenants, "", report=MetricReport.LOWER_IS_BETTER)
    zenbenchmark.record("active_after", num_active, "", report=MetricReport.LOWER_IS_BETTER)


def test_metrics_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    timeline_1 = env.neon_cli.create_timeline('test_metrics_normal_work', tenant_id=tenant_1)
    timeline_2 = env.neon_cli.create_timeline('test_metrics_normal_work', tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start('test_metrics_normal_work', tenant_id=tenant_1)
    pg_tenant2 = env.postgres.create_start('test_metrics_normal_work', tenant_id=tenant_2)

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000, )

    collected_metrics = {
        "pageserver": env.pageserver.http_client().get_metrics(),
    }
    for sk in env.safekeepers:
        collected_metrics[f'safekeeper{sk.id}'] = sk.http_client().get_metrics_str()

    for name in collected_metrics:
        basepath = os.path.join(neon_env_builder.repo_dir, f'{name}.metrics')

        with open(basepath, 'w') as stdout_f:
            print(collected_metrics[name], file=stdout_f, flush=True)

    all_metrics = [parse_metrics(m, name) for name, m in collected_metrics.items()]
    ps_metrics = all_metrics[0]
    sk_metrics = all_metrics[1:]

    ttids = [{
        'tenant_id': tenant_1.hex, 'timeline_id': timeline_1.hex
    }, {
        'tenant_id': tenant_2.hex, 'timeline_id': timeline_2.hex
    }]

    # Test metrics per timeline
    for tt in ttids:
        log.info(f"Checking metrics for {tt}")

        ps_lsn = int(ps_metrics.query_one("pageserver_last_record_lsn", filter=tt).value)
        sk_lsns = [int(sk.query_one("safekeeper_commit_lsn", filter=tt).value) for sk in sk_metrics]

        log.info(f"ps_lsn: {lsn_to_hex(ps_lsn)}")
        log.info(f"sk_lsns: {list(map(lsn_to_hex, sk_lsns))}")

        assert ps_lsn <= max(sk_lsns)
        assert ps_lsn > 0

    # Test common metrics
    for metrics in all_metrics:
        log.info(f"Checking common metrics for {metrics.name}")

        log.info(
            f"process_cpu_seconds_total: {metrics.query_one('process_cpu_seconds_total').value}")
        log.info(f"process_threads: {int(metrics.query_one('process_threads').value)}")
        log.info(
            f"process_resident_memory_bytes (MB): {metrics.query_one('process_resident_memory_bytes').value / 1024 / 1024}"
        )
        log.info(
            f"process_virtual_memory_bytes (MB): {metrics.query_one('process_virtual_memory_bytes').value / 1024 / 1024}"
        )
        log.info(f"process_open_fds: {int(metrics.query_one('process_open_fds').value)}")
        log.info(f"process_max_fds: {int(metrics.query_one('process_max_fds').value)}")
        log.info(
            f"process_start_time_seconds (UTC): {datetime.fromtimestamp(metrics.query_one('process_start_time_seconds').value)}"
        )
