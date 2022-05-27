from contextlib import closing
from datetime import datetime
import os
import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.utils import lsn_to_hex


@pytest.mark.parametrize('with_safekeepers', [False, True])
def test_tenants_normal_work(zenith_env_builder: ZenithEnvBuilder, with_safekeepers: bool):
    if with_safekeepers:
        zenith_env_builder.num_safekeepers = 3

    env = zenith_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.zenith_cli.create_tenant()
    tenant_2, _ = env.zenith_cli.create_tenant()

    env.zenith_cli.create_timeline(f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
                                   tenant_id=tenant_1)
    env.zenith_cli.create_timeline(f'test_tenants_normal_work_with_safekeepers{with_safekeepers}',
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


def test_metrics_normal_work(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3

    env = zenith_env_builder.init_start()
    tenant_1, _ = env.zenith_cli.create_tenant()
    tenant_2, _ = env.zenith_cli.create_tenant()

    timeline_1 = env.zenith_cli.create_timeline('test_metrics_normal_work', tenant_id=tenant_1)
    timeline_2 = env.zenith_cli.create_timeline('test_metrics_normal_work', tenant_id=tenant_2)

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
        basepath = os.path.join(zenith_env_builder.repo_dir, f'{name}.metrics')

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
