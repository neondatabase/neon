import os
import shutil
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import List

import pytest
from fixtures.log_helper import log
from fixtures.metrics import PAGESERVER_PER_TENANT_METRICS, parse_metrics
from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder
from fixtures.types import Lsn, TenantId
from prometheus_client.samples import Sample


def test_tenant_creation_fails(neon_simple_env: NeonEnv):
    tenants_dir = Path(neon_simple_env.repo_dir) / "tenants"
    initial_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    initial_tenant_dirs = set([d for d in tenants_dir.iterdir()])

    pageserver_http = neon_simple_env.pageserver.http_client()
    pageserver_http.configure_failpoints(("tenant-creation-before-tmp-rename", "return"))
    with pytest.raises(Exception, match="tenant-creation-before-tmp-rename"):
        _ = neon_simple_env.neon_cli.create_tenant()

    new_tenants = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    assert initial_tenants == new_tenants, "should not create new tenants"

    new_tenant_dirs = list(set([d for d in tenants_dir.iterdir()]) - initial_tenant_dirs)
    assert len(new_tenant_dirs) == 1, "should have new tenant directory created"
    tmp_tenant_dir = new_tenant_dirs[0]
    assert str(tmp_tenant_dir).endswith(
        ".___temp"
    ), "new tenant directory created should be a temporary one"

    neon_simple_env.pageserver.stop()
    neon_simple_env.pageserver.start()

    tenants_after_restart = sorted(
        map(lambda t: t.split()[0], neon_simple_env.neon_cli.list_tenants().stdout.splitlines())
    )
    dirs_after_restart = set([d for d in tenants_dir.iterdir()])
    assert (
        tenants_after_restart == initial_tenants
    ), "should load all non-corrupt tenants after restart"
    assert (
        dirs_after_restart == initial_tenant_dirs
    ), "pageserver should clean its temp tenant dirs on restart"


def test_tenants_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    """Tests tenants with and without wal acceptors"""
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_tenants_normal_work", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start(
        "test_tenants_normal_work",
        tenant_id=tenant_1,
    )
    pg_tenant2 = env.postgres.create_start(
        "test_tenants_normal_work",
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
                assert cur.fetchone() == (5000050000,)


def test_metrics_normal_work(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    timeline_1 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_1)
    timeline_2 = env.neon_cli.create_timeline("test_metrics_normal_work", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start("test_metrics_normal_work", tenant_id=tenant_1)
    pg_tenant2 = env.postgres.create_start("test_metrics_normal_work", tenant_id=tenant_2)

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    collected_metrics = {
        "pageserver": env.pageserver.http_client().get_metrics(),
    }
    for sk in env.safekeepers:
        collected_metrics[f"safekeeper{sk.id}"] = sk.http_client().get_metrics_str()

    for name in collected_metrics:
        basepath = os.path.join(neon_env_builder.repo_dir, f"{name}.metrics")

        with open(basepath, "w") as stdout_f:
            print(collected_metrics[name], file=stdout_f, flush=True)

    all_metrics = [parse_metrics(m, name) for name, m in collected_metrics.items()]
    ps_metrics = all_metrics[0]
    sk_metrics = all_metrics[1:]

    ttids = [
        {"tenant_id": str(tenant_1), "timeline_id": str(timeline_1)},
        {"tenant_id": str(tenant_2), "timeline_id": str(timeline_2)},
    ]

    # Test metrics per timeline
    for tt in ttids:
        log.info(f"Checking metrics for {tt}")

        ps_lsn = Lsn(int(ps_metrics.query_one("pageserver_last_record_lsn", filter=tt).value))
        sk_lsns = [
            Lsn(int(sk.query_one("safekeeper_commit_lsn", filter=tt).value)) for sk in sk_metrics
        ]

        log.info(f"ps_lsn: {ps_lsn}")
        log.info(f"sk_lsns: {sk_lsns}")

        assert ps_lsn <= max(sk_lsns)
        assert ps_lsn > Lsn(0)

    # Test common metrics
    for metrics in all_metrics:
        log.info(f"Checking common metrics for {metrics.name}")

        log.info(
            f"process_cpu_seconds_total: {metrics.query_one('process_cpu_seconds_total').value}"
        )
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


def test_pageserver_metrics_removed_after_detach(neon_env_builder: NeonEnvBuilder):
    """Tests that when a tenant is detached, the tenant specific metrics are not left behind"""

    neon_env_builder.num_safekeepers = 3

    env = neon_env_builder.init_start()
    tenant_1, _ = env.neon_cli.create_tenant()
    tenant_2, _ = env.neon_cli.create_tenant()

    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_1)
    env.neon_cli.create_timeline("test_metrics_removed_after_detach", tenant_id=tenant_2)

    pg_tenant1 = env.postgres.create_start("test_metrics_removed_after_detach", tenant_id=tenant_1)
    pg_tenant2 = env.postgres.create_start("test_metrics_removed_after_detach", tenant_id=tenant_2)

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5000050000,)

    def get_ps_metric_samples_for_tenant(tenant_id: TenantId) -> List[Sample]:
        ps_metrics = parse_metrics(env.pageserver.http_client().get_metrics(), "pageserver")
        samples = []
        for metric_name in ps_metrics.metrics:
            for sample in ps_metrics.query_all(
                name=metric_name, filter={"tenant_id": str(tenant_id)}
            ):
                samples.append(sample)
        return samples

    for tenant in [tenant_1, tenant_2]:
        pre_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        assert pre_detach_samples == set(PAGESERVER_PER_TENANT_METRICS)

        env.pageserver.http_client().tenant_detach(tenant)

        post_detach_samples = set([x.name for x in get_ps_metric_samples_for_tenant(tenant)])
        assert post_detach_samples == set()


def test_pageserver_with_empty_tenants(neon_simple_env: NeonEnv):
    env = neon_simple_env
    client = env.pageserver.http_client()

    tenant_without_timelines_dir = env.initial_tenant
    shutil.rmtree(Path(env.repo_dir) / "tenants" / str(tenant_without_timelines_dir) / "timelines")

    tenant_with_empty_timelines_dir = client.tenant_create()
    for timeline_dir_entry in Path.iterdir(
        Path(env.repo_dir) / "tenants" / str(tenant_with_empty_timelines_dir) / "timelines"
    ):
        if timeline_dir_entry.is_dir():
            shutil.rmtree(timeline_dir_entry)
        else:
            timeline_dir_entry.unlink()

    env.postgres.stop_all()
    for _ in range(0, 3):
        env.pageserver.stop()
        env.pageserver.start()

    client = env.pageserver.http_client()
    tenants = client.tenant_list()

    assert (
        len(tenants) == 1
    ), "Pageserver should attach only tenants with empty timelines/ dir on restart"
    loaded_tenant = tenants[0]
    assert loaded_tenant["id"] == str(
        tenant_with_empty_timelines_dir
    ), f"Tenant {tenant_with_empty_timelines_dir} should be loaded as the only one with tenants/ directory"
    assert loaded_tenant["state"] == {
        "Active": {"background_jobs_running": False}
    }, "Empty tenant should be loaded and ready for timeline creation"
