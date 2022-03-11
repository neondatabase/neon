from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder


def test_tenant_config(zenith_env_builder: ZenithEnvBuilder):
    env = zenith_env_builder.init_start()
    """Test per tenant configuration"""
    tenant = env.zenith_cli.create_tenant(
        conf={
            'gc_period': '100sec',
            'gc_horizon': '1024',
            'pitr_interval': '3600sec',
            'checkpoint_distance': '10000',
            'compaction_period': '60sec'
        })

    env.zenith_cli.create_timeline(f'test_tenant_conf', tenant_id=tenant)
    pg = env.postgres.create_start(
        "test_tenant_conf",
        "main",
        tenant,
    )

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            assert pscur.fetchone() == (10000, 60, 1024, 100, 3600)
