from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder


def test_tenant_config(zenith_env_builder: ZenithEnvBuilder):
    env = zenith_env_builder.init_start()
    """Test per tenant configuration"""
    tenant = env.zenith_cli.create_tenant(
        conf={
            'checkpoint_distance': '10000',
            'compaction_target_size': '1048576',
            'compaction_period': '60sec',
            'compaction_threshold': '20',
            'gc_horizon': '1024',
            'gc_period': '100sec',
            'pitr_interval': '3600sec',
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
            assert pscur.fetchone() == (10000, 1048576, 60, 20, 1024, 100, 3600)

    # update the config and ensure that it has changed
    env.zenith_cli.config_tenant(tenant_id=tenant,
                                 conf={
                                     'checkpoint_distance': '100000',
                                     'compaction_target_size': '1048576',
                                     'compaction_period': '30sec',
                                     'compaction_threshold': '15',
                                     'gc_horizon': '256',
                                     'gc_period': '10sec',
                                     'pitr_interval': '360sec',
                                 })

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            assert pscur.fetchone() == (100000, 1048576, 30, 15, 256, 10, 360)
