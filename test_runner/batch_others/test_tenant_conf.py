from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log


def test_tenant_config(zenith_env_builder: ZenithEnvBuilder):
    # set some non-default global config
    zenith_env_builder.pageserver_config_override = '''
page_cache_size=444;
wait_lsn_timeout='111 s';
tenant_config={checkpoint_distance = 10000, compaction_target_size = 1048576}'''

    env = zenith_env_builder.init_start()
    """Test per tenant configuration"""
    tenant = env.zenith_cli.create_tenant(conf={
        'checkpoint_distance': '20000',
        'gc_period': '30sec',
    })

    env.zenith_cli.create_timeline(f'test_tenant_conf', tenant_id=tenant)
    pg = env.postgres.create_start(
        "test_tenant_conf",
        "main",
        tenant,
    )

    # check the configuration of the default tenant
    # it should match global configuration
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {env.initial_tenant.hex}")
            res = pscur.fetchone()
            log.info(f"initial_tenant res: {res}")
            assert res == (10000, 1048576, 1, 10, 67108864, 100, 2592000)

    # check the configuration of the new tenant
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            res = pscur.fetchone()
            log.info(f"res: {res}")
            assert res == (20000, 1048576, 1, 10, 67108864, 30, 2592000)

    # update the config and ensure that it has changed
    env.zenith_cli.config_tenant(tenant_id=tenant,
                                 conf={
                                     'checkpoint_distance': '15000',
                                     'gc_period': '80sec',
                                 })

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            res = pscur.fetchone()
            log.info(f"after config res: {res}")
            assert res == (15000, 1048576, 1, 10, 67108864, 80, 2592000)

    # restart the pageserver and ensure that the config is still correct
    env.pageserver.stop()
    env.pageserver.start()

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            res = pscur.fetchone()
            log.info(f"after restart res: {res}")
            assert res == (15000, 1048576, 1, 10, 67108864, 80, 2592000)
