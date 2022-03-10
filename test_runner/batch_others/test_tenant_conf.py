from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder

def test_tenants_normal_work(zenith_env_builder: ZenithEnvBuilder):
    env = zenith_env_builder.init()
    """Test per tenant configuration"""
    tenant = env.create_tenant(conf={'gc_period':'100sec','gc_horizon':'1024','pitr_interval':'3600sec','checkpoint_distance':'10000','checkpoint_period':'60sec'})

    pg = env.postgres.create_start(
        "test_tenant_conf",
        "main",
        tenant,
    )

    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor() as pscur:
            pscur.execute(f"show {tenant.hex}")
            assert pscur.fetchone() == (10000, 60, 1024, 100, 3600)
