from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnvBuilder


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
