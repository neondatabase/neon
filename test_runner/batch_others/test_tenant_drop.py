from contextlib import closing

import pytest
import time

from fixtures.zenith_fixtures import (
    TenantFactory,
    ZenithCli,
    PostgresFactory,
)

from fixtures.zenith_fixtures import ZenithPageserver


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_tenant_drop(
    zenith_cli: ZenithCli,
    tenant_factory: TenantFactory,
    postgres: PostgresFactory,
    wa_factory,
    with_wal_acceptors: bool,
    pageserver: ZenithPageserver,
):
    """Tests tenant drop with and without wal acceptors"""
    tenant_1 = tenant_factory.create()
    tenant_2 = tenant_factory.create()

    tenants = pageserver.http_client().tenant_list()
    assert tenant_1 in {t['id'] for t in tenants if t['state'] == 'Active'}
    assert tenant_2 in {t['id'] for t in tenants if t['state'] == 'Active'}

    zenith_cli.run([
        "branch",
        f"test_tenant_drop_with_wal_acceptors{with_wal_acceptors}",
        "main",
        f"--tenantid={tenant_1}"
    ])
    zenith_cli.run([
        "branch",
        f"test_tenant_drop_with_wal_acceptors{with_wal_acceptors}",
        "main",
        f"--tenantid={tenant_2}"
    ])
    if with_wal_acceptors:
        wa_factory.start_n_new(3)

    pg_tenant1 = postgres.create_start(
        f"test_tenant_drop_with_wal_acceptors{with_wal_acceptors}",
        None,  # branch name, None means same as node name
        tenant_1,
        wal_acceptors=wa_factory.get_connstrs() if with_wal_acceptors else None,
    )
    pg_tenant2 = postgres.create_start(
        f"test_tenant_drop_with_wal_acceptors{with_wal_acceptors}",
        None,  # branch name, None means same as node name
        tenant_2,
        wal_acceptors=wa_factory.get_connstrs() if with_wal_acceptors else None,
    )

    for pg in [pg_tenant1, pg_tenant2]:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute("CREATE TABLE t(key int primary key, value text)")
                cur.execute("INSERT INTO t SELECT generate_series(1,100), 'payload'")
                cur.execute("SELECT sum(key) FROM t")
                assert cur.fetchone() == (5050, )

    tenant_2 = tenant_factory.drop(tenant_2)

    tenants = pageserver.http_client().tenant_list()
    assert tenant_1 in {t['id'] for t in tenants if t['state'] == 'Active'}
    assert tenant_2 in {t['id'] for t in tenants if t['state'] == 'Stopping'}

    time.sleep(5)

    tenants = pageserver.http_client().tenant_list()
    assert tenant_1 in {t['id'] for t in tenants if t['state'] == 'Active'}
    assert tenant_2 in {t['id'] for t in tenants if t['state'] == 'CloudOnly'}
