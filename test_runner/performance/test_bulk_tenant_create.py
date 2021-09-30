import timeit
import pytest

from contextlib import closing
from fixtures.zenith_fixtures import (
    TenantFactory,
    ZenithCli,
    PostgresFactory,
)

pytest_plugins = ("fixtures.benchmark_fixture")

# Run bulk tenant cration test.
#
# Collects metrics:
#
# 1. Time to create {1,10,50} tenants
# 2. Average creation time per tenant

@pytest.mark.parametrize('tenants_count', [1, 10, 50])
def test_bulk_tenant_create(
    zenith_cli: ZenithCli,
    tenant_factory: TenantFactory,
    postgres: PostgresFactory,
    tenants_count: int,
    zenbenchmark,
):
    """Measure tenant creation time"""
    start = timeit.default_timer()
    for i in range(tenants_count):
        tenant = tenant_factory.create()
        zenith_cli.run(["branch", f"test_bulk_tenant_create_{tenants_count}_{i}", "main", f"--tenantid={tenant}"])
        pg_tenant = postgres.create_start(
            f"test_bulk_tenant_create_{tenants_count}_{i}",
            tenant,
            wal_acceptors=None,
        )
        with closing(pg_tenant.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                assert cur.fetchone() == (1,)
    end = timeit.default_timer()

    zenbenchmark.record('tenant_creation_time_total', end - start, 's')
    zenbenchmark.record('tenant_creation_time_avg', (end - start) / tenants_count, 's')
