import timeit
import pytest

from fixtures.zenith_fixtures import (
    TenantFactory,
    ZenithCli,
    PostgresFactory,
)

pytest_plugins = ("fixtures.benchmark_fixture")

# Run bulk tenant creation test.
#
# Collects metrics:
#
# 1. Time to create {1,10,50} tenants
# 2. Average creation time per tenant


@pytest.mark.parametrize('tenants_count', [1, 5, 10])
@pytest.mark.parametrize('use_wal_acceptors', ['with_wa', 'without_wa'])
def test_bulk_tenant_create(
    zenith_cli: ZenithCli,
    tenant_factory: TenantFactory,
    postgres: PostgresFactory,
    wa_factory,
    use_wal_acceptors: str,
    tenants_count: int,
    zenbenchmark,
):
    """Measure tenant creation time (with and without wal acceptors)"""

    time_slices = []

    for i in range(tenants_count):
        start = timeit.default_timer()

        tenant = tenant_factory.create()
        zenith_cli.run([
            "branch",
            f"test_bulk_tenant_create_{tenants_count}_{i}_{use_wal_acceptors}",
            "main",
            f"--tenantid={tenant}"
        ])

        if use_wal_acceptors == 'with_wa':
            wa_factory.start_n_new(3)

        pg_tenant = postgres.create_start(
            f"test_bulk_tenant_create_{tenants_count}_{i}_{use_wal_acceptors}",
            None,  # branch name, None means same as node name
            tenant,
            wal_acceptors=wa_factory.get_connstrs() if use_wal_acceptors == 'with_wa' else None,
        )

        end = timeit.default_timer()
        time_slices.append(end - start)

        pg_tenant.stop()

    zenbenchmark.record('tenant_creation_time', sum(time_slices) / len(time_slices), 's')
