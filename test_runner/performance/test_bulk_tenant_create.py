from __future__ import annotations

import timeit

import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.neon_fixtures import NeonEnvBuilder

# Run bulk tenant creation test.
#
# Collects metrics:
#
# 1. Time to create {1,10,50} tenants
# 2. Average creation time per tenant


@pytest.mark.parametrize("tenants_count", [1, 5, 10])
def test_bulk_tenant_create(
    neon_env_builder: NeonEnvBuilder,
    tenants_count: int,
    zenbenchmark,
):
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    time_slices = []

    for i in range(tenants_count):
        start = timeit.default_timer()

        tenant, _ = env.create_tenant()
        env.create_timeline(f"test_bulk_tenant_create_{tenants_count}_{i}", tenant_id=tenant)

        # FIXME: We used to start new safekeepers here. Did that make sense? Should we do it now?
        # if use_safekeepers == 'with_sa':
        #    wa_factory.start_n_new(3)

        endpoint_tenant = env.endpoints.create_start(
            f"test_bulk_tenant_create_{tenants_count}_{i}", tenant_id=tenant
        )

        end = timeit.default_timer()
        time_slices.append(end - start)

        endpoint_tenant.stop()

    zenbenchmark.record(
        "tenant_creation_time",
        sum(time_slices) / len(time_slices),
        "s",
        report=MetricReport.LOWER_IS_BETTER,
    )
