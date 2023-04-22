import threading
import timeit
from threading import BoundedSemaphore

import pytest
from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import NeonCompare
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

        tenant, _ = env.neon_cli.create_tenant()
        env.neon_cli.create_timeline(
            f"test_bulk_tenant_create_{tenants_count}_{i}", tenant_id=tenant
        )

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


@pytest.mark.parametrize("tenants_count", [50])
def test_parallel_tenant_create(
    neon_compare: NeonCompare,
    tenants_count: int,
):
    """Create lots of tenants in parallel

    One important thing that this measures is the amount of prometheus
    metrics per tenant. The pageserver exposes a lot of metrics for
    each timeline, and this test gives some visibility to how much
    exactly.  (We've had to raise the prometheus scraper's limit on
    the max metrics size several times, because we expose so many.)
    """
    env = neon_compare.env
    zenbenchmark = neon_compare.zenbenchmark

    max_concurrent = 5
    pool_sema = BoundedSemaphore(value=max_concurrent)

    def worker(i: int):
        with pool_sema:
            tenant, timeline_id = env.neon_cli.create_tenant()

            endpoint_tenant = env.endpoints.create_start("main", tenant_id=tenant)

            with endpoint_tenant.cursor() as cur:
                cur.execute("select count(*) from pg_class")
                endpoint_tenant.stop()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(tenants_count)]
    start = timeit.default_timer()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    end = timeit.default_timer()

    zenbenchmark.record(
        "tenant_creation_time",
        end - start,
        "s",
        report=MetricReport.LOWER_IS_BETTER,
    )

    metrics = env.pageserver.http_client().get_metrics_str()
    zenbenchmark.record(
        "prometheus_metrics_size",
        len(metrics) / tenants_count,
        "bytes",
        report=MetricReport.LOWER_IS_BETTER,
    )
