import concurrent.futures
import random
import time

from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pg_version import PgVersion
from fixtures.types import TenantId, TenantShardId, TimelineId


def test_sharding_service_many_tenants(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Check that we cope well with a not-totally-trivial number of tenants.

    This is checking for:
    - Obvious concurrency bugs from issuing many tenant creations/modifications
      concurrently.
    - Obvious scaling bugs like O(N^2) scaling that would be so slow that even
      a basic test starts failing from slowness.

    This is _not_ a comprehensive scale test: just a basic sanity check that
    we don't fall over for a thousand shards.
    """

    neon_env_builder.num_pageservers = 5

    env = neon_env_builder.init_start()

    # Total tenants
    tenant_count = 2000

    # Shards per tenant
    shard_count = 2
    stripe_size = 1024

    tenants = set(TenantId.generate() for _i in range(0, tenant_count))

    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    # We use a fixed seed to make the test reproducible: we want a randomly
    # chosen order, but not to change the order every time we run the test.
    rng = random.Random(1234)

    # We will create tenants directly via API, not via neon_local, to avoid any false
    # serialization of operations in neon_local (it e.g. loads/saves a config file on each call)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futs = []
        for tenant_id in tenants:
            f = executor.submit(
                env.storage_controller.tenant_create, tenant_id, shard_count, stripe_size
            )
            futs.append(f)

        # Wait for creations to finish
        for f in futs:
            f.result()

        # Generate a mixture of operations and dispatch them all concurrently
        futs = []
        for tenant_id in tenants:
            op = rng.choice([0, 1, 2])
            if op == 0:
                # A fan-out write operation to all shards in a tenant (timeline creation)
                f = executor.submit(
                    virtual_ps_http.timeline_create,
                    PgVersion.NOT_SET,
                    tenant_id,
                    TimelineId.generate(),
                )
            elif op == 1:
                # A reconciler operation: migrate a shard.
                shard_number = rng.randint(0, shard_count - 1)
                tenant_shard_id = TenantShardId(tenant_id, shard_number, shard_count)
                dest_ps_id = rng.choice([ps.id for ps in env.pageservers])
                f = executor.submit(
                    env.storage_controller.tenant_shard_migrate, tenant_shard_id, dest_ps_id
                )
            elif op == 2:
                # A passthrough read to shard zero
                f = executor.submit(virtual_ps_http.tenant_status, tenant_id)

            futs.append(f)

        # Wait for mixed ops to finish
        for f in futs:
            f.result()

    # Rolling node failures: this is a small number of requests, but results in a large
    # number of scheduler calls and reconcile tasks.
    for pageserver in env.pageservers:
        env.storage_controller.node_configure(pageserver.id, {"availability": "Offline"})
        # The sleeps are just to make sure we aren't optimizing-away any re-scheduling operations
        # from a brief flap in node state.
        time.sleep(1)
        env.storage_controller.node_configure(pageserver.id, {"availability": "Active"})
        time.sleep(1)

    # Restart the storage controller
    env.storage_controller.stop()
    env.storage_controller.start()

    # Restart pageservers: this exercises the /re-attach API
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()
