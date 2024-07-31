import concurrent.futures
import random
import time
from collections import defaultdict
from typing import Any, Dict

import pytest
from fixtures.common_types import TenantId, TenantShardId, TimelineId
from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PageserverAvailability,
    PageserverSchedulingPolicy,
)
from fixtures.pageserver.http import PageserverHttpClient
from fixtures.pg_version import PgVersion


def get_consistent_node_shard_counts(env: NeonEnv, total_shards) -> defaultdict[str, int]:
    """
    Get the number of shards attached to each node.
    This function takes into account the intersection of the intent and the observed state.
    If they do not match, it asserts out.
    """
    tenants = env.storage_controller.tenant_list()

    intent = dict()
    observed = dict()

    tenant_placement: defaultdict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "observed": {"attached": None, "secondary": []},
            "intent": {"attached": None, "secondary": []},
        }
    )

    for t in tenants:
        for node_id, loc_state in t["observed"]["locations"].items():
            if (
                loc_state is not None
                and "conf" in loc_state
                and loc_state["conf"] is not None
                and loc_state["conf"]["mode"]
                in set(["AttachedSingle", "AttachedMulti", "AttachedStale"])
            ):
                observed[t["tenant_shard_id"]] = int(node_id)
                tenant_placement[t["tenant_shard_id"]]["observed"]["attached"] = int(node_id)

            if (
                loc_state is not None
                and "conf" in loc_state
                and loc_state["conf"] is not None
                and loc_state["conf"]["mode"] == "Secondary"
            ):
                tenant_placement[t["tenant_shard_id"]]["observed"]["secondary"].append(int(node_id))

        if "attached" in t["intent"]:
            intent[t["tenant_shard_id"]] = t["intent"]["attached"]
            tenant_placement[t["tenant_shard_id"]]["intent"]["attached"] = t["intent"]["attached"]

        if "secondary" in t["intent"]:
            tenant_placement[t["tenant_shard_id"]]["intent"]["secondary"] += t["intent"][
                "secondary"
            ]

    log.info(f"{tenant_placement=}")

    matching = {
        tid: intent[tid] for tid in observed if tid in intent and intent[tid] == observed[tid]
    }
    assert len(matching) == total_shards

    attached_per_node: defaultdict[str, int] = defaultdict(int)
    for node_id in matching.values():
        attached_per_node[node_id] += 1

    return attached_per_node


def assert_consistent_balanced_attachments(env: NeonEnv, total_shards):
    attached_per_node = get_consistent_node_shard_counts(env, total_shards)

    min_shard_count = min(attached_per_node.values())
    max_shard_count = max(attached_per_node.values())

    flake_factor = 5 / 100
    assert max_shard_count - min_shard_count <= int(total_shards * flake_factor)


@pytest.mark.timeout(3600)  # super long running test: should go down as we optimize
def test_storage_controller_many_tenants(
    neon_env_builder: NeonEnvBuilder, compute_reconfigure_listener: ComputeReconfigure
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
    neon_env_builder.storage_controller_config = {
        # Default neon_local uses a small timeout: use a longer one to tolerate longer pageserver restarts.
        # TODO: tune this down as restarts get faster (https://github.com/neondatabase/neon/pull/7553), to
        # guard against regressions in restart time.
        "max_offline": "30s",
        "max_warming_up": "300s",
    }
    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )

    # A small sleep on each call into the notify hook, to simulate the latency of doing a database write
    compute_reconfigure_listener.register_on_notify(lambda body: time.sleep(0.01))

    env = neon_env_builder.init_configs()
    neon_env_builder.start()

    # We will intentionally stress reconciler concurrrency, which triggers a warning when lots
    # of shards are hitting the delayed path.
    env.storage_controller.allowed_errors.extend(
        [
            # We will intentionally stress reconciler concurrrency, which triggers a warning when lots
            # of shards are hitting the delayed path.
            ".*Many shards are waiting to reconcile",
            # We will create many timelines concurrently, so they might get slow enough to trip the warning
            # that timeline creation is holding a lock too long.
            ".*Shared lock by TimelineCreate.*was held.*",
        ]
    )

    for ps in env.pageservers:
        # Storage controller is allowed to drop pageserver requests when the cancellation token
        # for a Reconciler fires.
        ps.allowed_errors.append(".*request was dropped before completing.*")

    # Total tenants
    tenant_count = 4000

    # Shards per tenant
    shard_count = 2
    stripe_size = 1024

    total_shards = tenant_count * shard_count

    tenants = set(TenantId.generate() for _i in range(0, tenant_count))

    virtual_ps_http = PageserverHttpClient(env.storage_controller_port, lambda: True)

    def check_memory():
        # Shards should be cheap_ in memory, as we will have very many of them
        expect_memory_per_shard = 128 * 1024

        rss = env.storage_controller.get_metric_value("process_resident_memory_bytes")
        assert rss is not None
        log.info(f"Resident memory: {rss} ({ rss / (shard_count * tenant_count)} per shard)")
        assert rss < expect_memory_per_shard * shard_count * tenant_count

    # We use a fixed seed to make the test somewhat reproducible: we want a randomly
    # chosen order in the sense that it's arbitrary, but not in the sense that it should change every run.
    rng = random.Random(1234)

    # Issue more concurrent operations than the storage controller's reconciler concurrency semaphore
    # permits, to ensure that we are exercising stressing that.
    api_concurrency = 135

    # We will create tenants directly via API, not via neon_local, to avoid any false
    # serialization of operations in neon_local (it e.g. loads/saves a config file on each call)
    with concurrent.futures.ThreadPoolExecutor(max_workers=api_concurrency) as executor:
        futs = []
        t1 = time.time()
        for tenant_id in tenants:
            f = executor.submit(
                env.storage_controller.tenant_create,
                tenant_id,
                shard_count,
                stripe_size,
                # Upload heatmaps fast, so that secondary downloads happen promptly, enabling
                # the controller's optimization migrations to proceed promptly.
                tenant_config={"heatmap_period": "10s"},
                placement_policy={"Attached": 1},
            )
            futs.append(f)

        # Wait for creations to finish
        for f in futs:
            f.result()
        log.info(
            f"Created {len(tenants)} tenants in {time.time() - t1}, {len(tenants) / (time.time() - t1)}/s"
        )

        run_ops = api_concurrency * 4
        assert run_ops < len(tenants)
        op_tenants = list(tenants)[0:run_ops]

        # Generate a mixture of operations and dispatch them all concurrently
        futs = []
        for tenant_id in op_tenants:
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

    # Consistency check is safe here: all the previous operations waited for reconcile before completing
    env.storage_controller.consistency_check()
    check_memory()

    # This loop waits for reconcile_all to indicate no pending work, and then calls it once more to time
    # how long the call takes when idle: this iterates over shards while doing no I/O and should be reliably fast: if
    # it isn't, that's a sign that we have made some algorithmic mistake (e.g. O(N**2) scheduling)
    #
    # We do not require that the system is quiescent already here, although at present in this point in the test
    # that may be the case.
    while True:
        t1 = time.time()
        reconcilers = env.storage_controller.reconcile_all()
        if reconcilers == 0:
            # Time how long a no-op background reconcile takes: this measures how long it takes to
            # loop over all the shards looking for work to do.
            runtime = time.time() - t1
            log.info(f"No-op call to reconcile_all took {runtime}s")
            assert runtime < 1
            break

    # Restart the storage controller
    env.storage_controller.stop()
    env.storage_controller.start()

    # See how long the controller takes to pass its readiness check.  This should be fast because
    # all the nodes are online: offline pageservers are the only thing that's allowed to delay
    # startup.
    readiness_period = env.storage_controller.wait_until_ready()
    assert readiness_period < 5

    # Consistency check is safe here: the storage controller's restart should not have caused any reconcilers
    # to run, as it was in a stable state before restart.  If it did, that's a bug.
    env.storage_controller.consistency_check()
    check_memory()

    shard_counts = get_consistent_node_shard_counts(env, total_shards)
    log.info(f"Shard counts before rolling restart: {shard_counts}")

    assert_consistent_balanced_attachments(env, total_shards)

    # Restart pageservers gracefully: this exercises the /re-attach pageserver API
    # and the storage controller drain and fill API
    for ps in env.pageservers:
        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_drain(ps_id), ps.id, max_attempts=3, backoff=2
        )

        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.PAUSE_FOR_RESTART,
            max_attempts=24,
            backoff=5,
        )

        shard_counts = get_consistent_node_shard_counts(env, total_shards)
        log.info(f"Shard counts after draining node {ps.id}: {shard_counts}")
        # Assert that we've drained the node
        assert shard_counts[str(ps.id)] == 0
        # Assert that those shards actually went somewhere
        assert sum(shard_counts.values()) == total_shards

        ps.restart()
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=24,
            backoff=1,
        )

        env.storage_controller.retryable_node_operation(
            lambda ps_id: env.storage_controller.node_fill(ps_id), ps.id, max_attempts=3, backoff=2
        )
        env.storage_controller.poll_node_status(
            ps.id,
            PageserverAvailability.ACTIVE,
            PageserverSchedulingPolicy.ACTIVE,
            max_attempts=24,
            backoff=5,
        )

        shard_counts = get_consistent_node_shard_counts(env, total_shards)
        log.info(f"Shard counts after filling node {ps.id}: {shard_counts}")

        assert_consistent_balanced_attachments(env, total_shards)

        env.storage_controller.reconcile_until_idle()
        env.storage_controller.consistency_check()

    # Consistency check is safe here: restarting pageservers should not have caused any Reconcilers to spawn,
    # as they were not offline long enough to trigger any scheduling changes.
    env.storage_controller.consistency_check()
    check_memory()

    # Stop the storage controller before tearing down fixtures, because it otherwise might log
    # errors trying to call our `ComputeReconfigure`.
    env.storage_controller.stop()
