import concurrent.futures
import random
from collections import defaultdict

from fixtures.compute_reconfigure import ComputeReconfigure
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
)
from fixtures.types import TenantId, TenantShardId, TimelineId
from fixtures.utils import wait_until
from fixtures.workload import Workload


def get_node_shard_counts(env: NeonEnv, tenant_ids):
    total: defaultdict[int, int] = defaultdict(int)
    attached: defaultdict[int, int] = defaultdict(int)
    for tid in tenant_ids:
        for shard in env.storage_controller.tenant_describe(tid)["shards"]:
            log.info(
                f"{shard['tenant_shard_id']}: attached={shard['node_attached']}, secondary={shard['node_secondary']} "
            )
            for node in shard["node_secondary"]:
                total[int(node)] += 1
            attached[int(shard["node_attached"])] += 1
            total[int(shard["node_attached"])] += 1

    return total, attached


def test_storcon_rolling_failures(
    neon_env_builder: NeonEnvBuilder,
    compute_reconfigure_listener: ComputeReconfigure,
):
    neon_env_builder.num_pageservers = 8

    neon_env_builder.control_plane_compute_hook_api = (
        compute_reconfigure_listener.control_plane_compute_hook_api
    )

    workloads: dict[TenantId, Workload] = {}

    env = neon_env_builder.init_start()

    for ps in env.pageservers:
        # We will do unclean detaches
        ps.allowed_errors.append(".*Dropped remote consistent LSN updates.*")

    n_tenants = 32
    tenants = [(env.initial_tenant, env.initial_timeline)]
    for i in range(0, n_tenants - 1):
        tenant_id = TenantId.generate()
        timeline_id = TimelineId.generate()
        shard_count = [1, 2, 4][i % 3]
        env.neon_cli.create_tenant(
            tenant_id, timeline_id, shard_count=shard_count, placement_policy='{"Double":1}'
        )
        tenants.append((tenant_id, timeline_id))

    # Background pain:
    # - TODO: some fraction of pageserver API requests hang
    #   (this requires implementing wrap of location_conf calls with proper timeline/cancel)
    # - TODO: continuous tenant/timeline creation/destruction over a different ID range than
    #   the ones we're using for availability checks.

    rng = random.Random(0xDEADBEEF)

    for tenant_id, timeline_id in tenants:
        workload = Workload(env, tenant_id, timeline_id)
        compute_reconfigure_listener.register_workload(workload)
        workloads[tenant_id] = workload

    def node_evacuated(node_id: int):
        total, attached = get_node_shard_counts(env, [t[0] for t in tenants])
        assert attached[node_id] == 0

    def attachments_active():
        for tid, _tlid in tenants:
            for shard in env.storage_controller.locate(tid):
                psid = shard["node_id"]
                tsid = TenantShardId.parse(shard["shard_id"])
                status = env.get_pageserver(psid).http_client().tenant_status(tenant_id=tsid)
                assert status["state"]["slug"] == "Active"
                log.info(f"Shard {tsid} active on node {psid}")

    failpoints = ("api-503", "5%1000*return(1)")
    failpoints_str = f"{failpoints[0]}={failpoints[1]}"
    for ps in env.pageservers:
        ps.http_client().configure_failpoints(failpoints)

    def for_all_workloads(callback, timeout=60):
        futs = []
        with concurrent.futures.ThreadPoolExecutor() as pool:
            for _tenant_id, workload in workloads.items():
                futs.append(pool.submit(callback, workload))

            for f in futs:
                f.result(timeout=timeout)

    def clean_fail_restore():
        """
        Clean shutdown of a node: mark it offline in storage controller, wait for new attachment
        locations to activate, then SIGTERM it.
        - Endpoints should not fail any queries
        - New attach locations should activate within bounded time.
        """
        victim = rng.choice(env.pageservers)
        env.storage_controller.node_configure(victim.id, {"availability": "Offline"})

        wait_until(10, 1, lambda node_id=victim.id: node_evacuated(node_id))  # type: ignore[misc]
        wait_until(10, 1, attachments_active)

        victim.stop(immediate=False)

        traffic()

        victim.start(extra_env_vars={"FAILPOINTS": failpoints_str})

        # Revert shards to attach at their original locations
        # TODO
        # env.storage_controller.balance_attached()
        wait_until(10, 1, attachments_active)

    def hard_fail_restore():
        """
        Simulate an unexpected death of a pageserver node
        """
        victim = rng.choice(env.pageservers)
        victim.stop(immediate=True)
        # TODO: once we implement heartbeats detecting node failures, remove this
        # explicit marking offline and rely on storage controller to detect it itself.
        env.storage_controller.node_configure(victim.id, {"availability": "Offline"})
        wait_until(10, 1, lambda node_id=victim.id: node_evacuated(node_id))  # type: ignore[misc]
        wait_until(10, 1, attachments_active)
        traffic()
        victim.start(extra_env_vars={"FAILPOINTS": failpoints_str})
        # TODO
        # env.storage_controller.balance_attached()
        wait_until(10, 1, attachments_active)

    def traffic():
        """
        Check that all tenants are working for postgres clients
        """

        def exercise_one(workload):
            workload.churn_rows(100)
            workload.validate()

        for_all_workloads(exercise_one)

    def init_one(workload):
        workload.init()
        workload.write_rows(100)

    for_all_workloads(init_one, timeout=60)

    for i in range(0, 20):
        mode = rng.choice([0, 1, 2])
        log.info(f"Iteration {i}, mode {mode}")
        if mode == 0:
            # Traffic interval: sometimes, instead of a failure, just let the clients
            # write a load of data.  This avoids chaos tests ending up with unrealistically
            # small quantities of data in flight.
            traffic()
        elif mode == 1:
            clean_fail_restore()
        elif mode == 2:
            hard_fail_restore()

        # Fail and restart: hard-kill one node. Notify the storage controller that it is offline.
        # Success criteria:
        # - New attach locations should activate within bounded time
        # - TODO: once we do heartbeating, we should not have to explicitly mark the node offline

        # TODO: fail and remove: fail a node, and remove it from the cluster.
        # Success criteria:
        # - Endpoints should not fail any queries
        # - New attach locations should activate within bounded time
        # - New secondary locations should fill up with data within bounded time

        # TODO: somehow need to wait for reconciles to complete before doing consistency check
        # (or make the check wait).

        # Do consistency check on every iteration, not just at the end: this makes it more obvious
        # which change caused an issue.
        env.storage_controller.consistency_check()
