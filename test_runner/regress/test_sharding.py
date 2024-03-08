import os
from typing import Optional

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    AttachmentServiceApiException,
    NeonEnv,
    NeonEnvBuilder,
    tenant_get_shards,
)
from fixtures.remote_storage import s3_storage
from fixtures.types import Lsn, TenantShardId, TimelineId
from fixtures.utils import wait_until
from fixtures.workload import Workload


def test_sharding_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basic lifecycle of a sharded tenant:
     - ingested data gets split up
     - page service reads
     - timeline creation and deletion
     - splits
    """

    shard_count = 4
    neon_env_builder.num_pageservers = shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()

    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant

    pageservers = dict((int(p.id), p) for p in env.pageservers)
    shards = env.attachment_service.locate(tenant_id)

    def get_sizes():
        sizes = {}
        for shard in shards:
            node_id = int(shard["node_id"])
            pageserver = pageservers[node_id]
            sizes[node_id] = pageserver.http_client().tenant_status(shard["shard_id"])[
                "current_physical_size"
            ]
        log.info(f"sizes = {sizes}")
        return sizes

    # Test that timeline creation works on a sharded tenant
    timeline_b = env.neon_cli.create_branch("branch_b", tenant_id=tenant_id)

    # Test that we can write data to a sharded tenant
    workload = Workload(env, tenant_id, timeline_b, branch_name="branch_b")
    workload.init()

    sizes_before = get_sizes()
    workload.write_rows(256)

    # Test that we can read data back from a sharded tenant
    workload.validate()

    # Validate that the data is spread across pageservers
    sizes_after = get_sizes()
    # Our sizes increased when we wrote data
    assert sum(sizes_after.values()) > sum(sizes_before.values())
    # That increase is present on all shards
    assert all(sizes_after[ps.id] > sizes_before[ps.id] for ps in env.pageservers)

    # Validate that timeline list API works properly on all shards
    for shard in shards:
        node_id = int(shard["node_id"])
        pageserver = pageservers[node_id]
        timelines = set(
            TimelineId(tl["timeline_id"])
            for tl in pageserver.http_client().timeline_list(shard["shard_id"])
        )
        assert timelines == {env.initial_timeline, timeline_b}

    env.attachment_service.consistency_check()


def test_sharding_split_unsharded(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that shard splitting works on a tenant created as unsharded (i.e. with
    ShardCount(0)).
    """
    env = neon_env_builder.init_start()
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Check that we created with an unsharded TenantShardId: this is the default,
    # but check it in case we change the default in future
    assert env.attachment_service.inspect(TenantShardId(tenant_id, 0, 0)) is not None

    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()
    workload.write_rows(256)
    workload.validate()

    # Split one shard into two
    env.attachment_service.tenant_shard_split(tenant_id, shard_count=2)

    # Check we got the shard IDs we expected
    assert env.attachment_service.inspect(TenantShardId(tenant_id, 0, 2)) is not None
    assert env.attachment_service.inspect(TenantShardId(tenant_id, 1, 2)) is not None

    workload.validate()

    env.attachment_service.consistency_check()


def test_sharding_split_smoke(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test the basics of shard splitting:
    - The API results in more shards than we started with
    - The tenant's data remains readable

    """

    # We will start with 4 shards and split into 8, then migrate all those
    # 8 shards onto separate pageservers
    shard_count = 4
    split_shard_count = 8
    neon_env_builder.num_pageservers = split_shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.  The stripe size is given
    # in number of 8KiB pages.
    stripe_size = 128

    # Use S3-compatible remote storage so that we can scrub: this test validates
    # that the scrubber doesn't barf when it sees a sharded tenant.
    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()

    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    workload = Workload(env, tenant_id, timeline_id, branch_name="main")
    workload.init()

    # Initial data
    workload.write_rows(256)

    # Note which pageservers initially hold a shard after tenant creation
    pre_split_pageserver_ids = [loc["node_id"] for loc in env.attachment_service.locate(tenant_id)]

    # For pageservers holding a shard, validate their ingest statistics
    # reflect a proper splitting of the WAL.
    for pageserver in env.pageservers:
        if pageserver.id not in pre_split_pageserver_ids:
            continue

        metrics = pageserver.http_client().get_metrics_values(
            [
                "pageserver_wal_ingest_records_received_total",
                "pageserver_wal_ingest_records_committed_total",
                "pageserver_wal_ingest_records_filtered_total",
            ]
        )

        log.info(f"Pageserver {pageserver.id} metrics: {metrics}")

        # Not everything received was committed
        assert (
            metrics["pageserver_wal_ingest_records_received_total"]
            > metrics["pageserver_wal_ingest_records_committed_total"]
        )

        # Something was committed
        assert metrics["pageserver_wal_ingest_records_committed_total"] > 0

        # Counts are self consistent
        assert (
            metrics["pageserver_wal_ingest_records_received_total"]
            == metrics["pageserver_wal_ingest_records_committed_total"]
            + metrics["pageserver_wal_ingest_records_filtered_total"]
        )

    # TODO: validate that shards have different sizes

    workload.validate()

    assert len(pre_split_pageserver_ids) == 4

    def shards_on_disk(shard_ids):
        for pageserver in env.pageservers:
            for shard_id in shard_ids:
                if pageserver.tenant_dir(shard_id).exists():
                    return True

        return False

    old_shard_ids = [TenantShardId(tenant_id, i, shard_count) for i in range(0, shard_count)]
    # Before split, old shards exist
    assert shards_on_disk(old_shard_ids)

    env.attachment_service.tenant_shard_split(tenant_id, shard_count=split_shard_count)

    post_split_pageserver_ids = [loc["node_id"] for loc in env.attachment_service.locate(tenant_id)]
    # We should have split into 8 shards, on the same 4 pageservers we started on.
    assert len(post_split_pageserver_ids) == split_shard_count
    assert len(set(post_split_pageserver_ids)) == shard_count
    assert set(post_split_pageserver_ids) == set(pre_split_pageserver_ids)

    # The old parent shards should no longer exist on disk
    assert not shards_on_disk(old_shard_ids)

    workload.validate()

    workload.churn_rows(256)

    workload.validate()

    # Run GC on all new shards, to check they don't barf or delete anything that breaks reads
    # (compaction was already run as part of churn_rows)
    all_shards = tenant_get_shards(env, tenant_id)
    for tenant_shard_id, pageserver in all_shards:
        pageserver.http_client().timeline_gc(tenant_shard_id, timeline_id, None)
    workload.validate()

    migrate_to_pageserver_ids = list(
        set(p.id for p in env.pageservers) - set(pre_split_pageserver_ids)
    )
    assert len(migrate_to_pageserver_ids) == split_shard_count - shard_count

    # Migrate shards away from the node where the split happened
    for ps_id in pre_split_pageserver_ids:
        shards_here = [
            tenant_shard_id
            for (tenant_shard_id, pageserver) in all_shards
            if pageserver.id == ps_id
        ]
        assert len(shards_here) == 2
        migrate_shard = shards_here[0]
        destination = migrate_to_pageserver_ids.pop()

        log.info(f"Migrating shard {migrate_shard} from {ps_id} to {destination}")
        env.neon_cli.tenant_migrate(migrate_shard, destination, timeout_secs=10)

    workload.validate()

    # Check that we didn't do any spurious reconciliations.
    # Total number of reconciles should have been one per original shard, plus
    # one for each shard that was migrated.
    reconcile_ok = env.attachment_service.get_metric_value(
        "storage_controller_reconcile_complete_total", filter={"status": "ok"}
    )
    assert reconcile_ok == shard_count + split_shard_count // 2

    # Check that no cancelled or errored reconciliations occurred: this test does no
    # failure injection and should run clean.
    assert (
        env.attachment_service.get_metric_value(
            "storage_controller_reconcile_complete_total", filter={"status": "cancel"}
        )
        is None
    )
    assert (
        env.attachment_service.get_metric_value(
            "storage_controller_reconcile_complete_total", filter={"status": "error"}
        )
        is None
    )

    env.attachment_service.consistency_check()

    # Validate pageserver state
    shards_exist: list[TenantShardId] = []
    for pageserver in env.pageservers:
        locations = pageserver.http_client().tenant_list_locations()
        shards_exist.extend(TenantShardId.parse(s[0]) for s in locations["tenant_shards"])

    log.info("Shards after split: {shards_exist}")
    assert len(shards_exist) == split_shard_count

    # Ensure post-split pageserver locations survive a restart (i.e. the child shards
    # correctly wrote config to disk, and the storage controller responds correctly
    # to /re-attach)
    for pageserver in env.pageservers:
        pageserver.stop()
        pageserver.start()

    shards_exist = []
    for pageserver in env.pageservers:
        locations = pageserver.http_client().tenant_list_locations()
        shards_exist.extend(TenantShardId.parse(s[0]) for s in locations["tenant_shards"])

    log.info("Shards after restart: {shards_exist}")
    assert len(shards_exist) == split_shard_count

    workload.validate()


@pytest.mark.skipif(
    # The quantity of data isn't huge, but debug can be _very_ slow, and the things we're
    # validating in this test don't benefit much from debug assertions.
    os.getenv("BUILD_TYPE") == "debug",
    reason="Avoid running bulkier ingest tests in debug mode",
)
def test_sharding_ingest(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Check behaviors related to ingest:
    - That we generate properly sized layers
    - TODO: that updates to remote_consistent_lsn are made correctly via safekeepers
    """

    # Set a small stripe size and checkpoint distance, so that we can exercise rolling logic
    # without writing a lot of data.
    expect_layer_size = 131072
    TENANT_CONF = {
        # small checkpointing and compaction targets to ensure we generate many upload operations
        "checkpoint_distance": f"{expect_layer_size}",
        "compaction_target_size": f"{expect_layer_size}",
    }
    shard_count = 4
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start(
        initial_tenant_conf=TENANT_CONF,
        initial_tenant_shard_count=shard_count,
        # A stripe size the same order of magnitude as layer size: this ensures that
        # within checkpoint_distance some shards will have no data to ingest, if LSN
        # contains sequential page writes.  This test checks that this kind of
        # scenario doesn't result in some shards emitting empty/tiny layers.
        initial_tenant_shard_stripe_size=expect_layer_size // 8192,
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.write_rows(4096, upload=False)
    workload.validate()

    small_layer_count = 0
    ok_layer_count = 0
    huge_layer_count = 0

    # Inspect the resulting layer map, count how many layers are undersized.
    for shard in env.attachment_service.locate(tenant_id):
        pageserver = env.get_pageserver(shard["node_id"])
        shard_id = shard["shard_id"]
        layer_map = pageserver.http_client().layer_map_info(shard_id, timeline_id)

        for layer in layer_map.historic_layers:
            assert layer.layer_file_size is not None
            if layer.layer_file_size < expect_layer_size // 2:
                classification = "Small"
                small_layer_count += 1
            elif layer.layer_file_size > expect_layer_size * 2:
                classification = "Huge "
                huge_layer_count += 1
            else:
                classification = "OK   "
                ok_layer_count += 1

            if layer.kind == "Delta":
                assert layer.lsn_end is not None
                lsn_size = Lsn(layer.lsn_end) - Lsn(layer.lsn_start)
            else:
                lsn_size = 0

            log.info(
                f"{classification} layer[{pageserver.id}]: {layer.layer_file_name} (size {layer.layer_file_size}, LSN distance {lsn_size})"
            )

    # Why an inexact check?
    # - Because we roll layers on checkpoint_distance * shard_count, we expect to obey the target
    #   layer size on average, but it is still possible to write some tiny layers.
    log.info(f"Totals: {small_layer_count} small layers, {ok_layer_count} ok layers")
    if small_layer_count <= shard_count:
        # If each shard has <= 1 small layer
        pass
    else:
        # General case:
        assert float(small_layer_count) / float(ok_layer_count) < 0.25

    # Each shard may emit up to one huge layer, because initdb ingest doesn't respect checkpoint_distance.
    assert huge_layer_count <= shard_count


class Failure:
    pageserver_id: Optional[int]

    def apply(self, env: NeonEnv):
        raise NotImplementedError()

    def clear(self, env: NeonEnv):
        """
        Clear the failure, in a way that should enable the system to proceed
        to a totally clean state (all nodes online and reconciled)
        """
        raise NotImplementedError()

    def expect_available(self):
        raise NotImplementedError()

    def can_mitigate(self):
        """Whether Self.mitigate is available for use"""
        return False

    def mitigate(self, env: NeonEnv):
        """
        Mitigate the failure in a way that should allow shard split to
        complete and service to resume, but does not guarantee to leave
        the whole world in a clean state (e.g. an Offline node might have
        junk LocationConfigs on it)
        """
        raise NotImplementedError()

    def fails_forward(self):
        """
        If true, this failure results in a state that eventualy completes the split.
        """
        return False


class PageserverFailpoint(Failure):
    def __init__(self, failpoint, pageserver_id, mitigate):
        self.failpoint = failpoint
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.allowed_errors.extend(
            [".*failpoint.*", ".*Resetting.*after shard split failure.*"]
        )
        pageserver.http_client().configure_failpoints((self.failpoint, "return(1)"))

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.http_client().configure_failpoints((self.failpoint, "off"))
        if self._mitigate:
            env.attachment_service.node_configure(self.pageserver_id, {"availability": "Active"})

    def expect_available(self):
        return True

    def can_mitigate(self):
        return self._mitigate

    def mitigate(self, env):
        env.attachment_service.node_configure(self.pageserver_id, {"availability": "Offline"})


class StorageControllerFailpoint(Failure):
    def __init__(self, failpoint):
        self.failpoint = failpoint
        self.pageserver_id = None

    def apply(self, env: NeonEnv):
        env.attachment_service.configure_failpoints((self.failpoint, "return(1)"))

    def clear(self, env: NeonEnv):
        env.attachment_service.configure_failpoints((self.failpoint, "off"))

    def expect_available(self):
        return True

    def can_mitigate(self):
        return False

    def fails_forward(self):
        # Edge case: the very last failpoint that simulates a DB connection error, where
        # the abort path will fail-forward and result in a complete split.
        return self.failpoint == "shard-split-post-complete"


class NodeKill(Failure):
    def __init__(self, pageserver_id, mitigate):
        self.pageserver_id = pageserver_id
        self._mitigate = mitigate

    def apply(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.stop(immediate=True)

    def clear(self, env: NeonEnv):
        pageserver = env.get_pageserver(self.pageserver_id)
        pageserver.start()

    def expect_available(self):
        return False

    def mitigate(self, env):
        env.attachment_service.node_configure(self.pageserver_id, {"availability": "Offline"})


@pytest.mark.parametrize(
    "failure",
    [
        PageserverFailpoint("api-500", 1, False),
        NodeKill(1, False),
        PageserverFailpoint("api-500", 1, True),
        NodeKill(1, True),
        PageserverFailpoint("shard-split-pre-prepare", 1, False),
        PageserverFailpoint("shard-split-post-prepare", 1, False),
        PageserverFailpoint("shard-split-pre-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-hardlink", 1, False),
        PageserverFailpoint("shard-split-post-child-conf", 1, False),
        PageserverFailpoint("shard-split-lsn-wait", 1, False),
        PageserverFailpoint("shard-split-pre-finish", 1, False),
        StorageControllerFailpoint("shard-split-validation"),
        StorageControllerFailpoint("shard-split-post-begin"),
        StorageControllerFailpoint("shard-split-post-remote"),
        StorageControllerFailpoint("shard-split-post-complete"),
    ],
)
def test_sharding_split_failures(neon_env_builder: NeonEnvBuilder, failure: Failure):
    neon_env_builder.num_pageservers = 4
    initial_shard_count = 2
    split_shard_count = 4

    env = neon_env_builder.init_start(initial_tenant_shard_count=initial_shard_count)
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    # Make sure the node we're failing has a shard on it, otherwise the test isn't testing anything
    assert (
        failure.pageserver_id is None
        or len(
            env.get_pageserver(failure.pageserver_id)
            .http_client()
            .tenant_list_locations()["tenant_shards"]
        )
        > 0
    )

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(100)

    # Set one pageserver to 500 all requests, then do a split
    # TODO: also test with a long-blocking failure: controller should time out its request and then
    # clean up in a well defined way.
    failure.apply(env)

    with pytest.raises(AttachmentServiceApiException):
        env.attachment_service.tenant_shard_split(tenant_id, shard_count=4)

    # We expect that the overall operation will fail, but some split requests
    # will have succeeded: the net result should be to return to a clean state, including
    # detaching any child shards.
    def assert_rolled_back(exclude_ps_id=None) -> None:
        count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id}")
                assert tenant_shard_id.shard_count == initial_shard_count
                count += 1
        assert count == initial_shard_count

    def assert_split_done(exclude_ps_id=None) -> None:
        count = 0
        for ps in env.pageservers:
            if exclude_ps_id is not None and ps.id == exclude_ps_id:
                continue

            locations = ps.http_client().tenant_list_locations()["tenant_shards"]
            for loc in locations:
                tenant_shard_id = TenantShardId.parse(loc[0])
                log.info(f"Shard {tenant_shard_id} seen on node {ps.id}")
                assert tenant_shard_id.shard_count == split_shard_count
                count += 1
        assert count == split_shard_count

    def finish_split():
        # Having failed+rolled back, we should be able to split again
        # No failures this time; it will succeed
        env.attachment_service.tenant_shard_split(tenant_id, shard_count=split_shard_count)

        workload.churn_rows(10)
        workload.validate()

    if failure.expect_available():
        # Even though the split failed partway through, this should not have interrupted
        # clients.  Disable waiting for pageservers in the workload helper, because our
        # failpoints may prevent API access.
        # This only applies for failure modes that leave pageserver page_service API available.
        workload.churn_rows(10, upload=False, ingest=False)
        workload.validate()

    if failure.fails_forward():
        # A failure type which results in eventual completion of the split
        wait_until(30, 1, assert_split_done)
    elif failure.can_mitigate():
        # Mitigation phase: we expect to be able to proceed with a successful shard split
        failure.mitigate(env)

        # The split should appear to be rolled back from the point of view of all pageservers
        # apart from the one that is offline
        wait_until(30, 1, lambda: assert_rolled_back(exclude_ps_id=failure.pageserver_id))

        finish_split()
        wait_until(30, 1, lambda: assert_split_done(exclude_ps_id=failure.pageserver_id))

        # Having cleared the failure, everything should converge to a pristine state
        failure.clear(env)
        wait_until(30, 1, assert_split_done)
    else:
        # Once we restore the faulty pageserver's API to good health, rollback should
        # eventually complete.
        failure.clear(env)

        wait_until(30, 1, assert_rolled_back)

        # Having rolled back, the tenant should be working
        workload.churn_rows(10)
        workload.validate()

        # Splitting again should work, since we cleared the failure
        finish_split()
        assert_split_done()

    env.attachment_service.consistency_check()
