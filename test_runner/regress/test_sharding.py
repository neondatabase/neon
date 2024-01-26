from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.remote_storage import s3_storage
from fixtures.types import TimelineId
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

    # TODO: test timeline deletion and tenant deletion (depends on change in attachment_service)
