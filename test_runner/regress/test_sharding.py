from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    tenant_get_shards,
)
from fixtures.remote_storage import s3_storage
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

    # We will start with 4 shards and split into 8, then migrate all those
    # 8 shards onto separate pageservers
    shard_count = 4
    split_shard_count = 8
    neon_env_builder.num_pageservers = split_shard_count

    # 1MiB stripes: enable getting some meaningful data distribution without
    # writing large quantities of data in this test.
    stripe_size = 128

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()
    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    for ps in env.pageservers:
        ps.allowed_errors.extend(
            [
                # FIXME: during a split, control plane should respond affirmatively to validation requests
                # that refer to a shard that no longer exists, but has a child shard.
                ".*Dropped remote consistent LSN updates.*",
                # FIXME: improve logging in the pageserver so that this isn't considered an erorr, or
                # figure out how to make the migration even more seamless.
                ".*Tenant.*is not active.*",
            ]
        )

    # TODO: do some timeline creations & deletions on the sharded tenant
    # TODO: validate that timeline APIs show the created timelines on all shards

    workload = Workload(env, tenant_id, timeline_id)
    workload.init()
    workload.write_rows(1000)

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

    env.attachment_service.tenant_shard_split(tenant_id, shard_count=split_shard_count)

    post_split_pageserver_ids = [loc["node_id"] for loc in env.attachment_service.locate(tenant_id)]
    # We should have split into 8 shards, on the same 4 pageservers we started on.
    assert len(post_split_pageserver_ids) == split_shard_count
    assert len(set(post_split_pageserver_ids)) == shard_count
    assert set(post_split_pageserver_ids) == set(pre_split_pageserver_ids)

    workload.validate()

    workload.churn_rows(1000)

    workload.validate()

    # Run GC on all new shards, to check they don't barf or delete anything that breaks reads
    # (compaction was already run as part of churn_rows)
    all_shards = tenant_get_shards(env, tenant_id)
    for tenant_shard_id, pageserver in all_shards:
        pageserver.http_client().timeline_gc(tenant_shard_id, timeline_id, None)

    # Restart all nodes, to check that the newly created shards are durable
    for ps in env.pageservers:
        ps.restart()

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
