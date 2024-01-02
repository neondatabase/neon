from fixtures.neon_fixtures import (
    NeonEnvBuilder,
)
from fixtures.remote_storage import s3_storage


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
    # writing large quantities of data in this test.
    stripe_size = 128

    neon_env_builder.enable_pageserver_remote_storage(s3_storage())
    neon_env_builder.enable_scrub_on_exit()
    neon_env_builder.preserve_database_files = True

    env = neon_env_builder.init_start(
        initial_tenant_shard_count=shard_count, initial_tenant_shard_stripe_size=stripe_size
    )

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
