from fixtures.neon_fixtures import NeonEnvBuilder
import pytest

@pytest.mark.parametrize("shard_count", [2])
def test_sharding(neon_env_builder: NeonEnvBuilder, shard_count: int):
    neon_env_builder.enable_generations = True
    neon_env_builder.initial_shard_count = shard_count
    neon_env_builder.num_pageservers = shard_count
    env = neon_env_builder.init_start()

    for pageserver in env.pageservers:
        # FIXME: attachment_service is not yet sharding aware, so generation validation is broken.
        pageserver.allowed_errors.append(".*Dropped remote consistent LSN updates.*")
        pageserver.allowed_errors.append(".*Dropping stale deletions for tenant.*")

    endpoint = env.endpoints.create_start("main", pageserver_ids=[p.id for p in env.pageservers])
    endpoint.safe_psql("create table t(t bigint, payload text default repeat('?',200))")
    endpoint.safe_psql("insert into t values(generate_series(1,10000000))")
    assert endpoint.safe_psql("select count(*) from t")[0][0] == 10000000
