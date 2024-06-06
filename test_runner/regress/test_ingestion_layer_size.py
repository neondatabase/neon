import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, wait_for_last_flush_lsn
from fixtures.utils import human_bytes


def test_ingesting_large_batches_of_images(neon_env_builder: NeonEnvBuilder, build_type: str):
    """
    Build a non-small GIN index which includes similarly batched up images in WAL stream as does pgvector
    to show that we no longer create oversized layers.
    """

    if build_type == "debug":
        pytest.skip("debug run is unnecessarily slow")

    checkpoint_distance = 64 * 1024**2
    env = neon_env_builder.init_start(
        initial_tenant_conf={
            "checkpoint_distance": f"{checkpoint_distance}",
            "compaction_period": "0s",
            "gc_period": "0s",
            "compaction_threshold": "255",
            "image_creation_threshold": "99999",
        }
    )

    # build a 391MB gin index which exhibits the same behaviour as the pgvector with the two phase build
    with env.endpoints.create_start("main") as ep, ep.cursor() as cur:
        cur.execute(
            "create table int_array_test as select array_agg(g) as int_array from generate_series(1, 5000000) g group by g / 10;"
        )
        cur.execute("create index on int_array_test using gin (int_array);")
        wait_for_last_flush_lsn(env, ep, env.initial_tenant, env.initial_timeline)

    ps_http = env.pageserver.http_client()
    ps_http.timeline_checkpoint(env.initial_tenant, env.initial_timeline)

    infos = ps_http.layer_map_info(env.initial_tenant, env.initial_timeline)
    assert len(infos.in_memory_layers) == 0, "should had flushed open layers"

    buckets = list(enumerate([0, 20 * 1024**2, checkpoint_distance * 0.9, 2 * checkpoint_distance]))

    counts = [0 for _ in buckets]
    sums = [0 for _ in buckets]

    for layer in infos.historic_layers:
        found = False
        for index, min_size in reversed(buckets):
            if layer.layer_file_size >= min_size:
                counts[index] += 1
                sums[index] += layer.layer_file_size
                found = True
                break
        assert found

        log.info(
            f"{layer.layer_file_name} {human_bytes(layer.layer_file_size)} ({layer.layer_file_size} bytes)"
        )

    for index, min_size in buckets:
        log.info(
            f">= {human_bytes(min_size)}: {counts[index]} layers total {human_bytes(sums[index])}"
        )

    assert (
        counts[3] == 0
    ), f"there should be no layers larger than 2*checkpoint_distance ({human_bytes(2*checkpoint_distance)})"
    assert counts[1] == 1, "expect one smaller layer for initdb"
    assert counts[0] <= 1, "expect at most one tiny layer from shutting down the endpoint"
