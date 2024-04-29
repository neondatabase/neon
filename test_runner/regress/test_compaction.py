import os

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.workload import Workload

AGGRESIVE_COMPACTION_TENANT_CONF = {
    # Disable gc and compaction. The test runs compaction manually.
    "gc_period": "0s",
    "compaction_period": "0s",
    # Small checkpoint distance to create many layers
    "checkpoint_distance": 1024**2,
    # Compact small layers
    "compaction_target_size": 1024**2,
    "image_creation_threshold": 2,
}


@pytest.mark.skipif(os.environ.get("BUILD_TYPE") == "debug", reason="only run with release build")
def test_pageserver_compaction_smoke(neon_env_builder: NeonEnvBuilder):
    """
    This is a smoke test that compaction kicks in. The workload repeatedly churns
    a small number of rows and manually instructs the pageserver to run compaction
    between iterations. At the end of the test validate that the average number of
    layers visited to gather reconstruct data for a given key is within the empirically
    observed bounds.
    """

    # Effectively disable the page cache to rely only on image layers
    # to shorten reads.
    neon_env_builder.pageserver_config_override = """
page_cache_size=10
"""

    env = neon_env_builder.init_start(initial_tenant_conf=AGGRESIVE_COMPACTION_TENANT_CONF)

    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline

    row_count = 10000
    churn_rounds = 100

    ps_http = env.pageserver.http_client()

    workload = Workload(env, tenant_id, timeline_id)
    workload.init(env.pageserver.id)

    log.info("Writing initial data ...")
    workload.write_rows(row_count, env.pageserver.id)

    for i in range(1, churn_rounds + 1):
        if i % 10 == 0:
            log.info(f"Running churn round {i}/{churn_rounds} ...")

        workload.churn_rows(row_count, env.pageserver.id)
        ps_http.timeline_compact(tenant_id, timeline_id)

    log.info("Validating at workload end ...")
    workload.validate(env.pageserver.id)

    log.info("Checking layer access metrics ...")

    layer_access_metric_names = [
        "pageserver_layers_visited_per_read_global_sum",
        "pageserver_layers_visited_per_read_global_count",
        "pageserver_layers_visited_per_read_global_bucket",
        "pageserver_layers_visited_per_vectored_read_global_sum",
        "pageserver_layers_visited_per_vectored_read_global_count",
        "pageserver_layers_visited_per_vectored_read_global_bucket",
    ]

    metrics = env.pageserver.http_client().get_metrics()
    for name in layer_access_metric_names:
        layer_access_metrics = metrics.query_all(name)
        log.info(f"Got metrics: {layer_access_metrics}")

    non_vectored_sum = metrics.query_one("pageserver_layers_visited_per_read_global_sum")
    non_vectored_count = metrics.query_one("pageserver_layers_visited_per_read_global_count")
    non_vectored_average = non_vectored_sum.value / non_vectored_count.value

    vectored_sum = metrics.query_one("pageserver_layers_visited_per_vectored_read_global_sum")
    vectored_count = metrics.query_one("pageserver_layers_visited_per_vectored_read_global_count")
    vectored_average = vectored_sum.value / vectored_count.value

    log.info(f"{non_vectored_average=} {vectored_average=}")

    # The upper bound for average number of layer visits below (8)
    # was chosen empirically for this workload.
    assert non_vectored_average < 8
    assert vectored_average < 8
