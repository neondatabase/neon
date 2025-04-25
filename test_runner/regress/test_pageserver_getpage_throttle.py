from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

import pytest
from anyio import Path
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.pg_version import PgVersion
from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder, PgBin


def test_pageserver_getpage_throttle(neon_env_builder: NeonEnvBuilder, pg_bin: PgBin):
    env = neon_env_builder.init_start()

    env.pageserver.tenant_detach(env.initial_tenant)

    env.pageserver.allowed_errors.append(
        # https://github.com/neondatabase/neon/issues/6925
        r".*query handler for.*pagestream.*failed: unexpected message: CopyFail during COPY.*"
    )

    tenant_id = TenantId.generate()
    timeline_id = TimelineId.generate()

    rate_limit_rps = 100
    compaction_period = 5
    env.pageserver.tenant_create(
        tenant_id,
        conf={
            "compaction_period": f"{compaction_period}s",
            "timeline_get_throttle": {
                "task_kinds": [
                    "PageRequestHandler"
                ],  # any non-empty array will do here https://github.com/neondatabase/neon/pull/9962
                "initial": 0,
                "refill_interval": "100ms",
                "refill_amount": int(rate_limit_rps / 10),
                "max": int(rate_limit_rps / 10),
            },
        },
    )

    ps_http = env.pageserver.http_client()

    ps_http.timeline_create(PgVersion.V16, tenant_id, timeline_id)

    def run_pagebench_at_max_speed_and_get_total_requests_completed(duration_secs: int):
        cmd = [
            str(env.neon_binpath / "pagebench"),
            "get-page-latest-lsn",
            "--mgmt-api-endpoint",
            ps_http.base_url,
            "--page-service-connstring",
            env.pageserver.connstr(password=None),
            "--runtime",
            f"{duration_secs}s",
            f"{tenant_id}/{timeline_id}",
        ]

        basepath = pg_bin.run_capture(cmd, with_command_header=False)
        results_path = Path(basepath + ".stdout")
        log.info(f"Benchmark results at: {results_path}")

        with open(results_path) as f:
            results = json.load(f)
        log.info(f"Results:\n{json.dumps(results, sort_keys=True, indent=2)}")
        return int(results["total"]["request_count"])

    log.info("warmup / make sure metrics are present")
    run_pagebench_at_max_speed_and_get_total_requests_completed(2)
    smgr_metrics_query = {
        "tenant_id": str(tenant_id),
        "timeline_id": str(timeline_id),
        "smgr_query_type": "get_page_at_lsn",
    }
    smgr_metric_name = "pageserver_smgr_query_seconds_sum"
    throttle_metrics_query = {
        "tenant_id": str(tenant_id),
    }
    throttle_metric_name = "pageserver_tenant_throttling_wait_usecs_sum_total"

    smgr_query_seconds_pre = ps_http.get_metric_value(smgr_metric_name, smgr_metrics_query)
    assert smgr_query_seconds_pre is not None
    throttled_usecs_pre = ps_http.get_metric_value(throttle_metric_name, throttle_metrics_query)
    assert throttled_usecs_pre is not None

    marker = uuid.uuid4().hex
    ps_http.post_tracing_event("info", marker)
    _, marker_offset = wait_until(lambda: env.pageserver.assert_log_contains(marker, offset=None))

    log.info("run pagebench")
    duration_secs = 20
    actual_ncompleted = run_pagebench_at_max_speed_and_get_total_requests_completed(duration_secs)

    log.info("validate the client is capped at the configured rps limit")
    expect_ncompleted = duration_secs * rate_limit_rps
    assert pytest.approx(expect_ncompleted, 0.05) == actual_ncompleted, (
        "the throttling deviates more than 5percent from the expectation"
    )

    log.info("validate that we logged the throttling")

    wait_until(
        lambda: env.pageserver.assert_log_contains(
            f".*{tenant_id}.*shard was throttled in the last n_seconds.*",
            offset=marker_offset,
        ),
        timeout=compaction_period,
    )

    log.info("validate the metrics")
    smgr_query_seconds_post = ps_http.get_metric_value(smgr_metric_name, smgr_metrics_query)
    assert smgr_query_seconds_post is not None
    throttled_usecs_post = ps_http.get_metric_value(throttle_metric_name, throttle_metrics_query)
    assert throttled_usecs_post is not None
    actual_smgr_query_seconds = smgr_query_seconds_post - smgr_query_seconds_pre
    actual_throttled_usecs = throttled_usecs_post - throttled_usecs_pre
    actual_throttled_secs = actual_throttled_usecs / 1_000_000

    assert pytest.approx(actual_throttled_secs + actual_smgr_query_seconds, 0.1) == duration_secs, (
        "throttling and processing latency = total request time; this assert validates thi holds on average"
    )

    # without this assertion, the test would pass even if the throttling was completely broken
    # but the request processing is so slow that it makes up for the latency that a correct throttling
    # implementation would add
    assert actual_smgr_query_seconds < 0.66 * duration_secs, (
        "test self-test: request processing is consuming most of the wall clock time; this risks that we're not actually testing throttling"
    )
