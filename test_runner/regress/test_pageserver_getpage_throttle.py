from __future__ import annotations

import copy
import json
import uuid

import pytest
from anyio import Path
from fixtures.common_types import TenantId, TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin
from fixtures.pg_version import PgVersion
from fixtures.utils import wait_until


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
                "fair": True,
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
    duration_secs = 10
    actual_ncompleted = run_pagebench_at_max_speed_and_get_total_requests_completed(duration_secs)

    log.info("validate the client is capped at the configured rps limit")
    expect_ncompleted = duration_secs * rate_limit_rps
    delta_abs = abs(expect_ncompleted - actual_ncompleted)
    threshold = 0.05 * expect_ncompleted
    assert (
        threshold / rate_limit_rps < 0.1 * duration_secs
    ), "test self-test: unrealistic expecations regarding precision in this test"
    assert (
        delta_abs < 0.05 * expect_ncompleted
    ), "the throttling deviates more than 5percent from the expectation"

    log.info("validate that we logged the throttling")

    wait_until(
        lambda: env.pageserver.assert_log_contains(
            f".*{tenant_id}.*shard was throttled in the last n_seconds.*",
            offset=marker_offset,
        ),
        timeout=compaction_period,
    )

    smgr_query_seconds_post = ps_http.get_metric_value(smgr_metric_name, smgr_metrics_query)
    assert smgr_query_seconds_post is not None
    throttled_usecs_post = ps_http.get_metric_value(throttle_metric_name, throttle_metrics_query)
    assert throttled_usecs_post is not None
    actual_smgr_query_seconds = smgr_query_seconds_post - smgr_query_seconds_pre
    actual_throttled_usecs = throttled_usecs_post - throttled_usecs_pre
    actual_throttled_secs = actual_throttled_usecs / 1_000_000

    log.info("validate that the metric doesn't include throttle wait time")
    assert (
        duration_secs >= 10 * actual_smgr_query_seconds
    ), "smgr metrics should not include throttle wait time"

    log.info("validate that the throttling wait time metrics is correct")
    assert (
        pytest.approx(actual_throttled_secs + actual_smgr_query_seconds, 0.1) == duration_secs
    ), "most of the time in this test is spent throttled because the rate-limit's contribution to latency dominates"


throttle_config_with_field_fair_set = {
    "task_kinds": ["PageRequestHandler"],
    "fair": True,
    "initial": 27,
    "refill_interval": "43s",
    "refill_amount": 23,
    "max": 42,
}


def assert_throttle_config_with_field_fair_set(conf):
    """
    Field `fair` is ignored, so, responses don't contain it
    """
    without_fair = copy.deepcopy(throttle_config_with_field_fair_set)
    without_fair.pop("fair")

    assert conf == without_fair


def test_throttle_fair_config_is_settable_but_ignored_in_mgmt_api(neon_env_builder: NeonEnvBuilder):
    """
    To be removed after https://github.com/neondatabase/neon/pull/8539 is rolled out.
    """
    env = neon_env_builder.init_start()
    vps_http = env.storage_controller.pageserver_api()
    # with_fair config should still be settable
    vps_http.set_tenant_config(
        env.initial_tenant,
        {"timeline_get_throttle": throttle_config_with_field_fair_set},
    )
    conf = vps_http.tenant_config(env.initial_tenant)
    assert_throttle_config_with_field_fair_set(conf.effective_config["timeline_get_throttle"])
    assert_throttle_config_with_field_fair_set(
        conf.tenant_specific_overrides["timeline_get_throttle"]
    )


def test_throttle_fair_config_is_settable_but_ignored_in_config_toml(
    neon_env_builder: NeonEnvBuilder,
):
    """
    To be removed after https://github.com/neondatabase/neon/pull/8539 is rolled out.
    """

    def set_tenant_config(ps_cfg):
        tenant_config = ps_cfg.setdefault("tenant_config", {})
        tenant_config["timeline_get_throttle"] = throttle_config_with_field_fair_set

    neon_env_builder.pageserver_config_override = set_tenant_config
    env = neon_env_builder.init_start()
    ps_http = env.pageserver.http_client()
    conf = ps_http.tenant_config(env.initial_tenant)
    assert_throttle_config_with_field_fair_set(conf.effective_config["timeline_get_throttle"])
