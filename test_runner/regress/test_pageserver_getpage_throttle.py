from __future__ import annotations

import copy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder

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

    env.pageserver.allowed_errors.append(
        r'.*ignoring unknown configuration item path="tenant_config\.timeline_get_throttle\.fair"*'
    )
