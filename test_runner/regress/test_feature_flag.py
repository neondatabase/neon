from __future__ import annotations

from typing import TYPE_CHECKING

from fixtures.utils import run_only_on_default_postgres

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


@run_only_on_default_postgres("Pageserver-only test only needs to run on one version")
def test_feature_flag(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.pageserver.http_client().force_override_feature_flag("test-feature-flag", "true")
    assert env.pageserver.http_client().evaluate_feature_flag_boolean(
        env.initial_tenant, "test-feature-flag"
    )["result"]["Ok"]
    assert (
        env.pageserver.http_client().evaluate_feature_flag_multivariate(
            env.initial_tenant, "test-feature-flag"
        )["result"]["Ok"]
        == "true"
    )

    env.pageserver.http_client().force_override_feature_flag("test-feature-flag", "false")
    assert (
        env.pageserver.http_client().evaluate_feature_flag_boolean(
            env.initial_tenant, "test-feature-flag"
        )["result"]["Err"]
        == "No condition group is matched"
    )
    assert (
        env.pageserver.http_client().evaluate_feature_flag_multivariate(
            env.initial_tenant, "test-feature-flag"
        )["result"]["Ok"]
        == "false"
    )

    env.pageserver.http_client().force_override_feature_flag("test-feature-flag", None)
    assert (
        "Err"
        in env.pageserver.http_client().evaluate_feature_flag_boolean(
            env.initial_tenant, "test-feature-flag"
        )["result"]
    )
    assert (
        "Err"
        in env.pageserver.http_client().evaluate_feature_flag_multivariate(
            env.initial_tenant, "test-feature-flag"
        )["result"]
    )
