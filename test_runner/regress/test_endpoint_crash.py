from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder


@pytest.mark.parametrize(
    "sql_func",
    [
        "trigger_panic",
        "trigger_segfault",
        "💣",  # calls `trigger_segfault` internally
    ],
)
def test_endpoint_crash(neon_env_builder: NeonEnvBuilder, sql_func: str):
    """
    Test that triggering crash from neon_test_utils crashes the endpoint
    """
    env = neon_env_builder.init_start()
    env.create_branch("test_endpoint_crash")
    endpoint = env.endpoints.create_start("test_endpoint_crash")

    endpoint.safe_psql("CREATE EXTENSION neon_test_utils;")
    with pytest.raises(Exception, match="This probably means the server terminated abnormally"):
        endpoint.safe_psql(f"SELECT {sql_func}();")
