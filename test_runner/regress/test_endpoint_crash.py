from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from fixtures.pg_version import PgVersion
from fixtures.utils import WITH_SANITIZERS, run_only_on_postgres

if TYPE_CHECKING:
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


@run_only_on_postgres([PgVersion.V17], "Currently, build vith sanitizers is possible with v17 only")
def test_sanitizers(neon_env_builder: NeonEnvBuilder):
    """
    Test that undefined behavior leads to endpoint abort with sanitizers enabled
    """
    env = neon_env_builder.init_start()
    env.create_branch("test_ubsan")
    endpoint = env.endpoints.create_start("test_ubsan")

    # Test case based on https://www.postgresql.org/message-id/17167-028026e4ca333817@postgresql.org
    if not WITH_SANITIZERS:
        endpoint.safe_psql("SELECT 1::int4 << 128")
    else:
        with pytest.raises(Exception, match="This probably means the server terminated abnormally"):
            endpoint.safe_psql("SELECT 1::int4 << 128")
