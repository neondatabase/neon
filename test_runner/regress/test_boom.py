import pytest
from fixtures.neon_fixtures import NeonEnvBuilder


def test_boom(neon_env_builder: NeonEnvBuilder):
    """
    Test that calling `SELECT 💣();` (from neon_test_utils) crashes the endpoint
    """
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_💣")
    endpoint = env.endpoints.create_start("test_💣")

    endpoint.safe_psql("CREATE EXTENSION neon_test_utils;")
    with pytest.raises(Exception, match="This probably means the server terminated abnormally"):
        endpoint.safe_psql("SELECT 💣();")
