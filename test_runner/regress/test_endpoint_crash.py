import pytest
from fixtures.neon_tenant import NeonTestTenant


@pytest.mark.parametrize(
    "sql_func",
    [
        "trigger_panic",
        "trigger_segfault",
        "💣",  # calls `trigger_segfault` internally
    ],
)
def test_endpoint_crash(neon_tenant: NeonTestTenant, sql_func: str):
    """
    Test that triggering crash from neon_test_utils crashes the endpoint
    """
    neon_tenant.create_branch("test_endpoint_crash")
    endpoint = neon_tenant.endpoints.create_start("test_endpoint_crash")

    endpoint.safe_psql("CREATE EXTENSION neon_test_utils;")
    with pytest.raises(Exception, match="This probably means the server terminated abnormally"):
        endpoint.safe_psql(f"SELECT {sql_func}();")
