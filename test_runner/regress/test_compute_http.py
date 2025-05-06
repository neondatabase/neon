from __future__ import annotations

from http.client import FORBIDDEN, UNAUTHORIZED
from typing import TYPE_CHECKING

import jwt
import pytest
from fixtures.endpoint.http import COMPUTE_AUDIENCE, ComputeClaimsScope, EndpointHttpClient
from fixtures.utils import run_only_on_default_postgres
from requests import RequestException

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


@run_only_on_default_postgres("The code path being tested is not dependent on Postgres version")
def test_compute_no_scope_claim(neon_simple_env: NeonEnv):
    """
    Test that if the JWT scope is not admin and no compute_id is specified,
    the external HTTP server returns a 403 Forbidden error.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    # Encode nothing in the token
    token = jwt.encode({}, env.auth_keys.priv, algorithm="EdDSA")

    # Create an admin-scoped HTTP client
    client = EndpointHttpClient(
        external_port=endpoint.external_http_port,
        internal_port=endpoint.internal_http_port,
        jwt=token,
    )

    try:
        client.status()
        pytest.fail("Exception should have been raised")
    except RequestException as e:
        assert e.response is not None
        assert e.response.status_code == FORBIDDEN


@pytest.mark.parametrize(
    "audience",
    (COMPUTE_AUDIENCE, "invalid", None),
    ids=["with_audience", "with_invalid_audience", "without_audience"],
)
@run_only_on_default_postgres("The code path being tested is not dependent on Postgres version")
def test_compute_admin_scope_claim(neon_simple_env: NeonEnv, audience: str | None):
    """
    Test that an admin-scoped JWT can access the compute's external HTTP server
    without the compute_id being specified in the claims.
    """
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")

    data: dict[str, str | list[str]] = {"scope": str(ComputeClaimsScope.ADMIN)}
    if audience:
        data["aud"] = [audience]

    token = jwt.encode(data, env.auth_keys.priv, algorithm="EdDSA")

    # Create an admin-scoped HTTP client
    client = EndpointHttpClient(
        external_port=endpoint.external_http_port,
        internal_port=endpoint.internal_http_port,
        jwt=token,
    )

    try:
        client.status()
        if audience != COMPUTE_AUDIENCE:
            pytest.fail("Exception should have been raised")
    except RequestException as e:
        assert e.response is not None
        assert e.response.status_code == UNAUTHORIZED
