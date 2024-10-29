import json

import pytest
from fixtures.neon_fixtures import NeonAuthBroker
from jwcrypto import jwk, jwt


@pytest.mark.asyncio
@pytest.mark.parametrize("role_names", [["anonymous", "authenticated"]])
@pytest.mark.parametrize("audience", [None])
async def test_auth_broker_happy(
    static_auth_broker: NeonAuthBroker,
    cplane_wake_compute_local_proxy: None,
    cplane_endpoint_jwks: jwk.JWK,
):
    """
    Signs a JWT and uses it to authorize a query to local_proxy.
    """

    token = jwt.JWT(
        header={"kid": cplane_endpoint_jwks.key_id, "alg": "RS256"}, claims={"sub": "user1"}
    )
    token.make_signed_token(cplane_endpoint_jwks)
    res = await static_auth_broker.query(
        "foo", ["arg1"], user="anonymous", token=token.serialize(), expected_code=200
    )

    # local proxy mock just echos back the request
    # check that we forward the correct data

    assert (
        res["headers"]["authorization"] == f"Bearer {token.serialize()}"
    ), "JWT should be forwarded"

    assert (
        "anonymous" in res["headers"]["neon-connection-string"]
    ), "conn string should be forwarded"

    assert json.loads(res["body"]) == {
        "query": "foo",
        "params": ["arg1"],
    }, "Query body should be forwarded"


@pytest.mark.asyncio
@pytest.mark.parametrize("role_names", [["anonymous", "authenticated"]])
@pytest.mark.parametrize("audience", [None])
async def test_auth_broker_incorrect_role(
    static_auth_broker: NeonAuthBroker,
    cplane_wake_compute_local_proxy: None,
    cplane_endpoint_jwks: jwk.JWK,
):
    """
    Connects to auth broker with the wrong role associated with the JWKs
    """

    token = jwt.JWT(
        header={"kid": cplane_endpoint_jwks.key_id, "alg": "RS256"}, claims={"sub": "user1"}
    )
    token.make_signed_token(cplane_endpoint_jwks)
    res = await static_auth_broker.query(
        "foo", ["arg1"], user="wrong_role", token=token.serialize(), expected_code=400
    )

    # if the user is wrong, we announce that the jwk was not found.
    assert "jwk not found" in res["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("role_names", [["anonymous", "authenticated"]])
@pytest.mark.parametrize("audience", ["neon"])
async def test_auth_broker_incorrect_aud(
    static_auth_broker: NeonAuthBroker,
    cplane_wake_compute_local_proxy: None,
    cplane_endpoint_jwks: jwk.JWK,
):
    """
    Connects to auth broker with the wrong audience associated with the JWKs
    """

    token = jwt.JWT(
        header={"kid": cplane_endpoint_jwks.key_id, "alg": "RS256"},
        claims={"sub": "user1", "aud": "wrong_aud"},
    )
    token.make_signed_token(cplane_endpoint_jwks)
    res = await static_auth_broker.query(
        "foo", ["arg1"], user="anonymous", token=token.serialize(), expected_code=400
    )

    assert "invalid JWT token audience" in res["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("role_names", [["anonymous", "authenticated"]])
@pytest.mark.parametrize("audience", ["neon"])
async def test_auth_broker_missing_aud(
    static_auth_broker: NeonAuthBroker,
    cplane_wake_compute_local_proxy: None,
    cplane_endpoint_jwks: jwk.JWK,
):
    """
    Connects to auth broker with no audience
    """

    token = jwt.JWT(
        header={"kid": cplane_endpoint_jwks.key_id, "alg": "RS256"},
        claims={"sub": "user1"},
    )
    token.make_signed_token(cplane_endpoint_jwks)
    res = await static_auth_broker.query(
        "foo", ["arg1"], user="anonymous", token=token.serialize(), expected_code=400
    )

    assert "invalid JWT token audience" in res["message"]
