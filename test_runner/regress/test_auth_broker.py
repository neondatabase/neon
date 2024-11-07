import json

import pytest
from fixtures.neon_fixtures import NeonAuthBroker
from jwcrypto import jwk, jwt


@pytest.mark.asyncio
async def test_auth_broker_happy(
    static_auth_broker: NeonAuthBroker,
    neon_authorize_jwk: jwk.JWK,
):
    """
    Signs a JWT and uses it to authorize a query to local_proxy.
    """

    token = jwt.JWT(
        header={"kid": neon_authorize_jwk.key_id, "alg": "RS256"}, claims={"sub": "user1"}
    )
    token.make_signed_token(neon_authorize_jwk)
    res = await static_auth_broker.query("foo", ["arg1"], user="anonymous", token=token.serialize())

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
