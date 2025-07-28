import json
import signal
import time

import requests
from fixtures.utils import skip_if_proxy_lacks_rest_broker
from jwcrypto import jwt


@skip_if_proxy_lacks_rest_broker()
def test_rest_broker_happy(
    local_proxy_fixed_port, rest_broker_proxy, vanilla_pg, neon_authorize_jwk, httpserver
):
    """Test REST API endpoint using local_proxy and rest_broker_proxy."""

    # Use the fixed port local proxy
    local_proxy = local_proxy_fixed_port

    # Create the required roles for PostgREST authentication
    vanilla_pg.safe_psql("CREATE ROLE authenticator LOGIN")
    vanilla_pg.safe_psql("CREATE ROLE authenticated")
    vanilla_pg.safe_psql("CREATE ROLE anon")
    vanilla_pg.safe_psql("GRANT authenticated TO authenticator")
    vanilla_pg.safe_psql("GRANT anon TO authenticator")

    # Create the pgrst schema and configuration function required by the rest broker
    vanilla_pg.safe_psql("CREATE SCHEMA IF NOT EXISTS pgrst")
    vanilla_pg.safe_psql("""
        CREATE OR REPLACE FUNCTION pgrst.pre_config()
        RETURNS VOID AS $$
          SELECT
              set_config('pgrst.db_schemas', 'test', true)
            , set_config('pgrst.db_aggregates_enabled', 'true', true)
            , set_config('pgrst.db_anon_role', 'anon', true)
            , set_config('pgrst.jwt_aud', '', true)
            , set_config('pgrst.jwt_secret', '', true)
            , set_config('pgrst.jwt_role_claim_key', '."role"', true)

        $$ LANGUAGE SQL;
    """)
    vanilla_pg.safe_psql("GRANT USAGE ON SCHEMA pgrst TO authenticator")
    vanilla_pg.safe_psql("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgrst TO authenticator")

    # Bootstrap the database with test data
    vanilla_pg.safe_psql("CREATE SCHEMA IF NOT EXISTS test")
    vanilla_pg.safe_psql("""
        CREATE TABLE IF NOT EXISTS test.items (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        )
    """)
    vanilla_pg.safe_psql("INSERT INTO test.items (name) VALUES ('test_item')")

    # Grant access to the test schema for the authenticated role
    vanilla_pg.safe_psql("GRANT USAGE ON SCHEMA test TO authenticated")
    vanilla_pg.safe_psql("GRANT SELECT ON ALL TABLES IN SCHEMA test TO authenticated")

    # Set up HTTP server to serve JWKS (like static_auth_broker)
    # Generate public key from the JWK
    public_key = neon_authorize_jwk.export_public(as_dict=True)

    # Set up the httpserver to serve the JWKS
    httpserver.expect_request("/.well-known/jwks.json").respond_with_json({"keys": [public_key]})

    # Create JWKS configuration for the rest broker proxy
    jwks_config = {
        "jwks": [
            {
                "id": "1",
                "role_names": ["authenticator", "authenticated", "anon"],
                "jwks_url": httpserver.url_for("/.well-known/jwks.json"),
                "provider_name": "foo",
                "jwt_audience": None,
            }
        ]
    }

    # Write the JWKS config to the config file that rest_broker_proxy expects
    config_file = rest_broker_proxy.config_path
    with open(config_file, "w") as f:
        json.dump(jwks_config, f)

    # Write the same config to the local_proxy config file
    local_config_file = local_proxy.config_path
    with open(local_config_file, "w") as f:
        json.dump(jwks_config, f)

    # Signal both proxies to reload their config
    if rest_broker_proxy._popen is not None:
        rest_broker_proxy._popen.send_signal(signal.SIGHUP)
    if local_proxy._popen is not None:
        local_proxy._popen.send_signal(signal.SIGHUP)
    # Wait a bit for config to reload
    time.sleep(0.5)

    # Generate a proper JWT token using the JWK (similar to test_auth_broker.py)
    token = jwt.JWT(
        header={"kid": neon_authorize_jwk.key_id, "alg": "RS256"},
        claims={
            "sub": "user",
            "role": "authenticated",  # role that's in role_names
            "exp": 9999999999,  # expires far in the future
            "iat": 1000000000,  # issued at
        },
    )
    token.make_signed_token(neon_authorize_jwk)

    # Debug: Print the JWT claims and config for troubleshooting
    print(f"JWT claims: {token.claims}")
    print(f"JWT header: {token.header}")
    print(f"Config file contains: {jwks_config}")
    print(f"Public key kid: {public_key.get('kid')}")

    # Test REST API call - following SUBZERO.md pattern
    # REST API is served on the WSS port with HTTPS and includes database name
    # ep-purple-glitter-adqior4l-pooler.c-2.us-east-1.aws.neon.tech
    url = f"https://foo.apirest.c-2.local.neon.build:{rest_broker_proxy.wss_port}/postgres/rest/v1/items"

    response = requests.get(
        url,
        headers={
            "Authorization": f"Bearer {token.serialize()}",
        },
        params={"id": "eq.1", "select": "name"},
        verify=False,  # Skip SSL verification for self-signed certs
    )

    print(f"Response status: {response.status_code}")
    print(f"Response headers: {response.headers}")
    print(f"Response body: {response.text}")

    # For now, let's just check that we get some response
    # We can refine the assertions once we see what the actual response looks like
    assert response.status_code in [200]  # Any response means the proxies are working

    # check the response body
    assert response.json() == [{"name": "test_item"}]
