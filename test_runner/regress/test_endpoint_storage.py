from time import time

import pytest
from aiohttp import ClientSession
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import run_only_on_default_postgres
from jwcrypto import jwk, jwt


@pytest.mark.asyncio
@run_only_on_default_postgres("test doesn't use postgres")
async def test_endpoint_storage_insert_retrieve_delete(neon_simple_env: NeonEnv):
    """
    Inserts, retrieves, and deletes test file using a JWT token
    """
    env = neon_simple_env
    ep = env.endpoints.create_start(branch_name="main")
    tenant_id = str(ep.tenant_id)
    timeline_id = str(ep.show_timeline_id())
    endpoint_id = ep.endpoint_id

    key_path = env.repo_dir / "auth_private_key.pem"
    key = jwk.JWK.from_pem(key_path.read_bytes())
    claims = {
        "tenant_id": tenant_id,
        "timeline_id": timeline_id,
        "endpoint_id": endpoint_id,
        "exp": round(time()) + 99,
    }
    log.info(f"key path {key_path}\nclaims {claims}")
    token = jwt.JWT(header={"alg": "EdDSA"}, claims=claims)
    token.make_signed_token(key)
    token = token.serialize()

    base_url = env.endpoint_storage.base_url()
    key = f"http://{base_url}/{tenant_id}/{timeline_id}/{endpoint_id}/key"
    headers = {"Authorization": f"Bearer {token}"}
    log.info(f"cache key url {key}")

    async with ClientSession(headers=headers) as session:
        async with session.get(key) as res:
            assert res.status == 404, f"Non-existing file is present: {res}"

        data = b"cheburash"
        async with session.put(key, data=data) as res:
            assert res.status == 200, f"Error writing file: {res}"

        async with session.get(key) as res:
            read_data = await res.read()
            assert data == read_data

        async with session.delete(key) as res:
            assert res.status == 200, f"Error removing file {res}"

        async with session.get(key) as res:
            assert res.status == 404, f"File was not deleted: {res}"
