from __future__ import annotations

from typing import TYPE_CHECKING

import requests
from fixtures.endpoint.http import BearerAuth
from fixtures.utils import run_only_on_default_postgres

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


@run_only_on_default_postgres("The code path being tested is not dependent on Postgres version")
def test_compute_admin_scope(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")
    jwt = endpoint.generate_jwt(scope="admin")
    resp = requests.get(
        f"http://localhost:{endpoint.external_http_port}/status", auth=BearerAuth(jwt)
    )
    resp.raise_for_status()
