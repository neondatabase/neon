from time import sleep

import pytest
import requests
from fixtures.neon_fixtures import NeonEnv
from fixtures.pg_version import PgVersion


def test_upgrade(pg_version: PgVersion, neon_simple_env: NeonEnv):
    env = neon_simple_env

    upgrade_to: PgVersion
    if pg_version == PgVersion.V14:
        upgrade_to = PgVersion.V15
    elif pg_version == PgVersion.V15:
        upgrade_to = PgVersion.V16
    else:
        pytest.skip("Nothing to upgrade")

    env.neon_cli.create_timeline("test_upgrade")
    endpoint = env.endpoints.create_start("test_upgrade")
    resp = requests.post(
        f"http://localhost:{endpoint.http_port}/upgrade",
        json={
            "pg_version": upgrade_to,
        },
    )
    assert resp.status_code == 202

    while True:
        resp = requests.get(f"http://localhost:{endpoint.http_port}/status")
        assert resp.status_code == 200

        data = resp.json()
        if data["status"] == "upgrading":
            sleep(1)
            continue
        elif data["status"] == "running":
            break
        else:
            pytest.fail(f"Unexpected compute state during upgrade: {data['status']}")

    endpoint.stop_and_destroy()


def test_upgrade_bad_request(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.neon_cli.create_timeline("test_upgrade_bad_request")
    endpoint = env.endpoints.create_start("test_upgrade_bad_request")
    resp = requests.post(f"http://localhost:{endpoint.http_port}/upgrade")
    assert resp.status_code == 400

    # TODO: Use postgres versions that are out of range.
