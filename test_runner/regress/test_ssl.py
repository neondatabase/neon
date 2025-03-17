import pytest
import requests
from fixtures.neon_fixtures import NeonEnvBuilder, StorageControllerApiException
from fixtures.utils import wait_until


def test_pageserver_https_api(neon_env_builder: NeonEnvBuilder):
    """
    Test HTTPS pageserver management API.
    If NeonEnv starts with use_https_pageserver_api with no errors, it's already a success.
    Make /v1/status request to HTTPS API to ensure it's appropriately configured.
    """
    neon_env_builder.use_https_pageserver_api = True
    env = neon_env_builder.init_start()

    addr = f"https://localhost:{env.pageserver.service_port.https}/v1/status"
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()


def test_safekeeper_https_api(neon_env_builder: NeonEnvBuilder):
    """
    Test HTTPS safekeeper management API.
    1. Make /v1/status request to HTTPS API to ensure it's appropriately configured.
    2. Try to register safekeeper in storcon with https port missing.
    3. Register safekeeper with https port.
    4. Wait for a heartbeat round to complete.
    """
    neon_env_builder.use_https_safekeeper_api = True
    env = neon_env_builder.init_start()

    sk = env.safekeepers[0]

    # 1. Make simple https request.
    addr = f"https://localhost:{sk.port.https}/v1/status"
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()

    # Note: http_port is intentionally wrong.
    # Storcon should not use it if use_https is on.
    http_port = 0

    body = {
        "active": True,
        "id": sk.id,
        "created_at": "2023-10-25T09:11:25Z",
        "updated_at": "2024-08-28T11:32:43Z",
        "region_id": "aws-us-east-2",
        "host": "localhost",
        "port": sk.port.pg,
        "http_port": http_port,
        "https_port": None,
        "version": 5957,
        "availability_zone_id": "us-east-2b",
    }
    # 2. Try register with https port missing.
    with pytest.raises(StorageControllerApiException, match="https port is not specified"):
        env.storage_controller.on_safekeeper_deploy(sk.id, body)

    # 3. Register with https port.
    body["https_port"] = sk.port.https
    env.storage_controller.on_safekeeper_deploy(sk.id, body)

    # 4. Wait for hearbeat round complete.
    def storcon_heartbeat():
        assert env.storage_controller.log_contains(
            "Heartbeat round complete for 1 safekeepers, 0 offline"
        )

    wait_until(storcon_heartbeat)
