import os
import ssl

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


def test_storage_controller_https_api(neon_env_builder: NeonEnvBuilder):
    """
    Test HTTPS storage controller API.
    If NeonEnv starts with use_https_storage_controller_api with no errors, it's already a success.
    Make /status request to HTTPS API to ensure it's appropriately configured.
    """
    neon_env_builder.use_https_storage_controller_api = True
    env = neon_env_builder.init_start()

    addr = f"https://localhost:{env.storage_controller.port}/status"
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()


def test_certificate_rotation(neon_env_builder: NeonEnvBuilder):
    """
    Test that pageserver reloads certificates when they are updated on the disk.
    Safekeepers and storage controller use the same server implementation, so
    testing only pageserver is fine.
    1. Simple check that HTTPS API works.
    2. Check that the cert returned by the server matches the cert in file.
    3. Replace ps's cert (but not the key).
    4. Check that ps uses the old cert (because the new one doesn't match the key).
    5. Replace ps's key.
    6. Check that ps reloaded the cert and key and returns the new one.
    """
    neon_env_builder.use_https_pageserver_api = True
    # Speed up the test :)
    neon_env_builder.pageserver_config_override = "ssl_cert_reload_period='100 ms'"
    env = neon_env_builder.init_start()

    # It's expecte
    env.pageserver.allowed_errors.append(".*Error reloading certificate.*")

    port = env.pageserver.service_port.https
    assert port is not None

    # 1. Check if https works.
    addr = f"https://localhost:{port}/v1/status"
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()

    ps_cert_path = env.pageserver.workdir / "server.crt"
    ps_key_path = env.pageserver.workdir / "server.key"
    ps_cert = open(ps_cert_path).read()
    # We need another valid certificate to update to.
    # Let's steal it from safekeeper.
    sk_cert_path = env.safekeepers[0].data_dir / "server.crt"
    sk_key_path = env.safekeepers[0].data_dir / "server.key"
    sk_cert = open(sk_cert_path).read()

    # 2. Check that server's certificate match the cert in the file.
    cur_cert = ssl.get_server_certificate(("localhost", port))
    assert cur_cert == ps_cert

    # 3. Replace ps's cert with sk's one.
    os.rename(sk_cert_path, ps_cert_path)

    # Cert shouldn't be reloaded because it doesn't match private key.
    def error_reloading_cert():
        assert env.pageserver.log_contains("Error reloading certificate: .* KeyMismatch")

    wait_until(error_reloading_cert)

    # 4. Check that it uses old cert.
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()
    cur_cert = ssl.get_server_certificate(("localhost", port))
    assert cur_cert == ps_cert

    # 5. Replace ps's private key with sk's one.
    os.rename(sk_key_path, ps_key_path)

    # Wait till ps reloads certificate.
    def cert_reloaded():
        assert env.pageserver.log_contains("Certificate has been reloaded")

    wait_until(cert_reloaded)

    # 6. Check that server returns new cert.
    requests.get(addr, verify=str(env.ssl_ca_file)).raise_for_status()
    cur_cert = ssl.get_server_certificate(("localhost", port))
    assert cur_cert == sk_cert
