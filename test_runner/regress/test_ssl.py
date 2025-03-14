import requests
from fixtures.neon_fixtures import NeonEnvBuilder


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
