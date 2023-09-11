import threading
import time

import pytest
import requests
from fixtures.neon_fixtures import NeonEnvBuilder


def test_repro_incident_2023_09_10(
    neon_env_builder: NeonEnvBuilder,
):
    """
    https://neondb.slack.com/archives/C05SD9001PA/p1694452398525969
    """

    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()

    # TODO: have async "pause"-type failpoint
    ps_http.configure_failpoints(("tenant-delete-before-shutdown-sleep", "return(10000)"))

    def delete_thread_fn():
        ps_http = (
            env.pageserver.http_client()
        )  # not sure if ps_http is thread safe, so, not risk it
        ps_http.tenant_delete(tenant_id=env.initial_tenant)

    delete_thread = threading.Thread(target=delete_thread_fn)
    delete_thread.start()

    def create_thread_fn():
        env.neon_cli.create_tenant()

    create_thread = threading.Thread(target=create_thread_fn)
    create_thread.start()

    time.sleep(2)
    # at this point:
    # create thread will get stuck trying to get TENANTS.write()
    # but delete thread already holds TENANTS.read()

    # in our deadlock, imitate_layer_accesses calls get_tenant().await,
    # which in turn calls TENANTS.read(), which is queued behind the
    # create_thread's TENANTS.write() call
    #
    # for the purposes of this repoducer, we can just call any API
    # that does TENANTS.read(), e.g., list_tenants.
    # If the API doesn't respond in a timely manner, we know it's because of TENANTS.read()

    try:
        # assert our assumptions
        with pytest.raises(requests.exceptions.Timeout):
            ps_http.tenant_list(timeout=2)
    finally:
        delete_thread.join()
        create_thread.join()

    # further validate our assumption that above tenant_list timed out
    expect_msg = (
        r".*WARN.*method=GET path=/v1/tenant request_id=.*: request was dropped before completing.*"
    )
    assert env.pageserver.log_contains(expect_msg)
    env.pageserver.allowed_errors.append(expect_msg)
