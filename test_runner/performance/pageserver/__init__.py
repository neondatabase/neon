"""
Pageserver component-level benchmarks
"""


from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.pageserver.utils import wait_until_all_tenants_state


def ensure_pageserver_ready_for_benchmarking(env: NeonEnv, n_tenants: int):
    """
    Helper function.
    """
    ps_http = env.pageserver.http_client()

    log.info("wait for all tenants to become active")
    wait_until_all_tenants_state(
        ps_http, "Active", iterations=n_tenants, period=1, http_error_ok=False
    )

    # ensure all layers are resident for predictiable performance
    tenants = [info["id"] for info in ps_http.tenant_list()]
    for tenant in tenants:
        for timeline in ps_http.tenant_status(tenant)["timelines"]:
            info = ps_http.layer_map_info(tenant, timeline)
            for layer in info.historic_layers:
                assert not layer.remote

    log.info("ready")
