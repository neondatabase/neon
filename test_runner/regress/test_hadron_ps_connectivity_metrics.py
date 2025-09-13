import json
import shutil

from fixtures.common_types import TenantShardId
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.neon_fixtures import Endpoint, NeonEnvBuilder, NeonPageserver
from requests.exceptions import ConnectionError


# Helper function to attempt reconfiguration of the compute to point to a new pageserver. Note that in these tests,
# we don't expect the reconfiguration attempts to go through, as we will be pointing the compute at a "wrong" pageserver.
def _attempt_reconfiguration(endpoint: Endpoint, new_pageserver_id: int, timeout_sec: float):
    try:
        endpoint.reconfigure(pageserver_id=new_pageserver_id, timeout_sec=timeout_sec)
    except Exception as e:
        log.info(f"reconfiguration failed with exception {e}")
        pass


def read_misrouted_metric_value(pageserver: NeonPageserver) -> float:
    return (
        pageserver.http_client()
        .get_metrics()
        .query_one("pageserver_misrouted_pagestream_requests_total")
        .value
    )


def read_request_error_metric_value(endpoint: Endpoint) -> float:
    return (
        parse_metrics(endpoint.http_client().metrics())
        .query_one("pg_cctl_pagestream_request_errors_total")
        .value
    )


def test_misrouted_to_secondary(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Tests that the following metrics are incremented when compute tries to talk to a secondary pageserver:
    - On pageserver receiving the request: pageserver_misrouted_pagestream_requests_total
    - On compute: pg_cctl_pagestream_request_errors_total
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.broker.start()
    env.storage_controller.start()
    for ps in env.pageservers:
        ps.start()
    for sk in env.safekeepers:
        sk.start()

    # Create a tenant that has one primary and one secondary. Due to primary/secondary placement constraints,
    # the primary and secondary pageservers will be different.
    tenant_id, _ = env.create_tenant(shard_count=1, placement_policy=json.dumps({"Attached": 1}))
    endpoint = env.endpoints.create(
        "main", tenant_id=tenant_id, config_lines=["neon.lakebase_mode = true"]
    )
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    # Get the primary pageserver serving the zero shard of the tenant, and detach it from the primary pageserver.
    # This test operation configures tenant directly on the pageserver/does not go through the storage controller,
    # so the compute does not get any notifications and will keep pointing at the detached pageserver.
    tenant_zero_shard = TenantShardId(tenant_id, shard_number=0, shard_count=1)

    primary_ps = env.get_tenant_pageserver(tenant_zero_shard)
    secondary_ps = (
        env.pageservers[1] if primary_ps.id == env.pageservers[0].id else env.pageservers[0]
    )

    # Now try to point the compute at the pageserver that is acting as secondary for the tenant. Test that the metrics
    # on both compute_ctl and the pageserver register the misrouted requests following the reconfiguration attempt.
    assert read_misrouted_metric_value(secondary_ps) == 0
    assert read_request_error_metric_value(endpoint) == 0
    _attempt_reconfiguration(endpoint, new_pageserver_id=secondary_ps.id, timeout_sec=2.0)
    assert read_misrouted_metric_value(secondary_ps) > 0
    try:
        assert read_request_error_metric_value(endpoint) > 0
    except ConnectionError:
        # When configuring PG to use misconfigured pageserver, PG will cancel the query after certain number of failed
        # reconfigure attempts. This will cause compute_ctl to exit.
        log.info("Cannot connect to PG, ignoring")
        pass


def test_misrouted_to_ps_not_hosting_tenant(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Tests that the following metrics are incremented when compute tries to talk to a pageserver that does not host the tenant:
    - On pageserver receiving the request: pageserver_misrouted_pagestream_requests_total
    - On compute: pg_cctl_pagestream_request_errors_total
    """
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_configs()
    env.broker.start()
    env.storage_controller.start(handle_ps_local_disk_loss=False)
    for ps in env.pageservers:
        ps.start()
    for sk in env.safekeepers:
        sk.start()

    tenant_id, _ = env.create_tenant(shard_count=1)
    endpoint = env.endpoints.create(
        "main", tenant_id=tenant_id, config_lines=["neon.lakebase_mode = true"]
    )
    endpoint.respec(skip_pg_catalog_updates=False)
    endpoint.start()

    tenant_ps_id = env.get_tenant_pageserver(
        TenantShardId(tenant_id, shard_number=0, shard_count=1)
    ).id
    non_hosting_ps = (
        env.pageservers[1] if tenant_ps_id == env.pageservers[0].id else env.pageservers[0]
    )

    # Clear the disk of the non-hosting PS to make sure that it indeed doesn't have any information about the tenant.
    non_hosting_ps.stop(immediate=True)
    shutil.rmtree(non_hosting_ps.tenant_dir())
    non_hosting_ps.start()

    # Now try to point the compute to the non-hosting pageserver. Test that the metrics
    # on both compute_ctl and the pageserver register the misrouted requests following the reconfiguration attempt.
    assert read_misrouted_metric_value(non_hosting_ps) == 0
    assert read_request_error_metric_value(endpoint) == 0
    _attempt_reconfiguration(endpoint, new_pageserver_id=non_hosting_ps.id, timeout_sec=2.0)
    assert read_misrouted_metric_value(non_hosting_ps) > 0
    try:
        assert read_request_error_metric_value(endpoint) > 0
    except ConnectionError:
        # When configuring PG to use misconfigured pageserver, PG will cancel the query after certain number of failed
        # reconfigure attempts. This will cause compute_ctl to exit.
        log.info("Cannot connect to PG, ignoring")
        pass
