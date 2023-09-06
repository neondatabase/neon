#
# Test for collecting metrics from pageserver and proxy.
# Use mock HTTP server to receive metrics and verify that they look sane.
#

import time
from pathlib import Path
from typing import Iterator

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    PSQL,
    NeonEnvBuilder,
    NeonProxy,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import RemoteStorageKind
from fixtures.types import TenantId
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

# ==============================================================================
# Storage metrics tests
# ==============================================================================

initial_tenant = TenantId.generate()
remote_uploaded = 0
checks = {
    "written_size": lambda value: value > 0,
    "resident_size": lambda value: value >= 0,
    # >= 0 check here is to avoid race condition when we receive metrics before
    # remote_uploaded is updated
    "remote_storage_size": lambda value: value > 0 if remote_uploaded > 0 else value >= 0,
    # logical size may lag behind the actual size, so allow 0 here
    "timeline_logical_size": lambda value: value >= 0,
}

metric_kinds_checked = set([])


#
# verify that metrics look minilally sane
#
def metrics_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)

    events = request.json["events"]
    log.info("received events:")
    log.info(events)

    for event in events:
        assert event["tenant_id"] == str(
            initial_tenant
        ), "Expecting metrics only from the initial tenant"
        metric_name = event["metric"]

        check = checks.get(metric_name)
        # calm down mypy
        if check is not None:
            assert check(event["value"]), f"{metric_name} isn't valid"
            global metric_kinds_checked
            metric_kinds_checked.add(metric_name)

    return Response(status=200)


@pytest.mark.parametrize(
    "remote_storage_kind", [RemoteStorageKind.NOOP, RemoteStorageKind.LOCAL_FS]
)
def test_metric_collection(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address,
    remote_storage_kind: RemoteStorageKind,
):
    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"

    # Require collecting metrics frequently, since we change
    # the timeline and want something to be logged about it.
    #
    # Disable time-based pitr, we will use the manual GC calls
    # to trigger remote storage operations in a controlled way
    neon_env_builder.pageserver_config_override = (
        f"""
        metric_collection_interval="1s"
        metric_collection_endpoint="{metric_collection_endpoint}"
    """
        + "tenant_config={pitr_interval = '0 sec'}"
    )

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    log.info(f"test_metric_collection endpoint is {metric_collection_endpoint}")

    # Set initial tenant of the test, that we expect the logs from
    global initial_tenant
    initial_tenant = neon_env_builder.initial_tenant
    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        metrics_handler
    )

    # spin up neon,  after http server is ready
    env = neon_env_builder.init_start()
    # Order of fixtures shutdown is not specified, and if http server gets down
    # before pageserver, pageserver log might contain such errors in the end.
    env.pageserver.allowed_errors.append(".*metrics endpoint refused the sent metrics*")
    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_metric_collection")
    endpoint = env.endpoints.create_start("test_metric_collection")

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("CREATE TABLE foo (id int, counter int, t text)")
    cur.execute(
        """
        INSERT INTO foo
        SELECT g, 0, 'long string to consume some space' || g
        FROM generate_series(1, 100000) g
        """
    )

    # Helper function that gets the number of given kind of remote ops from the metrics
    def get_num_remote_ops(file_kind: str, op_kind: str) -> int:
        ps_metrics = env.pageserver.http_client().get_metrics()
        total = 0.0
        for sample in ps_metrics.query_all(
            name="pageserver_remote_operation_seconds_count",
            filter={
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
        ):
            total += sample[2]
        return int(total)

    # upload some data to remote storage
    if remote_storage_kind == RemoteStorageKind.LOCAL_FS:
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
        pageserver_http = env.pageserver.http_client()
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        pageserver_http.timeline_gc(tenant_id, timeline_id, 10000)
        global remote_uploaded
        remote_uploaded = get_num_remote_ops("index", "upload")
        assert remote_uploaded > 0

    # wait longer than collecting interval and check that all requests are served
    time.sleep(3)
    httpserver.check()
    global metric_kinds_checked, checks
    expected_checks = set(checks.keys())
    assert len(metric_kinds_checked) == len(
        checks
    ), f"Expected to receive and check all kind of metrics, but {expected_checks - metric_kinds_checked} got uncovered"


# ==============================================================================
# Proxy metrics tests
# ==============================================================================


def proxy_metrics_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)

    events = request.json["events"]
    log.info("received events:")
    log.info(events)

    # perform basic sanity checks
    for event in events:
        assert event["metric"] == "proxy_io_bytes_per_client"
        assert event["endpoint_id"] == "test_endpoint_id"
        assert event["value"] >= 0
        assert event["stop_time"] >= event["start_time"]

    return Response(status=200)


@pytest.fixture(scope="function")
def proxy_with_metric_collector(
    port_distributor: PortDistributor,
    neon_binpath: Path,
    httpserver_listen_address,
    test_output_dir: Path,
) -> Iterator[NeonProxy]:
    """Neon proxy that routes through link auth and has metric collection enabled."""

    http_port = port_distributor.get_port()
    proxy_port = port_distributor.get_port()
    mgmt_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()

    (host, port) = httpserver_listen_address
    metric_collection_endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"
    metric_collection_interval = "5s"

    with NeonProxy(
        neon_binpath=neon_binpath,
        test_output_dir=test_output_dir,
        proxy_port=proxy_port,
        http_port=http_port,
        mgmt_port=mgmt_port,
        external_http_port=external_http_port,
        metric_collection_endpoint=metric_collection_endpoint,
        metric_collection_interval=metric_collection_interval,
        auth_backend=NeonProxy.Link(),
    ) as proxy:
        proxy.start()
        yield proxy


@pytest.mark.asyncio
async def test_proxy_metric_collection(
    httpserver: HTTPServer,
    proxy_with_metric_collector: NeonProxy,
    vanilla_pg: VanillaPostgres,
):
    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        proxy_metrics_handler
    )

    # do something to generate load to generate metrics
    # sleep for 5 seconds to give metric collector time to collect metrics
    psql = await PSQL(
        host=proxy_with_metric_collector.host, port=proxy_with_metric_collector.proxy_port
    ).run(
        "create table tbl as select * from generate_series(0,1000); select pg_sleep(5); select 42"
    )

    base_uri = proxy_with_metric_collector.link_auth_uri
    link = await NeonProxy.find_auth_link(base_uri, psql)

    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(vanilla_pg, proxy_with_metric_collector, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"

    # do something to generate load to generate metrics
    # sleep for 5 seconds to give metric collector time to collect metrics
    psql = await PSQL(
        host=proxy_with_metric_collector.host, port=proxy_with_metric_collector.proxy_port
    ).run("insert into tbl select * from generate_series(0,1000);  select pg_sleep(5); select 42")

    link = await NeonProxy.find_auth_link(base_uri, psql)
    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(
        vanilla_pg, proxy_with_metric_collector, psql_session_id, create_user=False
    )

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"

    httpserver.check()
