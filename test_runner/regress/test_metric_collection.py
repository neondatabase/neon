import time

import pytest
from fixtures.log_helper import log
from fixtures.metrics import parse_metrics
from fixtures.neon_fixtures import (
    NeonEnvBuilder,
    PortDistributor,
    RemoteStorageKind,
    wait_for_last_flush_lsn,
)
from fixtures.types import TenantId, TimelineId
from fixtures.utils import query_scalar
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


@pytest.fixture(scope="session")
def httpserver_listen_address(port_distributor: PortDistributor):
    port = port_distributor.get_port()
    return ("localhost", port)


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

    neon_env_builder.enable_remote_storage(
        remote_storage_kind=remote_storage_kind,
        test_name="test_metric_collection",
    )

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
    env.neon_cli.create_branch("test_metric_collection")
    pg = env.postgres.create_start("test_metric_collection")

    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    tenant_id = TenantId(query_scalar(cur, "SHOW neon.tenant_id"))
    timeline_id = TimelineId(query_scalar(cur, "SHOW neon.timeline_id"))

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
        ps_metrics = parse_metrics(env.pageserver.http_client().get_metrics(), "pageserver")
        total = 0.0
        for sample in ps_metrics.query_all(
            name="pageserver_remote_operation_seconds_count",
            filter={
                "tenant_id": str(tenant_id),
                "timeline_id": str(timeline_id),
                "file_kind": str(file_kind),
                "op_kind": str(op_kind),
            },
        ):
            total += sample[2]
        return int(total)

    # upload some data to remote storage
    if remote_storage_kind == RemoteStorageKind.LOCAL_FS:
        wait_for_last_flush_lsn(env, pg, tenant_id, timeline_id)
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
