#
# Test for collecting metrics from pageserver and proxy.
# Use mock HTTP server to receive metrics and verify that they look sane.
#

import time
from pathlib import Path
from queue import SimpleQueue
from typing import Any, Iterator

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    PSQL,
    NeonEnvBuilder,
    NeonProxy,
    TenantId,
    TimelineId,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.port_distributor import PortDistributor
from fixtures.remote_storage import RemoteStorageKind
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response
import json


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

    uploads: SimpleQueue[Any] = SimpleQueue()

    def metrics_handler(request: Request) -> Response:
        if request.json is None:
            return Response(status=400)

        events = request.json["events"]
        uploads.put(events)
        return Response(status=200)

    # Require collecting metrics frequently, since we change
    # the timeline and want something to be logged about it.
    #
    # Disable time-based pitr, we will use the manual GC calls
    # to trigger remote storage operations in a controlled way
    neon_env_builder.pageserver_config_override = (
        f"""
        metric_collection_interval="1s"
        metric_collection_endpoint="{metric_collection_endpoint}"
        cached_metric_collection_interval="0s"
        synthetic_size_calculation_interval="3s"
    """
        + "tenant_config={pitr_interval = '0 sec'}"
    )

    neon_env_builder.enable_pageserver_remote_storage(remote_storage_kind)

    log.info(f"test_metric_collection endpoint is {metric_collection_endpoint}")

    # mock http server that returns OK for the metrics
    httpserver.expect_request("/billing/api/v1/usage_events", method="POST").respond_with_handler(
        metrics_handler
    )

    # spin up neon,  after http server is ready
    env = neon_env_builder.init_start()
    # httpserver is shut down before pageserver during passing run
    env.pageserver.allowed_errors.append(".*metrics endpoint refused the sent metrics*")
    tenant_id = env.initial_tenant
    timeline_id = env.initial_timeline
    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)

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

    remote_uploaded = 0

    # upload some data to remote storage
    if remote_storage_kind == RemoteStorageKind.LOCAL_FS:
        wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
        pageserver_http = env.pageserver.http_client()
        pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
        pageserver_http.timeline_gc(tenant_id, timeline_id, 10000)

        remote_uploaded = get_num_remote_ops("index", "upload")
        assert remote_uploaded > 0

    # we expect uploads at 1Hz, on busy runners this could be too optimistic,
    # so give 5s we only want to get the following upload after "ready" value.
    # later tests will be added to ensure that the timeseries are sane.
    timeout = 5
    uploads.put("ready")

    v = MetricsVerifier()

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            events = uploads.get(timeout=timeout)
            v.ingest(events)
            break
        else:
            v.ingest(events)

    if "synthetic_storage_size" not in v.accepted_event_names():
        log.info("waiting for synthetic storage size to be calculated and uploaded...")

    rounds = 0
    while "synthetic_storage_size" not in v.accepted_event_names():
        events = uploads.get(timeout=timeout)
        v.ingest(events)
        rounds += 1
        assert rounds < 10, "did not get synthetic_storage_size in 10 uploads"
        # once we have it in verifiers, it will assert that future batches will contain it

    env.pageserver.stop()
    time.sleep(1)
    uploads.put("ready")
    env.pageserver.start()

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            events = uploads.get(timeout=timeout*3)
            v.ingest(events)
            events = uploads.get(timeout=timeout)
            v.ingest(events)
            break
        else:
            v.ingest(events)

    httpserver.check()
    httpserver.stop()


class MetricsVerifier:
    """A graph of verifiers, allowing one for each metric"""
    def __init__(self):
        self.tenants = {}
        pass

    def ingest(self, events):
        stringified = json.dumps(events, indent=2)
        log.info(f"ingesting: {stringified}")
        for event in events:
            id = TenantId(event["tenant_id"])
            if id not in self.tenants:
                self.tenants[id] = TenantMetricsVerifier(id)

            self.tenants[id].ingest(event)

        for t in self.tenants.values():
            t.post_batch()

    def accepted_event_names(self):
        names = set()
        for t in self.tenants.values():
            names = names.union(t.accepted_event_names())
        return names



class TenantMetricsVerifier:
    def __init__(self, id: TenantId):
        self.id = id
        self.timelines = {}
        self.state = {}

    def ingest(self, event):
        assert TenantId(event["tenant_id"]) == self.id

        if "timeline_id" in event:
            id = TimelineId(event["timeline_id"])
            if id not in self.timelines:
                self.timelines[id] = TimelineMetricsVerifier(self.id, id)

            self.timelines[id].ingest(event)
        else:
            name = event["metric"]
            if name not in self.state:
                self.state[name] = PER_METRIC_VERIFIERS[name]()
            self.state[name].ingest(event, self)

    def post_batch(self):
        for v in self.state.values():
            v.post_batch(self)

        for tl in self.timelines.values():
            tl.post_batch(self)

    def accepted_event_names(self):
        names = set(self.state.keys())
        for t in self.timelines.values():
            names = names.union(t.accepted_event_names())
        return names


class TimelineMetricsVerifier:
    def __init__(self, tenant_id: TenantId, timeline_id: TimelineId):
        self.id = timeline_id
        self.state = {}

    def ingest(self, event):
        name = event["metric"]
        if name not in self.state:
            self.state[name] = PER_METRIC_VERIFIERS[name]()
        self.state[name].ingest(event, self)

    def post_batch(self, parent):
        for v in self.state.values():
            v.post_batch(self)

    def accepted_event_names(self):
        return set(self.state.keys())


class CannotVerifyAnything:
    """We can only assert types, but rust already has types, so no need."""

    def __init__(self):
        pass

    def ingest(self, event, parent):
        pass

    def post_batch(self, parent):
        pass


class WrittenDataVerifier:
    def __init__(self):
        self.values = []
        pass

    def ingest(self, event, parent):
        self.values.append(event["value"])

    def post_batch(self, parent):
        pass


class WrittenDataDeltaVerifier:
    def __init__(self):
        self.value = None
        self.sum = 0
        self.timerange = None
        pass

    def ingest(self, event, parent):
        assert event["type"] == "incremental"
        self.value = event["value"]
        self.sum += event["value"]
        start = event["start_time"]
        stop = event["stop_time"]
        timerange = (start, stop)
        if self.timerange is not None:
            assert self.timerange[1] == timerange[0], "time ranges should be continious"
        self.timerange = timerange

    def post_batch(self, parent):
        absolute = parent.state["written_size"]
        if len(absolute.values) == 1:
            # in tests this comes up as initdb execution, so we can have 0 or
            # about 30MB on the first event. it is not consistent.
            assert self.value is not None
        else:
            assert self.value == absolute.values[-1] - absolute.values[-2]
            # sounds like this should hold, but it will not for branches -- probably related to timing
            # assert self.sum == absolute.latest

class SyntheticSizeVerifier:
    def __init__(self):
        self.prev = None
        self.value = None
        pass

    def ingest(self, event, parent):
        assert isinstance(parent, TenantMetricsVerifier)
        assert event["type"] == "absolute"
        value = event["value"]
        self.value = value

    def post_batch(self, parent):
        if self.prev is not None:
            # this is assuming no one goes and deletes the cache file
            assert self.value is not None, "after calculating first synthetic size, cached or more recent should be returned"
        self.prev = self.value
        self.value = None


PER_METRIC_VERIFIERS = {
    "remote_storage_size": CannotVerifyAnything,
    "resident_size": CannotVerifyAnything,
    "written_size": WrittenDataVerifier,
    "written_data_bytes_delta": WrittenDataDeltaVerifier,
    "timeline_logical_size": CannotVerifyAnything,
    "synthetic_storage_size": SyntheticSizeVerifier,
}


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
