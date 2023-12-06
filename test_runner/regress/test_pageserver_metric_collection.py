import json
import time
from dataclasses import dataclass
from pathlib import Path
from queue import SimpleQueue
from typing import Any, Dict, Set, Tuple

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    wait_for_last_flush_lsn,
)
from fixtures.types import TenantId, TimelineId
from pytest_httpserver import HTTPServer


def test_metric_collection(
    neon_env_and_metrics_server: Tuple[NeonEnv, HTTPServer, SimpleQueue[Any]]
):
    (env, httpserver, uploads) = neon_env_and_metrics_server

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

    # upload some data to remote storage
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http = env.pageserver.http_client()
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)
    pageserver_http.timeline_gc(tenant_id, timeline_id, 10000)

    remote_uploaded = get_num_remote_ops("index", "upload")
    assert remote_uploaded > 0

    # we expect uploads at 1Hz, on busy runners this could be too optimistic,
    # so give 5s we only want to get the following upload after "ready" value.
    timeout = 5

    # these strings in the upload queue allow synchronizing with the uploads
    # and the main test execution
    uploads.put("ready")

    # note that this verifier graph should live across restarts as long as the
    # cache file lives
    v = MetricsVerifier()

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            (events, is_last) = uploads.get(timeout=timeout)
            v.ingest(events, is_last)
            break
        else:
            (events, is_last) = events
            v.ingest(events, is_last)

    if "synthetic_storage_size" not in v.accepted_event_names():
        log.info("waiting for synthetic storage size to be calculated and uploaded...")

    rounds = 0
    while "synthetic_storage_size" not in v.accepted_event_names():
        (events, is_last) = uploads.get(timeout=timeout)
        v.ingest(events, is_last)
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
            (events, is_last) = uploads.get(timeout=timeout * 3)
            v.ingest(events, is_last)
            (events, is_last) = uploads.get(timeout=timeout)
            v.ingest(events, is_last)
            break
        else:
            (events, is_last) = events
            v.ingest(events, is_last)

    httpserver.check()


def test_metric_collection_cleans_up_tempfile(
    neon_env_and_metrics_server: Tuple[NeonEnv, HTTPServer, SimpleQueue[Any]]
):
    (env, httpserver, uploads) = neon_env_and_metrics_server
    pageserver_http = env.pageserver.http_client()

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

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    pageserver_http.timeline_checkpoint(tenant_id, timeline_id)

    # we expect uploads at 1Hz, on busy runners this could be too optimistic,
    # so give 5s we only want to get the following upload after "ready" value.
    timeout = 5

    # these strings in the upload queue allow synchronizing with the uploads
    # and the main test execution
    uploads.put("ready")

    while True:
        events = uploads.get(timeout=timeout)

        if events == "ready":
            (events, _) = uploads.get(timeout=timeout)
            break

    # should really configure an env?
    pageserver_http.configure_failpoints(("before-persist-last-metrics-collected", "exit"))

    time.sleep(3)

    env.pageserver.stop()

    initially = iterate_pageserver_workdir(env.pageserver.workdir, "last_consumption_metrics.json")

    assert (
        len(initially.matching) == 2
    ), f"expecting actual file and tempfile, but not found: {initially.matching}"

    uploads.put("ready")
    env.pageserver.start()

    while True:
        events = uploads.get(timeout=timeout * 3)

        if events == "ready":
            (events, _) = uploads.get(timeout=timeout)
            break

    env.pageserver.stop()

    later = iterate_pageserver_workdir(env.pageserver.workdir, "last_consumption_metrics.json")

    # it is possible we shutdown the pageserver right at the correct time, so the old tempfile
    # is gone, but we also have a new one.
    only = set(["last_consumption_metrics.json"])
    assert (
        initially.matching.intersection(later.matching) == only
    ), "only initial tempfile should had been removed"
    assert initially.other.issuperset(later.other), "no other files should had been removed"

    httpserver.check()


@dataclass
class PrefixPartitionedFiles:
    matching: Set[str]
    other: Set[str]


def iterate_pageserver_workdir(path: Path, prefix: str) -> PrefixPartitionedFiles:
    """
    Iterates the files in the workdir, returns two sets:
    - files with the prefix
    - files without the prefix
    """

    matching = set()
    other = set()
    for entry in path.iterdir():
        if not entry.is_file():
            continue

        if not entry.name.startswith(prefix):
            other.add(entry.name)
        else:
            matching.add(entry.name)

    return PrefixPartitionedFiles(matching, other)


class MetricsVerifier:
    """
    A graph of per tenant per timeline verifiers, allowing one for each
    metric
    """

    def __init__(self):
        self.tenants: Dict[TenantId, TenantMetricsVerifier] = {}
        pass

    def ingest(self, events, is_last):
        stringified = json.dumps(events, indent=2)
        log.info(f"ingesting: {stringified}")
        for event in events:
            id = TenantId(event["tenant_id"])
            if id not in self.tenants:
                self.tenants[id] = TenantMetricsVerifier(id)

            self.tenants[id].ingest(event)

        if is_last:
            for t in self.tenants.values():
                t.post_batch()

    def accepted_event_names(self) -> Set[str]:
        names: Set[str] = set()
        for t in self.tenants.values():
            names = names.union(t.accepted_event_names())
        return names


class TenantMetricsVerifier:
    def __init__(self, id: TenantId):
        self.id = id
        self.timelines: Dict[TimelineId, TimelineMetricsVerifier] = {}
        self.state: Dict[str, Any] = {}

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

    def accepted_event_names(self) -> Set[str]:
        names = set(self.state.keys())
        for t in self.timelines.values():
            names = names.union(t.accepted_event_names())
        return names


class TimelineMetricsVerifier:
    def __init__(self, tenant_id: TenantId, timeline_id: TimelineId):
        self.id = timeline_id
        self.state: Dict[str, Any] = {}

    def ingest(self, event):
        name = event["metric"]
        if name not in self.state:
            self.state[name] = PER_METRIC_VERIFIERS[name]()
        self.state[name].ingest(event, self)

    def post_batch(self, parent):
        for v in self.state.values():
            v.post_batch(self)

    def accepted_event_names(self) -> Set[str]:
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
            # this holds across restarts
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
            assert (
                self.value is not None
            ), "after calculating first synthetic size, cached or more recent should be sent"
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
