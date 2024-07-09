import concurrent.futures
from typing import Any

import pytest
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

from fixtures.common_types import TenantId
from fixtures.log_helper import log


class ComputeReconfigure:
    def __init__(self, server):
        self.server = server
        self.control_plane_compute_hook_api = f"http://{server.host}:{server.port}/notify-attach"
        self.workloads = {}
        self.on_notify = None

    def register_workload(self, workload):
        self.workloads[workload.tenant_id] = workload

    def register_on_notify(self, fn):
        """
        Add some extra work during a notification, like sleeping to slow things down, or
        logging what was notified.
        """
        self.on_notify = fn


@pytest.fixture(scope="function")
def compute_reconfigure_listener(make_httpserver):
    """
    This fixture exposes an HTTP listener for the storage controller to submit
    compute notifications to us, instead of updating neon_local endpoints itself.

    Although storage controller can use neon_local directly, this causes problems when
    the test is also concurrently modifying endpoints.  Instead, configure storage controller
    to send notifications up to this test code, which will route all endpoint updates
    through Workload, which has a mutex to make concurrent updates safe.
    """
    server = make_httpserver

    self = ComputeReconfigure(server)

    # Do neon_local endpoint reconfiguration in the background so that we can
    # accept a healthy rate of calls into notify-attach.
    reconfigure_threads = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    def handler(request: Request):
        assert request.json is not None
        body: dict[str, Any] = request.json
        log.info("notify-attach request: %s", body)

        if self.on_notify is not None:
            self.on_notify(body)

        try:
            workload = self.workloads[TenantId(body["tenant_id"])]
        except KeyError:
            pass
        else:
            # This causes the endpoint to query storage controller for its location, which
            # is redundant since we already have it here, but this avoids extending the
            # neon_local CLI to take full lists of locations
            reconfigure_threads.submit(lambda workload=workload: workload.reconfigure())  # type: ignore[no-any-return]

        return Response(status=200)

    self.server.expect_request("/notify-attach", method="PUT").respond_with_handler(handler)

    yield self
    reconfigure_threads.shutdown()
    server.clear()
