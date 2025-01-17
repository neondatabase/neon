from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest
import requests
from pytest_httpserver import HTTPServer
from werkzeug.datastructures import Headers
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response

from fixtures.log_helper import log

if TYPE_CHECKING:
    from typing import Any


class StorageControllerProxy:
    def __init__(self, server: HTTPServer):
        self.server: HTTPServer = server
        self.listen: str = f"http://{server.host}:{server.port}"
        self.routing_to: str | None = None

    def route_to(self, storage_controller_api: str):
        self.routing_to = storage_controller_api

    def port(self) -> int:
        return self.server.port

    def upcall_api_endpoint(self) -> str:
        return f"{self.listen}/upcall/v1"


def proxy_request(method: str, url: str, **kwargs) -> requests.Response:
    return requests.request(method, url, **kwargs)


@pytest.fixture(scope="function")
def storage_controller_proxy(make_httpserver: HTTPServer):
    """
    Proxies requests into the storage controller to the currently
    selected storage controller instance via `StorageControllerProxy.route_to`.

    This fixture is intended for tests that need to run multiple instances
    of the storage controller at the same time.
    """
    server = make_httpserver

    self = StorageControllerProxy(server)

    log.info(f"Storage controller proxy listening on {self.listen}")

    def handler(request: Request) -> Response:
        if self.route_to is None:
            log.info(f"Storage controller proxy has no routing configured for {request.url}")
            return Response("Routing not configured", status=503)

        route_to_url = f"{self.routing_to}{request.path}"

        log.info(f"Routing {request.url} to {route_to_url}")

        args: dict[str, Any] = {"headers": request.headers}
        if request.is_json:
            args["json"] = request.json

        response = proxy_request(request.method, route_to_url, **args)

        headers = Headers()
        for key, value in response.headers.items():
            headers.add(key, value)

        return Response(response.content, headers=headers, status=response.status_code)

    self.server.expect_request(re.compile(".*")).respond_with_handler(handler)

    yield self
    server.clear()
