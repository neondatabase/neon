from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pytest_httpserver import HTTPServer

if TYPE_CHECKING:
    from collections.abc import Iterator

    from fixtures.port_distributor import PortDistributor

# TODO: mypy fails with:
#  Module "fixtures.neon_fixtures" does not explicitly export attribute "PortDistributor"  [attr-defined]
# from fixtures.neon_fixtures import PortDistributor

# compared to the fixtures from pytest_httpserver with same names, these are
# always function scoped, so you can check and stop the server in tests.


@pytest.fixture(scope="function")
def httpserver_ssl_context():
    return None


@pytest.fixture(scope="function")
def make_httpserver(httpserver_listen_address, httpserver_ssl_context) -> Iterator[HTTPServer]:
    host, port = httpserver_listen_address
    if not host:
        host = HTTPServer.DEFAULT_LISTEN_HOST
    if not port:
        port = HTTPServer.DEFAULT_LISTEN_PORT

    server = HTTPServer(host=host, port=port, ssl_context=httpserver_ssl_context)
    server.start()
    yield server
    server.clear()
    if server.is_running():
        server.stop()


@pytest.fixture(scope="function")
def httpserver(make_httpserver: HTTPServer) -> Iterator[HTTPServer]:
    server = make_httpserver
    yield server
    server.clear()


@pytest.fixture(scope="function")
def httpserver_listen_address(port_distributor: PortDistributor) -> tuple[str, int]:
    port = port_distributor.get_port()
    return ("localhost", port)
