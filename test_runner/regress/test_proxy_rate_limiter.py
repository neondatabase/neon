import asyncio
import time
from pathlib import Path
from typing import Iterator

import pytest
from fixtures.neon_fixtures import (
    PSQL,
    NeonProxy,
)
from fixtures.port_distributor import PortDistributor
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.response import Response


def waiting_handler(status_code: int) -> Response:
    # wait more than timeout to make sure that both (two) connections are open.
    # It would be better to use a barrier here, but I don't know how to do that together with pytest-httpserver.
    time.sleep(2)
    return Response(status=status_code)


@pytest.fixture(scope="function")
def proxy_with_rate_limit(
    port_distributor: PortDistributor,
    neon_binpath: Path,
    httpserver_listen_address,
    test_output_dir: Path,
) -> Iterator[NeonProxy]:
    """Neon proxy that routes directly to vanilla postgres."""

    proxy_port = port_distributor.get_port()
    mgmt_port = port_distributor.get_port()
    http_port = port_distributor.get_port()
    external_http_port = port_distributor.get_port()
    (host, port) = httpserver_listen_address
    endpoint = f"http://{host}:{port}/billing/api/v1/usage_events"

    with NeonProxy(
        neon_binpath=neon_binpath,
        test_output_dir=test_output_dir,
        proxy_port=proxy_port,
        http_port=http_port,
        mgmt_port=mgmt_port,
        external_http_port=external_http_port,
        auth_backend=NeonProxy.Console(endpoint, fixed_rate_limit=5),
    ) as proxy:
        proxy.start()
        yield proxy


@pytest.mark.asyncio
async def test_proxy_rate_limit(
    httpserver: HTTPServer,
    proxy_with_rate_limit: NeonProxy,
):
    uri = "/billing/api/v1/usage_events/proxy_get_role_secret"
    # mock control plane service
    httpserver.expect_ordered_request(uri, method="GET").respond_with_handler(
        lambda _: Response(status=200)
    )
    httpserver.expect_ordered_request(uri, method="GET").respond_with_handler(
        lambda _: waiting_handler(429)
    )
    httpserver.expect_ordered_request(uri, method="GET").respond_with_handler(
        lambda _: waiting_handler(500)
    )

    psql = PSQL(host=proxy_with_rate_limit.host, port=proxy_with_rate_limit.proxy_port)
    f = await psql.run("select 42;")
    await proxy_with_rate_limit.find_auth_link(uri, f)
    # Limit should be 2.

    # Run two queries in parallel.
    f1, f2 = await asyncio.gather(psql.run("select 42;"), psql.run("select 42;"))
    await proxy_with_rate_limit.find_auth_link(uri, f1)
    await proxy_with_rate_limit.find_auth_link(uri, f2)

    # Now limit should be 0.
    f = await psql.run("select 42;")
    await proxy_with_rate_limit.find_auth_link(uri, f)

    # There last query shouldn't reach the http-server.
    assert httpserver.assertions == []
