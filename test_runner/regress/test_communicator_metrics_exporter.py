from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
import requests
import requests_unixsocket  # type: ignore [import-untyped]
from fixtures.metrics import parse_metrics

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv

NEON_COMMUNICATOR_SOCKET_NAME = "neon-communicator.socket"


def test_communicator_metrics(neon_simple_env: NeonEnv):
    """
    Test the communicator's built-in HTTP prometheus exporter
    """
    env = neon_simple_env

    endpoint = env.endpoints.create("main")
    endpoint.start()

    # Change current directory to the data directory, so that we can use
    # a short relative path to refer to the socket. (There's a 100 char
    # limitation on the path.)
    os.chdir(str(endpoint.pgdata_dir))
    session = requests_unixsocket.Session()
    r = session.get(f"http+unix://{NEON_COMMUNICATOR_SOCKET_NAME}/metrics")
    assert r.status_code == 200, f"got response {r.status_code}: {r.text}"

    # quick test that the endpoint returned something expected. (We don't validate
    # that the metrics returned are sensible.)
    m = parse_metrics(r.text)
    m.query_one("lfc_hits")
    m.query_one("lfc_misses")

    # Test panic handling. The /debug/panic endpoint raises a Rust panic. It's
    # expected to unwind and drop the HTTP connection without response, but not
    # kill the process or the server.
    with pytest.raises(
        requests.ConnectionError, match="Remote end closed connection without response"
    ):
        r = session.get(f"http+unix://{NEON_COMMUNICATOR_SOCKET_NAME}/debug/panic")
        assert r.status_code == 500

    # Test that subsequent requests after the panic still work.
    r = session.get(f"http+unix://{NEON_COMMUNICATOR_SOCKET_NAME}/metrics")
    assert r.status_code == 200, f"got response {r.status_code}: {r.text}"
    m = parse_metrics(r.text)
    m.query_one("lfc_hits")
    m.query_one("lfc_misses")
