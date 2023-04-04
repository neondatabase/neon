from typing import Any, Dict, List, Tuple

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    PortDistributor,
    VanillaPostgres,
)
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


@pytest.fixture(scope="session")
def httpserver_listen_address(port_distributor: PortDistributor):
    port = port_distributor.get_port()
    return ("localhost", port)


def handle_db(dbs, operation):
    if operation["op"] == "set":
        if "old_name" in operation:
            dbs[operation["name"]] = dbs[operation["old_name"]]
            dbs.pop(operation["old_name"])
        if "owner" in operation:
            dbs[operation["name"]] = operation["owner"]
    elif operation["op"] == "del":
        dbs.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def handle_role(roles, operation):
    if operation["op"] == "set":
        if "old_name" in operation:
            roles[operation["name"]] = roles[operation["old_name"]]
            roles.pop(operation["old_name"])
        if "password" in operation:
            roles[operation["name"]] = operation["password"]
    elif operation["op"] == "del":
        if "old_name" in operation:
            roles.pop(operation["old_name"])
        roles.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def ddl_forward_handler(request: Request, dbs: Dict[str, str], roles: Dict[str, str]):
    log.info(f"Received request with data {request.get_data(as_text=True)}")
    if request.json is None:
        log.info("Received invalid JSON")
        return Response(status=400)
    json = request.json
    for operation in json:
        if operation["type"] == "db":
            handle_db(dbs, operation)
        elif operation["type"] == "role":
            handle_role(roles, operation)
        else:
            assert False, "JSON does not specify 'db' or 'role'"
    return Response(status=200)


class DdlForwardingContext:
    def __init__(self, httpserver: HTTPServer, vanilla_pg: VanillaPostgres, host: str, port: int):
        self.server = httpserver
        self.pg = vanilla_pg
        self.host = host
        self.port = port
        self.dbs = {}
        self.roles = {}
        endpoint = "/management/api/v1/roles_and_databases"
        ddl_url = f"http://{host}:{port}{endpoint}"
        self.pg.configure([f"neon.console_url={ddl_url}", "shared_preload_libraries = 'neon'"])
        log.info(f"Listening on {ddl_url}")
        self.server.expect_request(endpoint, method="PATCH").respond_with_handler(
            lambda request: ddl_forward_handler(request, self.dbs, self.roles)
        )
        self.pg.start()

    def send(self, query: str) -> List[Tuple[Any, ...]]:
        return self.pg.safe_psql(query)

    def wait(self, timeout=3):
        self.server.wait(timeout=timeout)

    def send_and_wait(self, query: str, timeout=3) -> List[str]:
        res = self.send(query)
        self.wait(timeout=timeout)
        return res


@pytest.fixture
def ddl(
    httpserver: HTTPServer, vanilla_pg: VanillaPostgres, httpserver_listen_address: tuple[str, int]
):
    (host, port) = httpserver_listen_address
    return DdlForwardingContext(httpserver, vanilla_pg, host, port)


def test_dbs_simple(ddl: DdlForwardingContext):
    curr_user = ddl.send("SELECT current_user")[0][0]
    log.info(f"Current user is {curr_user}")
    ddl.send_and_wait("CREATE DATABASE bork")
    assert ddl.dbs == {"bork": curr_user}
    ddl.send_and_wait("CREATE ROLE volk WITH PASSWORD 'nu_zayats'")
    ddl.send_and_wait("ALTER DATABASE bork RENAME TO nu_pogodi")
    assert ddl.dbs == {"nu_pogodi": curr_user}
    ddl.send_and_wait("ALTER DATABASE nu_pogodi OWNER TO volk")
    assert ddl.dbs == {"nu_pogodi": "volk"}
    ddl.send_and_wait("DROP DATABASE nu_pogodi")
    assert ddl.dbs == {}


def test_roles_simple(ddl: DdlForwardingContext):
    ddl.send_and_wait("CREATE ROLE tarzan WITH PASSWORD 'of_the_apes'")
    assert ddl.roles == {"tarzan": "of_the_apes"}
    ddl.send_and_wait("DROP ROLE tarzan")
    assert ddl.roles == {}
    ddl.send_and_wait("CREATE ROLE tarzan WITH PASSWORD 'of_the_apes'")
    assert ddl.roles == {"tarzan": "of_the_apes"}
    ddl.send_and_wait("ALTER ROLE tarzan WITH PASSWORD 'jungle_man'")
    assert ddl.roles == {"tarzan": "jungle_man"}
    ddl.send_and_wait("ALTER ROLE tarzan RENAME TO mowgli")
    assert ddl.roles == {"mowgli": "jungle_man"}
