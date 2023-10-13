from types import TracebackType
from typing import Any, Dict, List, Optional, Tuple, Type

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import VanillaPostgres
from pytest_httpserver import HTTPServer
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def handle_db(dbs, roles, operation):
    if operation["op"] == "set":
        if "old_name" in operation and operation["old_name"] in dbs:
            dbs[operation["name"]] = dbs[operation["old_name"]]
            dbs.pop(operation["old_name"])
        if "owner" in operation:
            dbs[operation["name"]] = operation["owner"]
    elif operation["op"] == "del":
        dbs.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def handle_role(dbs, roles, operation):
    if operation["op"] == "set":
        if "old_name" in operation and operation["old_name"] in roles:
            roles[operation["name"]] = roles[operation["old_name"]]
            roles.pop(operation["old_name"])
            for db, owner in dbs.items():
                if owner == operation["old_name"]:
                    dbs[db] = operation["name"]
        if "password" in operation:
            roles[operation["name"]] = operation["password"]
            assert "encrypted_password" in operation
    elif operation["op"] == "del":
        if "old_name" in operation:
            roles.pop(operation["old_name"])
        roles.pop(operation["name"])
    else:
        raise ValueError("Invalid op")


def ddl_forward_handler(
    request: Request, dbs: Dict[str, str], roles: Dict[str, str], ddl: "DdlForwardingContext"
) -> Response:
    log.info(f"Received request with data {request.get_data(as_text=True)}")
    if ddl.fail:
        log.info("FAILING")
        return Response(status=500, response="Failed just cuz")
    if request.json is None:
        log.info("Received invalid JSON")
        return Response(status=400)
    json = request.json
    # Handle roles first
    if "roles" in json:
        for operation in json["roles"]:
            handle_role(dbs, roles, operation)
    if "dbs" in json:
        for operation in json["dbs"]:
            handle_db(dbs, roles, operation)
    return Response(status=200)


class DdlForwardingContext:
    def __init__(self, httpserver: HTTPServer, vanilla_pg: VanillaPostgres, host: str, port: int):
        self.server = httpserver
        self.pg = vanilla_pg
        self.host = host
        self.port = port
        self.dbs: Dict[str, str] = {}
        self.roles: Dict[str, str] = {}
        self.fail = False
        endpoint = "/management/api/v2/roles_and_databases"
        ddl_url = f"http://{host}:{port}{endpoint}"
        self.pg.configure(
            [
                f"neon.console_url={ddl_url}",
                "shared_preload_libraries = 'neon'",
            ]
        )
        log.info(f"Listening on {ddl_url}")
        self.server.expect_request(endpoint, method="PATCH").respond_with_handler(
            lambda request: ddl_forward_handler(request, self.dbs, self.roles, self)
        )

    def __enter__(self):
        self.pg.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ):
        self.pg.stop()

    def send(self, query: str) -> List[Tuple[Any, ...]]:
        return self.pg.safe_psql(query)

    def wait(self, timeout=3):
        self.server.wait(timeout=timeout)

    def failures(self, bool):
        self.fail = bool

    def send_and_wait(self, query: str, timeout=3) -> List[Tuple[Any, ...]]:
        res = self.send(query)
        self.wait(timeout=timeout)
        return res


@pytest.fixture(scope="function")
def ddl(
    httpserver: HTTPServer, vanilla_pg: VanillaPostgres, httpserver_listen_address: tuple[str, int]
):
    (host, port) = httpserver_listen_address
    with DdlForwardingContext(httpserver, vanilla_pg, host, port) as ddl:
        yield ddl


def test_ddl_forwarding(ddl: DdlForwardingContext):
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
    ddl.send_and_wait("DROP ROLE volk")
    assert ddl.roles == {}

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
    ddl.send_and_wait("DROP ROLE mowgli")
    assert ddl.roles == {}

    conn = ddl.pg.connect()
    cur = conn.cursor()

    cur.execute("BEGIN")
    cur.execute("CREATE ROLE bork WITH PASSWORD 'cork'")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"bork": "cork"}
    cur.execute("BEGIN")
    cur.execute("CREATE ROLE stork WITH PASSWORD 'pork'")
    cur.execute("ABORT")
    ddl.wait()
    assert ("stork", "pork") not in ddl.roles.items()
    cur.execute("BEGIN")
    cur.execute("ALTER ROLE bork WITH PASSWORD 'pork'")
    cur.execute("ALTER ROLE bork RENAME TO stork")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"stork": "pork"}
    cur.execute("BEGIN")
    cur.execute("CREATE ROLE dork WITH PASSWORD 'york'")
    cur.execute("SAVEPOINT point")
    cur.execute("ALTER ROLE dork WITH PASSWORD 'zork'")
    cur.execute("ALTER ROLE dork RENAME TO fork")
    cur.execute("ROLLBACK TO SAVEPOINT point")
    cur.execute("ALTER ROLE dork WITH PASSWORD 'fork'")
    cur.execute("ALTER ROLE dork RENAME TO zork")
    cur.execute("RELEASE SAVEPOINT point")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"stork": "pork", "zork": "fork"}

    cur.execute("DROP ROLE stork")
    cur.execute("DROP ROLE zork")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'dork'")
    cur.execute("CREATE ROLE stork WITH PASSWORD 'cork'")
    cur.execute("BEGIN")
    cur.execute("DROP ROLE bork")
    cur.execute("ALTER ROLE stork RENAME TO bork")
    cur.execute("COMMIT")
    ddl.wait()
    assert ddl.roles == {"bork": "cork"}

    cur.execute("DROP ROLE bork")
    ddl.wait()
    assert ddl.roles == {}

    cur.execute("CREATE ROLE bork WITH PASSWORD 'dork'")
    cur.execute("CREATE DATABASE stork WITH OWNER=bork")
    cur.execute("ALTER ROLE bork RENAME TO cork")
    ddl.wait()
    assert ddl.dbs == {"stork": "cork"}

    with pytest.raises(psycopg2.InternalError):
        ddl.failures(True)
        cur.execute("CREATE DATABASE failure WITH OWNER=cork")
        ddl.wait()

    ddl.failures(False)
    conn.close()
