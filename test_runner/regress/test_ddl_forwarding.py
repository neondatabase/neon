import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    PSQL,
    NeonEnvBuilder,
    NeonProxy,
    PortDistributor,
    RemoteStorageKind,
    VanillaPostgres,
)
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response
from pytest_httpserver import HTTPServer

@pytest.fixture(scope="session")
def httpserver_listen_address(port_distributor: PortDistributor):
    port = port_distributor.get_port()
    return ("localhost", port)

def ddl_handler(request: Request) -> Response:
    if request.json is None:
        return Response(status=400)
    log.info(request.json)

def test_ddl_forwarding(
    httpserver: HTTPServer,
    neon_env_builder: NeonEnvBuilder,
    httpserver_listen_address,
):
    (host, port) = httpserver_listen_address
    ddl_url = f"http://{host}:{port}/management/api/v1/roles_and_databases"
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_ddl_forwarding")
    pg = env.postgres.create_start("test_ddl_forwarding")

    pg_conn = pg.connect()
    cur = pg_conn.cursor()

#    a = input('Attach debugger...')
    cur.execute(f"SET neon.console_url = \"{ddl_url}\"")
    cur.execute("CREATE ROLE a WITH PASSWORD 'asdf'")
    httpserver.expect_request("/management/api/v1/roles_and_databases", method="PATCH").respond_with_handler(
        ddl_handler
    )

