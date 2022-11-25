import json
from urllib.parse import urlparse

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres


def test_proxy_select_1(static_proxy: NeonProxy):
    static_proxy.safe_psql("select 1", options="project=generic-project-name")


def test_password_hack(static_proxy: NeonProxy):
    user = "borat"
    password = "password"
    static_proxy.safe_psql(
        f"create role {user} with login password '{password}'", options="project=irrelevant"
    )

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


def get_session_id(uri_prefix, uri_line):
    assert uri_prefix in uri_line

    url_parts = urlparse(uri_line)
    psql_session_id = url_parts.path[1:]
    assert psql_session_id.isalnum(), "session_id should only contain alphanumeric chars"

    return psql_session_id


async def find_auth_link(link_auth_uri_prefix, proc):
    for _ in range(100):
        line = (await proc.stderr.readline()).decode("utf-8").strip()
        log.info(f"psql line: {line}")
        if link_auth_uri_prefix in line:
            log.info(f"SUCCESS, found auth url: {line}")
            return line


async def activate_link_auth(local_vanilla_pg, link_proxy, psql_session_id):
    pg_user = "proxy"

    log.info("creating a new user for link auth test")
    local_vanilla_pg.start()
    local_vanilla_pg.safe_psql(f"create user {pg_user} with login superuser")

    db_info = json.dumps(
        {
            "session_id": psql_session_id,
            "result": {
                "Success": {
                    "host": local_vanilla_pg.default_options["host"],
                    "port": local_vanilla_pg.default_options["port"],
                    "dbname": local_vanilla_pg.default_options["dbname"],
                    "user": pg_user,
                    "project": "irrelevant",
                }
            },
        }
    )

    log.info("sending session activation message")
    psql = await PSQL(host=link_proxy.host, port=link_proxy.mgmt_port).run(db_info)
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "ok"


@pytest.mark.asyncio
async def test_psql_session_id(vanilla_pg: VanillaPostgres, link_proxy: NeonProxy):
    psql = await PSQL(host=link_proxy.host, port=link_proxy.proxy_port).run("select 42")

    uri_prefix = link_proxy.link_auth_uri_prefix
    link = await find_auth_link(uri_prefix, psql)

    psql_session_id = get_session_id(uri_prefix, link)
    await activate_link_auth(vanilla_pg, link_proxy, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"


# Pass extra options to the server.
def test_proxy_options(static_proxy: NeonProxy):
    with static_proxy.connect(options="project=irrelevant -cproxytest.option=value") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW proxytest.option")
            value = cur.fetchall()[0][0]
            assert value == "value"
