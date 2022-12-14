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


@pytest.mark.asyncio
async def test_psql_session_id(vanilla_pg: VanillaPostgres, link_proxy: NeonProxy):
    def get_session_id(uri_prefix, uri_line):
        assert uri_prefix in uri_line

        url_parts = urlparse(uri_line)
        psql_session_id = url_parts.path[1:]
        assert psql_session_id.isalnum(), "session_id should only contain alphanumeric chars"

        return psql_session_id

    async def find_auth_link(link_auth_uri, proc):
        for _ in range(100):
            line = (await proc.stderr.readline()).decode("utf-8").strip()
            log.info(f"psql line: {line}")
            if link_auth_uri in line:
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

    psql = await PSQL(host=link_proxy.host, port=link_proxy.proxy_port).run("select 42")

    base_uri = link_proxy.link_auth_uri
    link = await find_auth_link(base_uri, psql)

    psql_session_id = get_session_id(base_uri, link)
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


def test_auth_errors(static_proxy: NeonProxy):
    # User does not exist
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", options="project=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    static_proxy.safe_psql(
        "create role pinocchio with login password 'magic'", options="project=irrelevant"
    )

    # User exists, but password is missing
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password=None, options="project=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    # User exists, but password is wrong
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password="bad", options="project=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    # Finally, check that the user can connect
    with static_proxy.connect(user="pinocchio", password="magic", options="project=irrelevant"):
        pass
