import json
import subprocess
from urllib.parse import urlparse

import psycopg2
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import PSQL


def test_proxy_select_1(static_proxy):
    static_proxy.safe_psql("select 1", options="project=generic-project-name")


def test_password_hack(static_proxy):
    user = "borat"
    password = "password"
    static_proxy.safe_psql(
        f"create role {user} with login password '{password}'",
        options="project=irrelevant")

    # Note the format of `magic`!
    magic = f"project=irrelevant;{password}"
    static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.errors.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


def get_session_id_from_uri_line(uri_prefix, uri_line):
    assert uri_prefix in uri_line

    url_parts = urlparse(uri_line)
    psql_session_id = url_parts.path[1:]
    link_auth_uri_prefix = uri_line[:-len(url_parts.path)]
    assert (
        link_auth_uri_prefix == uri_prefix
    ), f"Line='{uri_line}' should contain a http auth link of form '{uri_prefix}/<psql_session_id>'."
    assert psql_session_id is not None, "did not find line containing " + uri_prefix

    return psql_session_id


def create_and_send_db_info(local_vanilla_pg, psql_session_id, mgmt_port):
    pg_user = "proxy"
    pg_password = "password"

    local_vanilla_pg.start()
    query = "create user " + pg_user + " with login superuser password '" + pg_password + "'"
    local_vanilla_pg.safe_psql(query)

    port = local_vanilla_pg.default_options["port"]
    host = local_vanilla_pg.default_options["host"]
    dbname = local_vanilla_pg.default_options["dbname"]

    db_info_dict = {
        "session_id": psql_session_id,
        "result": {
            "Success": {
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": pg_user,
                "password": pg_password,
            }
        },
    }
    db_info_str = json.dumps(db_info_dict)
    cmd_line_args__to__mgmt = [
        "psql",
        "-h",
        "127.0.0.1",  # localhost
        "-p",
        f"{mgmt_port}",
        "-c",
        db_info_str,
    ]

    log.info(
        f"Sending to proxy the user and db info: {' '.join(cmd_line_args__to__mgmt)}"
    )
    p = subprocess.Popen(cmd_line_args__to__mgmt, stdout=subprocess.PIPE)
    out, err = p.communicate()
    assert "ok" in str(out)


@pytest.mark.asyncio
async def test_psql_session_id(vanilla_pg, link_proxy):
    """
    Test copied and modified from: test_project_psql_link_auth test from cloud/tests_e2e/tests/test_project.py
     Step 1. establish connection to the proxy
     Step 2. retrieve session_id:
        Step 2.1: read welcome message
        Step 2.2: parse session_id
     Step 3. create a vanilla_pg and send user and db info via command line (using Popen) a psql query via mgmt port to proxy.
     Step 4. assert that select 1 has been executed correctly.
    """

    # Step 1.
    psql = PSQL(
        host=link_proxy.host,
        port=link_proxy.proxy_port,
    )
    proc = await psql.run("select 1")

    # Step 2.1
    line_str = ""
    while link_proxy.link_auth_uri_prefix not in line_str:
        line_raw_bytes = await proc.stderr.readline()
        line_str = line_raw_bytes.decode("utf-8").strip()

    # step 2.2
    uri_prefix = link_proxy.link_auth_uri_prefix
    psql_session_id = get_session_id_from_uri_line(uri_prefix, line_str)
    log.info(
        f"Parsed psql_session_id='{psql_session_id}' from Neon welcome message."
    )

    # Step 3.
    create_and_send_db_info(vanilla_pg, psql_session_id, link_proxy.mgmt_port)

    # Step 4.
    # Expecting proxy output::
    # b' ?column? \n'
    # b'----------\n'
    # b'        1\n'
    # b'(1 row)\n'
    out_bytes = await proc.stdout.read()
    expected_out_bytes = b" ?column? \n----------\n        1\n(1 row)\n\n"
    assert out_bytes == expected_out_bytes


# Pass extra options to the server.
#
# Currently, proxy eats the extra connection options, so this fails.
# See https://github.com/neondatabase/neon/issues/1287
@pytest.mark.xfail
def test_proxy_options(static_proxy):
    with static_proxy.connect(options="-cproxytest.option=value") as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW proxytest.option")
            value = cur.fetchall()[0][0]
            assert value == "value"
