import subprocess

import psycopg2
import pytest
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_proxy_select_1(static_proxy: NeonProxy, option_name: str):
    """
    A simplest smoke test: check proxy against a local postgres instance.
    """

    out = static_proxy.safe_psql("select 1", options=f"{option_name}=generic-project-name")
    assert out[0][0] == 1


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_password_hack(static_proxy: NeonProxy, option_name: str):
    """
    Check the PasswordHack auth flow: an alternative to SCRAM auth for
    clients which can't provide the project/endpoint name via SNI or `options`.
    """

    user = "borat"
    password = "password"
    static_proxy.safe_psql(
        f"create role {user} with login password '{password}'",
        options=f"{option_name}=irrelevant",
    )

    # Note the format of `magic`!
    magic = f"{option_name}=irrelevant;{password}"
    static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)

    # Must also check that invalid magic won't be accepted.
    with pytest.raises(psycopg2.OperationalError):
        magic = "broken"
        static_proxy.safe_psql("select 1", sslsni=0, user=user, password=magic)


@pytest.mark.asyncio
async def test_link_auth(vanilla_pg: VanillaPostgres, link_proxy: NeonProxy):
    """
    Check the Link auth flow: a lightweight auth method which delegates
    all necessary checks to the console by sending client an auth URL.
    """

    psql = await PSQL(host=link_proxy.host, port=link_proxy.proxy_port).run("select 42")

    base_uri = link_proxy.link_auth_uri
    link = await NeonProxy.find_auth_link(base_uri, psql)

    psql_session_id = NeonProxy.get_session_id(base_uri, link)
    await NeonProxy.activate_link_auth(vanilla_pg, link_proxy, psql_session_id)

    assert psql.stdout is not None
    out = (await psql.stdout.read()).decode("utf-8").strip()
    assert out == "42"


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_proxy_options(static_proxy: NeonProxy, option_name: str):
    """
    Check that we pass extra `options` to the PostgreSQL server:
    * `project=...` and `endpoint=...` shouldn't be passed at all
    * (otherwise postgres will raise an error).
    * everything else should be passed as-is.
    """

    options = f"{option_name}=irrelevant -cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options)
    assert out[0][0] == "value"

    options = f"-c proxytest.foo=\\ str {option_name}=irrelevant"
    out = static_proxy.safe_psql("show proxytest.foo", options=options)
    assert out[0][0] == " str"


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_auth_errors(static_proxy: NeonProxy, option_name: str):
    """
    Check that we throw very specific errors in some unsuccessful auth scenarios.
    """

    # User does not exist
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", options=f"{option_name}=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    static_proxy.safe_psql(
        "create role pinocchio with login password 'magic'",
        options=f"{option_name}=irrelevant",
    )

    # User exists, but password is missing
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password=None, options=f"{option_name}=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    # User exists, but password is wrong
    with pytest.raises(psycopg2.Error) as exprinfo:
        static_proxy.connect(user="pinocchio", password="bad", options=f"{option_name}=irrelevant")
    text = str(exprinfo.value).strip()
    assert text.endswith("password authentication failed for user 'pinocchio'")

    # Finally, check that the user can connect
    with static_proxy.connect(
        user="pinocchio", password="magic", options=f"{option_name}=irrelevant"
    ):
        pass


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
def test_forward_params_to_client(static_proxy: NeonProxy, option_name: str):
    """
    Check that we forward all necessary PostgreSQL server params to client.
    """

    # A subset of parameters (GUCs) which postgres
    # sends to the client during connection setup.
    # Unfortunately, `GUC_REPORT` can't be queried.
    # Proxy *should* forward them, otherwise client library
    # might misbehave (e.g. parse timestamps incorrectly).
    reported_params_subset = [
        "client_encoding",
        "integer_datetimes",
        "is_superuser",
        "server_encoding",
        "server_version",
        "session_authorization",
        "standard_conforming_strings",
    ]

    query = """
        select name, setting
        from pg_catalog.pg_settings
        where name = any(%s)
    """

    with static_proxy.connect(options=f"{option_name}=irrelevant") as conn:
        with conn.cursor() as cur:
            cur.execute(query, (reported_params_subset,))
            for name, value in cur.fetchall():
                # Check that proxy has forwarded this parameter.
                assert conn.get_parameter_status(name) == value


@pytest.mark.parametrize("option_name", ["project", "endpoint"])
@pytest.mark.timeout(5)
def test_close_on_connections_exit(static_proxy: NeonProxy, option_name: str):
    # Open two connections, send SIGTERM, then ensure that proxy doesn't exit
    # until after connections close.
    with static_proxy.connect(options=f"{option_name}=irrelevant"), static_proxy.connect(
        options=f"{option_name}=irrelevant"
    ):
        static_proxy.terminate()
        with pytest.raises(subprocess.TimeoutExpired):
            static_proxy.wait_for_exit(timeout=2)
        # Ensure we don't accept any more connections
        with pytest.raises(psycopg2.OperationalError):
            static_proxy.connect(options=f"{option_name}=irrelevant")
    static_proxy.wait_for_exit()
