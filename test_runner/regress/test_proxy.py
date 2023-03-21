import psycopg2
import pytest
from fixtures.neon_fixtures import PSQL, NeonProxy, VanillaPostgres


def test_proxy_select_1(static_proxy: NeonProxy):
    """
    A simplest smoke test: check proxy against a local postgres instance.
    """

    out = static_proxy.safe_psql("select 1", options="project=generic-project-name")
    assert out[0][0] == 1


def test_password_hack(static_proxy: NeonProxy):
    """
    Check the PasswordHack auth flow: an alternative to SCRAM auth for
    clients which can't provide the project/endpoint name via SNI or `options`.
    """

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


def test_proxy_options(static_proxy: NeonProxy):
    """
    Check that we pass extra `options` to the PostgreSQL server:
    * `project=...` shouldn't be passed at all (otherwise postgres will raise an error).
    * everything else should be passed as-is.
    """

    options = "project=irrelevant -cproxytest.option=value"
    out = static_proxy.safe_psql("show proxytest.option", options=options)
    assert out[0][0] == "value"

    options = "-c proxytest.foo=\\ str project=irrelevant"
    out = static_proxy.safe_psql("show proxytest.foo", options=options)
    assert out[0][0] == " str"


def test_auth_errors(static_proxy: NeonProxy):
    """
    Check that we throw very specific errors in some unsuccessful auth scenarios.
    """

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


def test_forward_params_to_client(static_proxy: NeonProxy):
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

    with static_proxy.connect(options="project=irrelevant") as conn:
        with conn.cursor() as cur:
            cur.execute(query, (reported_params_subset,))
            for name, value in cur.fetchall():
                # Check that proxy has forwarded this parameter.
                assert conn.get_parameter_status(name) == value
