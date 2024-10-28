from __future__ import annotations

import psycopg2
import pytest
from fixtures.neon_fixtures import (
    NeonProxy,
    VanillaPostgres,
)
from pytest_httpserver import HTTPServer

TABLE_NAME = "neon_control_plane.endpoints"


def test_proxy_psql_not_allowed_ips(
    static_proxy: NeonProxy,
    vanilla_pg: VanillaPostgres,
    httpserver: HTTPServer,
):
    [(rolpassword,)] = vanilla_pg.safe_psql(
        "select rolpassword from pg_catalog.pg_authid where rolname = 'proxy'"
    )

    # Shouldn't be able to connect to this project
    httpserver.expect_request("/cplane/proxy_get_role_secret").respond_with_json(
        {
            "role_secret": rolpassword,
            "allowed_ips": ["8.8.8.8"],
            "project_id": "foo-bar",
        }
    )

    def check_cannot_connect(**kwargs):
        with pytest.raises(psycopg2.Error) as exprinfo:
            static_proxy.safe_psql(**kwargs)
        text = str(exprinfo.value).strip()
        assert "not allowed to connect" in text

    # no SNI, deprecated `options=project` syntax (before we had several endpoint in project)
    check_cannot_connect(query="select 1", sslsni=0, options="project=private-project")

    # no SNI, new `options=endpoint` syntax
    check_cannot_connect(query="select 1", sslsni=0, options="endpoint=private-project")

    # with SNI
    check_cannot_connect(query="select 1", host="private-project.localtest.me")


def test_proxy_psql_allowed_ips(
    static_proxy: NeonProxy,
    vanilla_pg: VanillaPostgres,
    httpserver: HTTPServer,
):
    [(rolpassword,)] = vanilla_pg.safe_psql(
        "select rolpassword from pg_catalog.pg_authid where rolname = 'proxy'"
    )

    # Should be able to connect to this project
    httpserver.expect_request("/cplane/proxy_get_role_secret").respond_with_json(
        {
            "role_secret": rolpassword,
            "allowed_ips": ["::1", "127.0.0.1"],
            "project_id": "foo-bar",
        }
    )

    # no SNI, deprecated `options=project` syntax (before we had several endpoint in project)
    out = static_proxy.safe_psql(query="select 1", sslsni=0, options="project=generic-project")
    assert out[0][0] == 1

    # no SNI, new `options=endpoint` syntax
    out = static_proxy.safe_psql(query="select 1", sslsni=0, options="endpoint=generic-project")
    assert out[0][0] == 1

    # with SNI
    out = static_proxy.safe_psql(query="select 1", host="generic-project.localtest.me")
    assert out[0][0] == 1


def test_proxy_http_not_allowed_ips(
    static_proxy: NeonProxy,
    vanilla_pg: VanillaPostgres,
    httpserver: HTTPServer,
):
    vanilla_pg.safe_psql("create user http_auth with password 'http' superuser")

    [(rolpassword,)] = vanilla_pg.safe_psql(
        "select rolpassword from pg_catalog.pg_authid where rolname = 'http_auth'"
    )

    httpserver.expect_oneshot_request("/cplane/proxy_get_role_secret").respond_with_json(
        {
            "role_secret": rolpassword,
            "allowed_ips": ["8.8.8.8"],
            "project_id": "foo-bar",
        }
    )

    with httpserver.wait() as waiting:
        static_proxy.http_query(
            "select 1;",
            [],
            user="http_auth",
            password="http",
            expected_code=400,
        )
    assert waiting.result


def test_proxy_http_allowed_ips(
    static_proxy: NeonProxy,
    vanilla_pg: VanillaPostgres,
    httpserver: HTTPServer,
):
    vanilla_pg.safe_psql("create user http_auth with password 'http' superuser")

    [(rolpassword,)] = vanilla_pg.safe_psql(
        "select rolpassword from pg_catalog.pg_authid where rolname = 'http_auth'"
    )

    httpserver.expect_oneshot_request("/cplane/proxy_get_role_secret").respond_with_json(
        {
            "role_secret": rolpassword,
            "allowed_ips": ["8.8.8.8", "127.0.0.1"],
            "project_id": "foo-bar",
        }
    )

    with httpserver.wait() as waiting:
        static_proxy.http_query(
            "select 1;",
            [],
            user="http_auth",
            password="http",
            expected_code=200,
        )
    assert waiting.result
