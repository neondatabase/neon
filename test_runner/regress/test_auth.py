from __future__ import annotations

import os
from contextlib import closing
from pathlib import Path

import psycopg2
import pytest
from fixtures.common_types import TenantId, TimelineId
from fixtures.neon_fixtures import (
    NeonEnv,
    NeonEnvBuilder,
    PgProtocol,
)
from fixtures.pageserver.http import PageserverApiException, PageserverHttpClient


def assert_client_authorized(env: NeonEnv, http_client: PageserverHttpClient):
    http_client.timeline_create(
        pg_version=env.pg_version,
        tenant_id=env.initial_tenant,
        new_timeline_id=TimelineId.generate(),
        ancestor_timeline_id=env.initial_timeline,
    )


def assert_client_not_authorized(env: NeonEnv, http_client: PageserverHttpClient):
    with pytest.raises(
        PageserverApiException,
        match="Forbidden: JWT authentication error",
    ):
        assert_client_authorized(env, http_client)


def test_pageserver_auth(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()

    ps = env.pageserver

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant)
    tenant_http_client = env.pageserver.http_client(tenant_token)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(TenantId.generate())
    invalid_tenant_http_client = env.pageserver.http_client(invalid_tenant_token)

    pageserver_token = env.auth_keys.generate_pageserver_token()
    pageserver_http_client = env.pageserver.http_client(pageserver_token)

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("set FOO", password=tenant_token)
    ps.safe_psql("set FOO", password=pageserver_token)

    # tenant can create branches
    assert_client_authorized(env, tenant_http_client)

    # console can create branches for tenant
    assert_client_authorized(env, pageserver_http_client)

    # fail to create branch using token with different tenant_id
    with pytest.raises(PageserverApiException, match="Forbidden: JWT authentication error"):
        assert_client_authorized(env, invalid_tenant_http_client)

    # create tenant using management token
    env.pageserver.tenant_create(TenantId.generate(), auth_token=pageserver_token)

    # fail to create tenant using tenant token
    with pytest.raises(
        PageserverApiException,
        match="Forbidden: JWT authentication error",
    ):
        env.pageserver.tenant_create(TenantId.generate(), auth_token=tenant_token)


def test_compute_auth_to_pageserver(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    neon_env_builder.num_safekeepers = 3
    env = neon_env_builder.init_start()

    branch = "test_compute_auth_to_pageserver"
    env.create_branch(branch)
    endpoint = env.endpoints.create_start(branch)

    with closing(endpoint.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute("CREATE TABLE t(key int primary key, value text)")
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute("SELECT sum(key) FROM t")
            assert cur.fetchone() == (5000050000,)


def test_pageserver_multiple_keys(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [".*Authentication error: InvalidSignature.*", ".*Unauthorized: malformed jwt token.*"]
    )

    pageserver_token_old = env.auth_keys.generate_pageserver_token()
    pageserver_http_client_old = env.pageserver.http_client(pageserver_token_old)

    pageserver_http_client_old.reload_auth_validation_keys()

    # This test is to ensure that the pageserver supports multiple keys.
    # The neon_local tool generates one key pair at a hardcoded path by default.
    # As a preparation for our test, move the public key of the key pair into a
    # directory at the same location as the hardcoded path by:
    # 1. moving the file at `configured_pub_key_path` to a temporary location
    # 2. creating a new directory at `configured_pub_key_path`
    # 3. moving the file from the temporary location into the newly created directory
    configured_pub_key_path = Path(env.repo_dir) / "auth_public_key.pem"
    os.rename(configured_pub_key_path, Path(env.repo_dir) / "auth_public_key.pem.file")
    os.mkdir(configured_pub_key_path)
    os.rename(
        Path(env.repo_dir) / "auth_public_key.pem.file",
        configured_pub_key_path / "auth_public_key_old.pem",
    )

    # Add a new key pair
    # This invalidates env.auth_keys and makes them be regenerated
    env.regenerate_keys_at(
        Path("auth_private_key.pem"), Path("auth_public_key.pem/auth_public_key_new.pem")
    )

    # Reload the keys on the pageserver side
    pageserver_http_client_old.reload_auth_validation_keys()

    # We can continue doing things using the old token
    assert_client_authorized(env, pageserver_http_client_old)

    pageserver_token_new = env.auth_keys.generate_pageserver_token()
    pageserver_http_client_new = env.pageserver.http_client(pageserver_token_new)

    # The new token also works
    assert_client_authorized(env, pageserver_http_client_new)

    # Remove the old token and reload
    os.remove(Path(env.repo_dir) / "auth_public_key.pem" / "auth_public_key_old.pem")
    pageserver_http_client_old.reload_auth_validation_keys()

    # Reloading fails now with the old token, but the new token still works
    assert_client_not_authorized(env, pageserver_http_client_old)
    assert_client_authorized(env, pageserver_http_client_new)


def test_pageserver_key_reload(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.auth_enabled = True
    env = neon_env_builder.init_start()
    env.pageserver.allowed_errors.extend(
        [".*Authentication error: InvalidSignature.*", ".*Unauthorized: malformed jwt token.*"]
    )
    pageserver_token_old = env.auth_keys.generate_pageserver_token()
    pageserver_http_client_old = env.pageserver.http_client(pageserver_token_old)

    pageserver_http_client_old.reload_auth_validation_keys()

    # Regenerate the keys
    env.regenerate_keys_at(Path("auth_private_key.pem"), Path("auth_public_key.pem"))

    # Reload the keys on the pageserver side
    pageserver_http_client_old.reload_auth_validation_keys()

    # Next attempt fails as we use the old auth token
    with pytest.raises(
        PageserverApiException,
        match="Forbidden: JWT authentication error",
    ):
        pageserver_http_client_old.reload_auth_validation_keys()

    # same goes for attempts trying to create a timeline
    assert_client_not_authorized(env, pageserver_http_client_old)

    pageserver_token_new = env.auth_keys.generate_pageserver_token()
    pageserver_http_client_new = env.pageserver.http_client(pageserver_token_new)

    # timeline creation works with the new token
    assert_client_authorized(env, pageserver_http_client_new)

    # reloading also works with the new token
    pageserver_http_client_new.reload_auth_validation_keys()


@pytest.mark.parametrize("auth_enabled", [False, True])
def test_auth_failures(neon_env_builder: NeonEnvBuilder, auth_enabled: bool):
    neon_env_builder.auth_enabled = auth_enabled
    env = neon_env_builder.init_start()

    branch = f"test_auth_failures_auth_enabled_{auth_enabled}"
    timeline_id = env.create_branch(branch)
    env.endpoints.create_start(branch)

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(TenantId.generate())
    pageserver_token = env.auth_keys.generate_pageserver_token()
    safekeeper_token = env.auth_keys.generate_safekeeper_token()

    def check_connection(
        pg_protocol: PgProtocol, command: str, expect_success: bool, **conn_kwargs
    ):
        def op():
            with closing(pg_protocol.connect(**conn_kwargs)) as conn:
                with conn.cursor() as cur:
                    cur.execute(command)

        if expect_success:
            op()
        else:
            with pytest.raises(psycopg2.Error):
                op()

    def check_pageserver(expect_success: bool, **conn_kwargs):
        check_connection(
            env.pageserver,
            f"pagestream_v2 {env.initial_tenant} {env.initial_timeline}",
            expect_success,
            **conn_kwargs,
        )

    check_pageserver(not auth_enabled)
    if auth_enabled:
        check_pageserver(True, password=tenant_token)

        env.pageserver.allowed_errors.append(".*Tenant id mismatch. Permission denied.*")
        check_pageserver(False, password=invalid_tenant_token)

        check_pageserver(True, password=pageserver_token)

        env.pageserver.allowed_errors.append(".*JWT scope '.+' is ineligible for Pageserver auth.*")
        check_pageserver(False, password=safekeeper_token)

    def check_safekeeper(expect_success: bool, **conn_kwargs):
        check_connection(
            PgProtocol(
                host="localhost",
                port=env.safekeepers[0].port.pg,
                options=f"ztenantid={env.initial_tenant} ztimelineid={timeline_id}",
            ),
            "IDENTIFY_SYSTEM",
            expect_success,
            **conn_kwargs,
        )

    check_safekeeper(not auth_enabled)
    if auth_enabled:
        check_safekeeper(True, password=tenant_token)
        check_safekeeper(False, password=invalid_tenant_token)
        check_safekeeper(False, password=pageserver_token)
        check_safekeeper(True, password=safekeeper_token)
