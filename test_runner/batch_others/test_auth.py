from contextlib import closing
from typing import Iterator
from uuid import UUID, uuid4
from fixtures.zenith_fixtures import ZenithEnvBuilder, ZenithPageserverApiException
from requests.exceptions import HTTPError
import pytest


def test_pageserver_auth(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.pageserver_auth_enabled = True
    env = zenith_env_builder.init_start()

    ps = env.pageserver

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant.hex)
    tenant_http_client = env.pageserver.http_client(tenant_token)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(uuid4().hex)
    invalid_tenant_http_client = env.pageserver.http_client(invalid_tenant_token)

    management_token = env.auth_keys.generate_management_token()
    management_http_client = env.pageserver.http_client(management_token)

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("set FOO", password=tenant_token)
    ps.safe_psql("set FOO", password=management_token)

    new_timeline_id = env.zenith_cli.create_branch('test_pageserver_auth',
                                                   tenant_id=env.initial_tenant)

    # tenant can create branches
    tenant_http_client.timeline_create(tenant_id=env.initial_tenant,
                                       ancestor_timeline_id=new_timeline_id)
    # console can create branches for tenant
    management_http_client.timeline_create(tenant_id=env.initial_tenant,
                                           ancestor_timeline_id=new_timeline_id)

    # fail to create branch using token with different tenant_id
    with pytest.raises(ZenithPageserverApiException,
                       match='Forbidden: Tenant id mismatch. Permission denied'):
        invalid_tenant_http_client.timeline_create(tenant_id=env.initial_tenant,
                                                   ancestor_timeline_id=new_timeline_id)

    # create tenant using management token
    management_http_client.tenant_create()

    # fail to create tenant using tenant token
    with pytest.raises(
            ZenithPageserverApiException,
            match='Forbidden: Attempt to access management api with tenant scope. Permission denied'
    ):
        tenant_http_client.tenant_create()


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_compute_auth_to_pageserver(zenith_env_builder: ZenithEnvBuilder, with_wal_acceptors: bool):
    zenith_env_builder.pageserver_auth_enabled = True
    if with_wal_acceptors:
        zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    branch = f'test_compute_auth_to_pageserver{with_wal_acceptors}'
    env.zenith_cli.create_branch(branch)
    pg = env.postgres.create_start(branch)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )
