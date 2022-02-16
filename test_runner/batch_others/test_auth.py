from contextlib import closing
from typing import Iterator
from uuid import uuid4
import psycopg2
from fixtures.zenith_fixtures import ZenithEnvBuilder
import pytest


def test_pageserver_auth(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.pageserver_auth_enabled = True
    env = zenith_env_builder.init()

    ps = env.pageserver

    tenant_token = env.auth_keys.generate_tenant_token(env.initial_tenant)
    invalid_tenant_token = env.auth_keys.generate_tenant_token(uuid4().hex)
    management_token = env.auth_keys.generate_management_token()

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("status", password=tenant_token)
    ps.safe_psql("status", password=management_token)

    # tenant can create branches
    ps.safe_psql(f"branch_create {env.initial_tenant} new1 main", password=tenant_token)
    # console can create branches for tenant
    ps.safe_psql(f"branch_create {env.initial_tenant} new2 main", password=management_token)

    # fail to create branch using token with different tenantid
    with pytest.raises(psycopg2.DatabaseError, match='Tenant id mismatch. Permission denied'):
        ps.safe_psql(f"branch_create {env.initial_tenant} new2 main", password=invalid_tenant_token)

    # create tenant using management token
    ps.safe_psql(f"tenant_create {uuid4().hex}", password=management_token)

    # fail to create tenant using tenant token
    with pytest.raises(
            psycopg2.DatabaseError,
            match='Attempt to access management api with tenant scope. Permission denied'):
        ps.safe_psql(f"tenant_create {uuid4().hex}", password=tenant_token)


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_compute_auth_to_pageserver(zenith_env_builder: ZenithEnvBuilder, with_wal_acceptors: bool):
    zenith_env_builder.pageserver_auth_enabled = True
    if with_wal_acceptors:
        zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    branch = f"test_compute_auth_to_pageserver{with_wal_acceptors}"
    env.zenith_cli(["branch", branch, "main"])

    pg = env.postgres.create_start(branch)

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            # we rely upon autocommit after each statement
            # as waiting for acceptors happens there
            cur.execute('CREATE TABLE t(key int primary key, value text)')
            cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
            cur.execute('SELECT sum(key) FROM t')
            assert cur.fetchone() == (5000050000, )
