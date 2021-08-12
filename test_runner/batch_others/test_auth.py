
from contextlib import closing
from pathlib import Path
from uuid import uuid4
from dataclasses import dataclass
import jwt
import psycopg2
from fixtures.zenith_fixtures import Postgres, ZenithCli, ZenithPageserver
import pytest


@pytest.fixture
def pageserver_auth_enabled(zenith_cli: ZenithCli):
    with ZenithPageserver(zenith_cli).init(enable_auth=True).start() as ps:
        # For convenience in tests, create a branch from the freshly-initialized cluster.
        zenith_cli.run(["branch", "empty", "main"])
        yield ps


@dataclass
class AuthKeys:
    pub: bytes
    priv: bytes

    def generate_management_token(self):
        token = jwt.encode({"scope": "pageserverapi"}, self.priv, algorithm="RS256")

        # jwt.encode can return 'bytes' or 'str', depending on Python version or type
        # hinting or something (not sure what). If it returned 'bytes', convert it to 'str'
        # explicitly.
        if isinstance(token, bytes):
            token = token.decode()

        return token

    def generate_tenant_token(self, tenant_id):
        token = jwt.encode({"scope": "tenant", "tenant_id": tenant_id}, self.priv, algorithm="RS256")

        if isinstance(token, bytes):
            token = token.decode()

        return token


@pytest.fixture
def auth_keys(repo_dir: str) -> AuthKeys:
    # TODO probably this should be specified in cli config and used in tests for single source of truth
    pub = (Path(repo_dir) / 'auth_public_key.pem').read_bytes()
    priv = (Path(repo_dir) / 'auth_private_key.pem').read_bytes()
    return AuthKeys(pub=pub, priv=priv)


def test_pageserver_auth(pageserver_auth_enabled: ZenithPageserver, auth_keys: AuthKeys):
    ps = pageserver_auth_enabled

    tenant_token = auth_keys.generate_tenant_token(ps.initial_tenant)
    invalid_tenant_token = auth_keys.generate_tenant_token(uuid4().hex)
    management_token = auth_keys.generate_management_token()

    # this does not invoke auth check and only decodes jwt and checks it for validity
    # check both tokens
    ps.safe_psql("status", password=tenant_token)
    ps.safe_psql("status", password=management_token)

    # tenant can create branches
    ps.safe_psql(f"branch_create {ps.initial_tenant} new1 main", password=tenant_token)
    # console can create branches for tenant
    ps.safe_psql(f"branch_create {ps.initial_tenant} new2 main", password=management_token)

    # fail to create branch using token with different tenantid
    with pytest.raises(psycopg2.DatabaseError, match='Tenant id mismatch. Permission denied'):
        ps.safe_psql(f"branch_create {ps.initial_tenant} new2 main", password=invalid_tenant_token)

    # create tenant using management token
    ps.safe_psql(f"tenant_create {uuid4().hex}", password=management_token)

    # fail to create tenant using tenant token
    with pytest.raises(psycopg2.DatabaseError, match='Attempt to access management api with tenant scope. Permission denied'):
        ps.safe_psql(f"tenant_create {uuid4().hex}", password=tenant_token)


@pytest.mark.parametrize('with_wal_acceptors', [False, True])
def test_compute_auth_to_pageserver(
    zenith_cli: ZenithCli,
    wa_factory,
    pageserver_auth_enabled: ZenithPageserver,
    repo_dir: str,
    with_wal_acceptors: bool,
    auth_keys: AuthKeys,
):
    ps = pageserver_auth_enabled
    # since we are in progress of refactoring protocols between compute safekeeper and page server
    # use hardcoded management token in safekeeper
    management_token = auth_keys.generate_management_token()

    branch = f"test_compute_auth_to_pageserver{with_wal_acceptors}"
    zenith_cli.run(["branch", branch, "empty"])
    if with_wal_acceptors:
        wa_factory.start_n_new(3, management_token)

    with Postgres(
        zenith_cli=zenith_cli,
        repo_dir=repo_dir,
        tenant_id=ps.initial_tenant,
        port=55432, # FIXME port distribution is hardcoded in tests and in cli
    ).create_start(
        branch,
        wal_acceptors=wa_factory.get_connstrs() if with_wal_acceptors else None,
    ) as pg:
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                # we rely upon autocommit after each statement
                # as waiting for acceptors happens there
                cur.execute('CREATE TABLE t(key int primary key, value text)')
                cur.execute("INSERT INTO t SELECT generate_series(1,100000), 'payload'")
                cur.execute('SELECT sum(key) FROM t')
                assert cur.fetchone() == (5000050000, )
