import json
from uuid import uuid4
import pytest
import psycopg2
import requests
from fixtures.zenith_fixtures import ZenithCli, ZenithPageserver, ZenithPageserverHttpClient
from typing import cast

pytest_plugins = ("fixtures.zenith_fixtures")


def test_status_psql(pageserver):
    assert pageserver.safe_psql('status') == [
        ('hello world', ),
    ]


def test_branch_list_psql(pageserver: ZenithPageserver, zenith_cli):
    # Create a branch for us
    zenith_cli.run(["branch", "test_branch_list_main", "empty"])

    conn = pageserver.connect()
    cur = conn.cursor()

    cur.execute(f'branch_list {pageserver.initial_tenant}')
    branches = json.loads(cur.fetchone()[0])
    # Filter out branches created by other tests
    branches = [x for x in branches if x['name'].startswith('test_branch_list')]

    assert len(branches) == 1
    assert branches[0]['name'] == 'test_branch_list_main'
    assert 'timeline_id' in branches[0]
    assert 'latest_valid_lsn' in branches[0]
    assert 'ancestor_id' in branches[0]
    assert 'ancestor_lsn' in branches[0]

    # Create another branch, and start Postgres on it
    zenith_cli.run(['branch', 'test_branch_list_experimental', 'test_branch_list_main'])
    zenith_cli.run(['pg', 'create', 'test_branch_list_experimental'])

    cur.execute(f'branch_list {pageserver.initial_tenant}')
    new_branches = json.loads(cur.fetchone()[0])
    # Filter out branches created by other tests
    new_branches = [x for x in new_branches if x['name'].startswith('test_branch_list')]
    assert len(new_branches) == 2
    new_branches.sort(key=lambda k: k['name'])

    assert new_branches[0]['name'] == 'test_branch_list_experimental'
    assert new_branches[0]['timeline_id'] != branches[0]['timeline_id']

    # TODO: do the LSNs have to match here?
    assert new_branches[1] == branches[0]

    conn.close()


def test_tenant_list_psql(pageserver: ZenithPageserver, zenith_cli: ZenithCli):
    res = zenith_cli.run(["tenant", "list"])
    res.check_returncode()
    tenants = sorted(map(lambda t: t.split()[0], res.stdout.splitlines()))
    assert tenants == [pageserver.initial_tenant]

    conn = pageserver.connect()
    cur = conn.cursor()

    # check same tenant cannot be created twice
    with pytest.raises(psycopg2.DatabaseError,
                       match=f'tenant {pageserver.initial_tenant} already exists'):
        cur.execute(f'tenant_create {pageserver.initial_tenant}')

    # create one more tenant
    tenant1 = uuid4().hex
    cur.execute(f'tenant_create {tenant1}')

    cur.execute('tenant_list')

    # compare tenants list
    new_tenants = sorted(map(lambda t: cast(str, t['id']), json.loads(cur.fetchone()[0])))
    assert sorted([pageserver.initial_tenant, tenant1]) == new_tenants


def check_client(client: ZenithPageserverHttpClient, initial_tenant: str):
    client.check_status()

    # check initial tenant is there
    assert initial_tenant in {t['id'] for t in client.tenant_list()}

    # create new tenant and check it is also there
    tenant_id = uuid4()
    client.tenant_create(tenant_id)
    assert tenant_id.hex in {t['id'] for t in client.tenant_list()}

    # create branch
    branch_name = uuid4().hex
    client.branch_create(tenant_id, branch_name, "main")

    # check it is there
    assert branch_name in {b['name'] for b in client.branch_list(tenant_id)}


def test_pageserver_http_api_client(pageserver: ZenithPageserver):
    client = pageserver.http_client()
    check_client(client, pageserver.initial_tenant)


def test_pageserver_http_api_client_auth_enabled(pageserver_auth_enabled: ZenithPageserver):
    client = pageserver_auth_enabled.http_client(
        auth_token=pageserver_auth_enabled.auth_keys.generate_management_token())
    check_client(client, pageserver_auth_enabled.initial_tenant)
