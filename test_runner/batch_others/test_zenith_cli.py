import json
import uuid

from fixtures.zenith_fixtures import ZenithCli, ZenithPageserver
from typing import cast

pytest_plugins = ("fixtures.zenith_fixtures")


def helper_compare_branch_list(page_server_cur, zenith_cli: ZenithCli, initial_tenant: str):
    """
    Compare branches list returned by CLI and directly via API.
    Filters out branches created by other tests.
    """

    page_server_cur.execute(f'branch_list {initial_tenant}')
    branches_api = sorted(
        map(lambda b: cast(str, b['name']), json.loads(page_server_cur.fetchone()[0])))
    branches_api = [b for b in branches_api if b.startswith('test_cli_') or b in ('empty', 'main')]

    res = zenith_cli.run(["branch"])
    res.check_returncode()
    branches_cli = sorted(map(lambda b: b.split(':')[-1].strip(), res.stdout.strip().split("\n")))
    branches_cli = [b for b in branches_cli if b.startswith('test_cli_') or b in ('empty', 'main')]

    res = zenith_cli.run(["branch", f"--tenantid={initial_tenant}"])
    res.check_returncode()
    branches_cli_with_tenant_arg = sorted(
        map(lambda b: b.split(':')[-1].strip(), res.stdout.strip().split("\n")))
    branches_cli_with_tenant_arg = [
        b for b in branches_cli if b.startswith('test_cli_') or b in ('empty', 'main')
    ]

    assert branches_api == branches_cli == branches_cli_with_tenant_arg


def test_cli_branch_list(pageserver: ZenithPageserver, zenith_cli: ZenithCli):
    page_server_conn = pageserver.connect()
    page_server_cur = page_server_conn.cursor()

    # Initial sanity check
    helper_compare_branch_list(page_server_cur, zenith_cli, pageserver.initial_tenant)

    # Create a branch for us
    res = zenith_cli.run(["branch", "test_cli_branch_list_main", "main"])
    assert res.stderr == ''
    helper_compare_branch_list(page_server_cur, zenith_cli, pageserver.initial_tenant)

    # Create a nested branch
    res = zenith_cli.run(["branch", "test_cli_branch_list_nested", "test_cli_branch_list_main"])
    assert res.stderr == ''
    helper_compare_branch_list(page_server_cur, zenith_cli, pageserver.initial_tenant)

    # Check that all new branches are visible via CLI
    res = zenith_cli.run(["branch"])
    assert res.stderr == ''
    branches_cli = sorted(map(lambda b: b.split(':')[-1].strip(), res.stdout.strip().split("\n")))

    assert 'test_cli_branch_list_main' in branches_cli
    assert 'test_cli_branch_list_nested' in branches_cli


def helper_compare_tenant_list(page_server_cur, zenith_cli: ZenithCli):
    page_server_cur.execute(f'tenant_list')
    tenants_api = sorted(json.loads(page_server_cur.fetchone()[0]))

    res = zenith_cli.run(["tenant", "list"])
    assert res.stderr == ''
    tenants_cli = sorted(res.stdout.splitlines())

    assert tenants_api == tenants_cli


def test_cli_tenant_list(pageserver: ZenithPageserver, zenith_cli: ZenithCli):
    page_server_conn = pageserver.connect()
    page_server_cur = page_server_conn.cursor()

    # Initial sanity check
    helper_compare_tenant_list(page_server_cur, zenith_cli)

    # Create new tenant
    tenant1 = uuid.uuid4().hex
    res = zenith_cli.run(["tenant", "create", tenant1])
    res.check_returncode()

    # check tenant1 appeared
    helper_compare_tenant_list(page_server_cur, zenith_cli)

    # Create new tenant
    tenant2 = uuid.uuid4().hex
    res = zenith_cli.run(["tenant", "create", tenant2])
    res.check_returncode()

    # check tenant2 appeared
    helper_compare_tenant_list(page_server_cur, zenith_cli)

    res = zenith_cli.run(["tenant", "list"])
    res.check_returncode()
    tenants = sorted(res.stdout.splitlines())

    assert pageserver.initial_tenant in tenants
    assert tenant1 in tenants
    assert tenant2 in tenants
