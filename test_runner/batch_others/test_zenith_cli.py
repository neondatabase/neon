import psycopg2
import json

pytest_plugins = ("fixtures.zenith_fixtures")


def helper_compare_branch_list(page_server_cur, zenith_cli):
    """
    Compare branches list returned by CLI and directly via API.
    Filters out branches created by other tests.
    """

    page_server_cur.execute('branch_list')
    branches_api = sorted(map(lambda b: b['name'], json.loads(page_server_cur.fetchone()[0])))
    branches_api = [b for b in branches_api if b.startswith('test_cli_') or b in ('empty', 'main')]

    res = zenith_cli.run(["branch"])
    assert res.stderr == ''
    branches_cli = sorted(map(lambda b: b.split(':')[-1].strip(), res.stdout.strip().split("\n")))
    branches_cli = [b for b in branches_cli if b.startswith('test_cli_') or b in ('empty', 'main')]

    assert branches_api == branches_cli


def test_cli_branch_list(pageserver, zenith_cli):

    page_server_conn = psycopg2.connect(pageserver.connstr())
    page_server_conn.autocommit = True
    page_server_cur = page_server_conn.cursor()

    # Initial sanity check
    helper_compare_branch_list(page_server_cur, zenith_cli)

    # Create a branch for us
    res = zenith_cli.run(["branch", "test_cli_branch_list_main", "main"])
    assert res.stderr == ''
    helper_compare_branch_list(page_server_cur, zenith_cli)

    # Create a nested branch
    res = zenith_cli.run(["branch", "test_cli_branch_list_nested", "test_cli_branch_list_main"])
    assert res.stderr == ''
    helper_compare_branch_list(page_server_cur, zenith_cli)

    # Check that all new branches are visible via CLI
    res = zenith_cli.run(["branch"])
    assert res.stderr == ''
    branches_cli = sorted(map(lambda b: b.split(':')[-1].strip(), res.stdout.strip().split("\n")))

    assert 'test_cli_branch_list_main' in branches_cli
    assert 'test_cli_branch_list_nested' in branches_cli
