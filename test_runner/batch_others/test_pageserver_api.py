import json

pytest_plugins = ("fixtures.zenith_fixtures")


def test_status(pageserver):
    assert pageserver.safe_psql('status') == [
        ('hello world', ),
    ]


def test_branch_list(pageserver, zenith_cli):
    # Create a branch for us
    zenith_cli.run(["branch", "test_branch_list_main", "empty"])

    conn = pageserver.connect()
    cur = conn.cursor()

    cur.execute('branch_list')
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

    cur.execute('branch_list')
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
