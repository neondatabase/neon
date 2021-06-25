from contextlib import closing
import json

pytest_plugins = ("fixtures.zenith_fixtures")


def test_push(zenith_cli, pageserver, postgres, standalone_pageserver):
    zenith_cli.run(['branch', 'test_push', 'empty'])

    pg = postgres.create_start('test_push')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE DATABASE foodb')

    # TODO push by name instead of timeline id
    with closing(pageserver.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('branch_list')
            branches = json.loads(cur.fetchone()[0])
    branches = {branch['name']: branch for branch in branches}
    timeline_id = bytearray(branches['test_push']['timeline_id']).hex()

    zenith_cli.run(['remote', 'add', 'standalone', 'postgresql://127.0.0.1:64001'])

    zenith_cli.run(['push', timeline_id, 'standalone'])

    # TODO verify the presence of test_push on the standalone pageserver
