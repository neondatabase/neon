import pytest
import os

pytest_plugins = ("fixtures.zenith_fixtures")

"""

Use this test to see what happens when tests fail.

We should be able to clean up after ourselves, including stopping any
postgres or pageserver processes.

Set the environment variable RUN_BROKEN to see this test run (and fail,
and hopefully not leave any server processes behind).

"""


run_broken = pytest.mark.skipif(
    os.environ.get('RUN_BROKEN') == None,
    reason="only used for testing the fixtures"
)

@run_broken
def test_broken(zenith_cli, pageserver, postgres, pg_bin):
    zenith_cli.run_init()
    pageserver.start()
    print('pageserver is running')

    postgres.create_start()
    print('postgres is running')

    print('THIS NEXT COMMAND WILL FAIL:')
    pg_bin.run('pgbench -i_am_a_broken_test'.split())
