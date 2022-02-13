import pytest
import os

from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
"""
Use this test to see what happens when tests fail.

We should be able to clean up after ourselves, including stopping any
postgres or pageserver processes.

Set the environment variable RUN_BROKEN to see this test run (and fail,
and hopefully not leave any server processes behind).
"""

run_broken = pytest.mark.skipif(os.environ.get('RUN_BROKEN') is None,
                                reason="only used for testing the fixtures")


@run_broken
def test_broken(zenith_simple_env: ZenithEnv, pg_bin):
    env = zenith_simple_env

    new_timeline_id = env.zenith_cli.branch_timeline()
    env.postgres.create_start("test_broken", timeline_id=new_timeline_id)
    log.info('postgres is running')

    log.info('THIS NEXT COMMAND WILL FAIL:')
    pg_bin.run('pgbench -i_am_a_broken_test'.split())
