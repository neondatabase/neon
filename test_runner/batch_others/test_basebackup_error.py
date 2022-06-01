import pytest
from contextlib import closing

from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


#
# Test error handling, if the 'basebackup' command fails in the middle
# of building the tar archive.
#
def test_basebackup_error(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli.create_branch("test_basebackup_error", "empty")

    # Introduce failpoint
    env.pageserver.safe_psql(f"failpoints basebackup-before-control-file=return")

    with pytest.raises(Exception, match="basebackup-before-control-file"):
        pg = env.postgres.create_start('test_basebackup_error')
