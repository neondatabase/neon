import pytest

from fixtures.neon_fixtures import NeonEnv


#
# Test error handling, if the 'basebackup' command fails in the middle
# of building the tar archive.
#
def test_basebackup_error(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_basebackup_error", "empty")

    # Introduce failpoint
    env.pageserver.safe_psql(f"failpoints basebackup-before-control-file=return")

    with pytest.raises(Exception, match="basebackup-before-control-file"):
        pg = env.postgres.create_start('test_basebackup_error')
