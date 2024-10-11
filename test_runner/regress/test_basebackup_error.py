from __future__ import annotations

import pytest
from fixtures.neon_fixtures import NeonEnv


#
# Test error handling, if the 'basebackup' command fails in the middle
# of building the tar archive.
#
def test_basebackup_error(neon_simple_env: NeonEnv):
    env = neon_simple_env
    pageserver_http = env.pageserver.http_client()

    # Introduce failpoint
    pageserver_http.configure_failpoints(("basebackup-before-control-file", "return"))

    with pytest.raises(Exception, match="basebackup-before-control-file"):
        env.endpoints.create_start("main")
