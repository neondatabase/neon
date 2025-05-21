from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv, NeonEnvBuilder


def test_basebackup_error(env: NeonEnv):
    """
    Test error handling, if the 'basebackup' command fails in the middle
    of building the tar archive.
    """
    pageserver_http = env.pageserver.http_client()

    # Introduce failpoint
    pageserver_http.configure_failpoints(("basebackup-before-control-file", "return"))

    with pytest.raises(Exception, match="basebackup-before-control-file"):
        env.endpoints.create_start("main")


def test_basebackup_cache(neon_env_builder: NeonEnvBuilder):
    """
    
    """

    neon_env_builder.pageserver_config_override = """
        tenant_config = { basebackup_cache_enabled = true }
        basebackup_cache_config = { background_cleanup_period = '1s' }
    """
    
    env = neon_env_builder.init_start()
    ep = env.endpoints.create("main")
    ps = env.pageserver
    ps_http = ps.http_client()

    for i in range(3):
        ep.start()
        ep.stop()

        def check_metrics():
            metrics = ps_http.get_metrics()
            # Never miss.
            # The first time compute_ctl sends `get_basebackup` with lsn=None, we do not cache such requests.
            # All other requests should be a hit
            assert metrics.query_one("pageserver_basebackup_cache_read_total", {"result": "miss"}).value == 0
            # All but the first requests are hits.
            assert metrics.query_one("pageserver_basebackup_cache_read_total", {"result": "hit"}).value == i
            # Every compute shut down should trigger a prepare reuest.
            assert metrics.query_one("pageserver_basebackup_cache_prepare_total", {"result": "ok"}).value == i + 1

        wait_until(check_metrics)

    # Check that basebackup cache eventually deletes old backup files.
    def check_bb_file_count():
        bb_files = list(ps.workdir.joinpath("basebackup_cache").iterdir())
        assert len(bb_files) == 1

    wait_until(check_bb_file_count)

