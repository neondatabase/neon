from __future__ import annotations

from typing import TYPE_CHECKING

from fixtures.utils import wait_until

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnvBuilder


def test_basebackup_cache(neon_env_builder: NeonEnvBuilder):
    """
    Simple test for basebackup cache.
    1. Check that we always hit the cache after compute restart.
    2. Check that we eventually delete old basebackup files, but not the latest one.
    3. Check that we delete basebackup file for timeline with active compute.
    """

    neon_env_builder.pageserver_config_override = """
        tenant_config = { basebackup_cache_enabled = true }
        basebackup_cache_config = { cleanup_period = '1s' }
    """

    env = neon_env_builder.init_start()
    ep = env.endpoints.create("main")
    ps = env.pageserver
    ps_http = ps.http_client()

    storcon_managed_timelines = (env.storage_controller_config or {}).get(
        "timelines_onto_safekeepers", False
    )

    # 1. Check that we always hit the cache after compute restart.
    for i in range(3):
        ep.start()
        ep.stop()

        def check_metrics(i=i):
            metrics = ps_http.get_metrics()
            if storcon_managed_timelines:
                # We do not cache the initial basebackup yet,
                # so the first compute startup should be a miss.
                assert (
                    metrics.query_one(
                        "pageserver_basebackup_cache_read_total", {"result": "miss"}
                    ).value
                    == 1
                )
            else:
                # If the timeline is not initialized on safekeeprs,
                # the compute_ctl sends `get_basebackup` with lsn=None for the first startup.
                # We do not use cache for such requests, so it's niether a hit nor a miss.
                assert (
                    metrics.query_one(
                        "pageserver_basebackup_cache_read_total", {"result": "miss"}
                    ).value
                    == 0
                )

            # All but the first requests are hits.
            assert (
                metrics.query_one("pageserver_basebackup_cache_read_total", {"result": "hit"}).value
                == i
            )
            # Every compute shut down should trigger a prepare reuest.
            assert (
                metrics.query_one(
                    "pageserver_basebackup_cache_prepare_total", {"result": "ok"}
                ).value
                == i + 1
            )
            # There should be only one basebackup file in the cache.
            assert metrics.query_one("pageserver_basebackup_cache_entries_total").value == 1
            # The size of one basebackup for new DB is ~20KB.
            size_bytes = metrics.query_one("pageserver_basebackup_cache_size_bytes").value
            assert 10 * 1024 <= size_bytes <= 100 * 1024

        wait_until(check_metrics)

    # 2. Check that we eventually delete old basebackup files, but not the latest one.
    def check_bb_file_count():
        bb_files = list(ps.workdir.joinpath("basebackup_cache").iterdir())
        # tmp dir + 1 basebackup file.
        assert len(bb_files) == 2

    wait_until(check_bb_file_count)

    # 3. Check that we delete basebackup file for timeline with active compute.
    ep.start()
    ep.safe_psql("create table t1 as select generate_series(1, 10) as n")

    def check_bb_dir_empty():
        bb_files = list(ps.workdir.joinpath("basebackup_cache").iterdir())
        # only tmp dir.
        assert len(bb_files) == 1

    wait_until(check_bb_dir_empty)
