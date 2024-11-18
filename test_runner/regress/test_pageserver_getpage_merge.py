
import psycopg2.extras
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.log_helper import log

def test_getpage_merge_smoke(neon_env_builder: NeonEnvBuilder):
    """
    Do a bunch of sequential scans and ensure that the pageserver does some merging.
    """

    def patch_config_toml(ps_cfg):
        ps_cfg["server_side_batch_timeout"] = "100ms"

    neon_env_builder.pageserver_config_override = patch_config_toml
    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main")

    with endpoint.connect() as conn:
        with conn.cursor() as cur:

            # cur.execute("SET max_parallel_workers_per_gather=0") # disable parallel backends
            cur.execute("SET effective_io_concurrency=100")
            # cur.execute("SET neon.readahead_buffer_size=128")
            # cur.execute("SET neon.flush_output_after=1")

            cur.execute("CREATE EXTENSION IF NOT EXISTS neon;")
            cur.execute("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")

            log.info("Filling the table")
            cur.execute("CREATE TABLE t (data char(1000)) with (fillfactor=10)")
            cur.execute("INSERT INTO t SELECT generate_series(1, 1024)")
            # TODO: can we force postgres to doe sequential scans?

            def get_metrics():
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("select value from neon_perf_counters where metric='getpage_wait_seconds_count';")
                    compute = cur.fetchall()

                    pageserver_metrics  = ps_http.get_metrics()
                    pageserver = {
                        "getpage_count": pageserver_metrics.query_one("pageserver_smgr_query_seconds_count", {"smgr_query_type": "get_page_at_lsn"}),
                        "vectored_get_count": pageserver_metrics.query_one("pageserver_get_vectored_seconds_count", {"task_kind": "PageRequestHandler"}),
                    }
                    return {
                        "pageserver": pageserver,
                        "compute": compute
                    }

            log.info("Doing bunch of seqscans")

            for i in range(4):
                log.info("Seqscan %d", i)
                if i == 1:
                    # round zero for warming up all the metrics
                    before = get_metrics()
                cur.execute("select clear_buffer_cache()") # TODO: what about LFC? doesn't matter right now because LFC isn't enabled by default in tests
                cur.execute("select sum(data::bigint) from t")
                assert cur.fetchall()[0][0] == sum(range(1, 1024 + 1))

            after = get_metrics()


    import pdb; pdb.set_trace()
    # TODO: assert that getpage counts roughly match between compute and ps
    # TODO: assert that batching occurs by asserting that vectored get count is siginificantly less than getpage count


