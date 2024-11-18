
from dataclasses import dataclass
import time

import pytest
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.log_helper import log

@pytest.mark.parametrize("tablesize_mib", [50, 500])
@pytest.mark.parametrize("batch_timeout", ["10us", "100us", "1ms"])
@pytest.mark.parametrize("target_runtime", [30])
def test_getpage_merge_smoke(neon_env_builder: NeonEnvBuilder, tablesize_mib: int, batch_timeout: str, target_runtime: int):
    """
    Do a bunch of sequential scans and ensure that the pageserver does some merging.
    """

    env = neon_env_builder.init_start()

    ps_http = env.pageserver.http_client()

    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    #
    # Setup
    #
    cur.execute("CREATE EXTENSION IF NOT EXISTS neon;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")

    log.info("Filling the table")
    cur.execute("CREATE TABLE t (data char(1000)) with (fillfactor=10)")
    tablesize = tablesize_mib * 1024 * 1024
    npages = tablesize // (8*1024)
    cur.execute("INSERT INTO t SELECT generate_series(1, %s)", (npages,))
    # TODO: can we force postgres to doe sequential scans?

    #
    # Collect Data
    #

    @dataclass
    class Metrics:
        time: float
        pageserver_getpage_count: int
        pageserver_vectored_get_count: int
        compute_getpage_count: int

        def __sub__(self, other):
            return Metrics(
                time=self.time - other.time,
                pageserver_getpage_count=self.pageserver_getpage_count - other.pageserver_getpage_count,
                pageserver_vectored_get_count=self.pageserver_vectored_get_count - other.pageserver_vectored_get_count,
                compute_getpage_count=self.compute_getpage_count - other.compute_getpage_count
            )

        def __truediv__(self, other: "Metrics"):
            return Metrics(
                time=self.time / other.time, # doesn't really make sense but whatever
                pageserver_getpage_count=self.pageserver_getpage_count / other.pageserver_getpage_count,
                pageserver_vectored_get_count=self.pageserver_vectored_get_count / other.pageserver_vectored_get_count,
                compute_getpage_count=self.compute_getpage_count / other.compute_getpage_count
            )

        def normalize(self, by):
            return Metrics(
                time=self.time / by,
                pageserver_getpage_count=self.pageserver_getpage_count / by,
                pageserver_vectored_get_count=self.pageserver_vectored_get_count / by,
                compute_getpage_count=self.compute_getpage_count / by
            )

    def get_metrics():
        with conn.cursor() as cur:
            cur.execute("select value from neon_perf_counters where metric='getpage_wait_seconds_count';")
            compute_getpage_count = cur.fetchall()[0][0]
            pageserver_metrics = ps_http.get_metrics()
            return Metrics(
                time=time.time(),
                pageserver_getpage_count=pageserver_metrics.query_one("pageserver_smgr_query_seconds_count", {"smgr_query_type": "get_page_at_lsn"}).value,
                pageserver_vectored_get_count=pageserver_metrics.query_one("pageserver_get_vectored_seconds_count", {"task_kind": "PageRequestHandler"}).value,
                compute_getpage_count=compute_getpage_count
            )

    @dataclass
    class Result:
        metrics: Metrics
        iters: int

        @property
        def normalized(self) -> Metrics:
            return self.metrics.normalize(self.iters)
    def workload() -> Result:

        cur.execute("SET max_parallel_workers_per_gather=0") # disable parallel backends
        cur.execute("SET effective_io_concurrency=100")
        cur.execute("SET neon.readahead_buffer_size=128")
        # cur.execute("SET neon.flush_output_after=1")

        start = time.time()
        iters = 0
        while time.time() - start < target_runtime or iters < 2:
            log.info("Seqscan %d", iters)
            if iters == 1:
                # round zero for warming up
                before = get_metrics()
            cur.execute("select clear_buffer_cache()") # TODO: what about LFC? doesn't matter right now because LFC isn't enabled by default in tests
            cur.execute("select sum(data::bigint) from t")
            assert cur.fetchall()[0][0] == npages*(npages+1)//2
            iters += 1
        after = get_metrics()
        return Result(metrics=after-before, iters=iters)

    log.info("workload without merge")
    env.pageserver.restart() # reset the metrics
    without_merge = workload()

    log.info("workload with merge")
    env.pageserver.patch_config_toml_nonrecursive({"server_side_batch_timeout": batch_timeout})
    env.pageserver.restart()
    with_merge = workload()

    results = {
        "baseline": without_merge.normalized,
        "candiate": with_merge.normalized,
        "delta": with_merge.normalized - without_merge.normalized,
        "relative": with_merge.normalized / without_merge.normalized
    }

    #
    # Assertions on collected data
    #

    import pdb; pdb.set_trace()
    # TODO: assert that getpage counts roughly match between compute and ps
    # TODO: assert that batching occurs by asserting that vectored get count is siginificantly less than getpage count


