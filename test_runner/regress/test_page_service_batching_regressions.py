# NB: there are benchmarks that double-serve as tests inside the `performance` directory.

import threading
import time

import requests.exceptions

import fixtures
from fixtures.common_types import NodeId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, StorageControllerApiException


def test_slow_flush(neon_env_builder: NeonEnvBuilder):
    tablesize_mib = 500

    def patch_pageserver_toml(config):
        config["page_service_pipelining"] = {
            "mode": "pipelined",
            "max_batch_size": 32,
            "execution": "concurrent-futures",
        }

    neon_env_builder.pageserver_config_override = patch_pageserver_toml
    neon_env_builder.num_pageservers = 2
    env = neon_env_builder.init_start()

    ep = env.endpoints.create_start(
        "main",
        config_lines=[
            "max_parallel_workers_per_gather=0",  # disable parallel backends
            "effective_io_concurrency=100",  # give plenty of opportunity for pipelining
            "neon.readahead_buffer_size=128",  # this is the default value at time of writing
            "shared_buffers=128MB",  # keep lower than tablesize_mib
            # debug
            "log_statement=all",
        ],
    )

    conn = ep.connect()
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS neon;")
    cur.execute("CREATE EXTENSION IF NOT EXISTS neon_test_utils;")

    log.info("Filling the table")
    cur.execute("CREATE TABLE t (data char(1000)) with (fillfactor=10)")
    tablesize = tablesize_mib * 1024 * 1024
    npages = tablesize // (8 * 1024)
    cur.execute("INSERT INTO t SELECT generate_series(1, %s)", (npages,))

    cur.close()
    conn.close()

    def workload(stop: threading.Event, max_iters=None):
        iters = 0
        while stop.is_set() is False and (max_iters == None or iters < max_iters):
            log.info("Seqscan %d", iters)
            conn = ep.connect()
            cur = conn.cursor()
            cur.execute(
                "select clear_buffer_cache()"
            )  # TODO: what about LFC? doesn't matter right now because LFC isn't enabled by default in tests
            cur.execute("select sum(data::bigint) from t")
            assert cur.fetchall()[0][0] == npages * (npages + 1) // 2
            iters += 1
        log.info("workload done")

    stop = threading.Event()

    log.info("calibrating workload duration")
    workload(stop, 1)
    before = time.time()
    workload(stop, 1)
    after = time.time()
    duration = after - before
    log.info("duration: %f", duration)
    assert(duration > 3)

    log.info("begin")
    threading.Thread(target=workload, args=[stop]).start()

    # make flush appear slow
    ps_http = [p.http_client() for p in env.pageservers]
    ps_http[0].configure_failpoints(("page_service:flush:pre", "return(10000000)"))
    ps_http[1].configure_failpoints(("page_service:flush:pre", "return(10000000)"))

    time.sleep(1)

    # try to shut down the tenant
    for i in range(1, 10):
        log.info(f"start migration {i}")
        try:
            env.storage_controller.tenant_shard_migrate(env.initial_tenant, (i % 2)+1)
        except StorageControllerApiException as e:
            log.info(f"shard migrate request failed: {e}")
            while True:
                node_id = NodeId(env.storage_controller.tenant_describe(env.initial_tenant)["node_id"])
                if node_id == NodeId(i % 2)+1:
                    break
                log.info(f"waiting for migration to complete")
                time.sleep(1)
        log.info(f"migration done")
        time.sleep(1)

