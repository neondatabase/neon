from __future__ import annotations

import random
import threading
import time
import timeit
from typing import TYPE_CHECKING

import pytest
from fixtures.log_helper import log

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC


@pytest.mark.remote_cluster
@pytest.mark.timeout(100000)
@pytest.mark.parametrize("n_readers", [1, 2, 4, 8])
@pytest.mark.parametrize("n_writers", [0, 1, 2, 4, 8])
@pytest.mark.parametrize("chunk_size", [1, 8, 16])
@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_prefetch(neon_simple_env: NeonEnv, n_readers: int, n_writers: int, chunk_size: int):
    """
    Test prefetch under different kinds of workload
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=100MB",
            "neon.file_cache_size_limit=100MB",
            "effective_io_concurrency=100",
            "shared_buffers=128MB",
            "enable_bitmapscan=off",
            "enable_seqscan=off",
            f"neon.file_cache_chunk_size={chunk_size}",
            "neon.store_prefetch_result_in_lfc=on",
        ],
    )
    n_records = 100000  # 800Mb table
    top_n = n_records // 4  # 200Mb - should be larger than LFC size
    test_time = 100.0  # seconds

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute(
        "create table account(id integer primary key, balance integer default 0, filler text default repeat('?',1000)) with (fillfactor=10)"
    )
    cur.execute(f"insert into account values (generate_series(1,{n_records}))")
    cur.execute("vacuum account")

    def reader():
        conn = endpoint.connect()
        cur = conn.cursor()
        i = 0
        cur.execute("set statement_timeout=0")
        while running:
            cur.execute(
                f"select sum(balance) from (select balance from account order by id limit {top_n}) s"
            )
            sum = cur.fetchall()[0][0]
            assert sum == 0  # check consistency
            i += 1
        log.info(f"Did {i} index scans")

    def writer():
        conn = endpoint.connect()
        cur = conn.cursor()
        i = 0
        cur.execute("set statement_timeout=0")
        while running:
            r1 = random.randint(1, top_n)
            r2 = random.randint(1, top_n)
            # avoid deadlock by ordering src and dst
            src = min(r1, r2)
            dst = max(r1, r2)
            cur.execute(
                f"update account set balance=balance-1 where id={src}; update account set balance=balance+1 where id={dst}"
            )
            i += 1
        log.info(f"Did {i} updates")

    readers = [threading.Thread(target=reader) for _ in range(n_readers)]
    writers = [threading.Thread(target=writer) for _ in range(n_writers)]

    running = True
    for t in readers:
        t.start()
    for t in writers:
        t.start()

    time.sleep(test_time)
    running = False
    for t in readers:
        t.join()
    for t in writers:
        t.join()


@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_async_prefetch_performance(neon_simple_env: NeonEnv, zenbenchmark):
    """
    Demonstrate performance advantages of storing prefetch results in LFC
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=100MB",
            "neon.file_cache_size_limit=100MB",
            "effective_io_concurrency=100",
            "shared_buffers=1MB",
            "enable_bitmapscan=off",
            "enable_seqscan=off",
            "autovacuum=off",
        ],
    )
    n_records = 100000  # 800Mb table
    n_iterations = 1000

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute(
        "create table account(id integer primary key, balance integer default 0, filler text default repeat('?',1000)) with (fillfactor=10)"
    )
    cur.execute(f"insert into account values (generate_series(1,{n_records}))")
    cur.execute("vacuum account")

    start = timeit.default_timer()
    with zenbenchmark.record_duration("do_not_store_prefetch_results"):
        cur.execute("set neon.store_prefetch_result_in_lfc=off")
        for _ in range(n_iterations):
            cur.execute(
                "select sum(balance) from (select balance from account where id between 1000 and 2000 limit 100) s"
            )
            cur.execute(
                "select sum(balance) from (select balance from account where id between 6000 and 7000 limit 100) s"
            )
    end = timeit.default_timer()
    do_not_store_prefetch_results_duration = end - start

    start = timeit.default_timer()
    with zenbenchmark.record_duration("store_prefetch_results"):
        cur.execute("set neon.store_prefetch_result_in_lfc=on")
        for _ in range(n_iterations):
            cur.execute(
                "select sum(balance) from (select balance from account where id between 1000 and 2000 limit 100) s"
            )
            cur.execute(
                "select sum(balance) from (select balance from account where id between 6000 and 7000 limit 100) s"
            )
    end = timeit.default_timer()
    store_prefetch_results_duration = end - start

    assert do_not_store_prefetch_results_duration >= store_prefetch_results_duration
