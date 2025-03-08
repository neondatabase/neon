from __future__ import annotations

import random
import threading
import time

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import USE_LFC


@pytest.mark.timeout(1000)
@pytest.mark.skipif(not USE_LFC, reason="LFC is disabled, skipping")
def test_lfc_prefetch(neon_simple_env: NeonEnv):
    """
    Test resizing the Local File Cache
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "neon.max_file_cache_size=10MB",
            "neon.file_cache_size_limit=10MB",
            "effective_io_concurrency=100",
            "shared_buffers=1MB",
            "enable_bitmapscan=off",
            "enable_seqscan=off",
            "autovacuum=off",
            "statement_timeout=0",
            "neon.store_prefetch_result_in_lfc=on",
        ],
    )
    n_records = 10000  # 80Mb table
    top_n = n_records // 4  # 20Mb - should be larger than LFC size
    test_time = 100.0  # seconds
    n_readers = 10
    n_writers = 2

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
