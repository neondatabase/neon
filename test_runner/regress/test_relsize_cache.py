import concurrent.futures
import time
from contextlib import closing
import random

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar

def test_relsize_cache(neon_simple_env: NeonEnv):
    """Stress tests the relsize cache in compute

    The test runs a few different workloads in parallel on the same
    table:
    * INSERTs
    * SELECT with seqscan
    * VACUUM

    The table is created with 100 indexes, to exercise the relation
    extension codepath as much as possible.

    At the same time, we run yet another thread which creates a new
    target table, and switches 'tblname' a global variable, so that
    all the other threads start to use that too. Sometimes (with 50%
    probability ), it also TRUNCATEs the old table after switching, so
    that the relsize "forget" function also gets exercised.

    This test was written to test a bug in locking of the relsize
    cache's LRU list, which lead to a corrupted LRU list, causing the
    effective size of the relsize cache to shrink to just a few
    entries over time as old entries were missing from the LRU list
    and thus "leaked", with the right workload. This is probably more
    complicated than necessary to reproduce that particular bug, but
    it gives a nice variety of concurrent activities on the relsize
    cache.
    """
    env = neon_simple_env
    env.neon_cli.create_branch("test_relsize_cache", "empty")

    endpoint = env.endpoints.create_start(
        "test_relsize_cache",
        config_lines=[
            # Make the relsize cache small, so that the LRU-based
            # eviction gets exercised
            "neon.relsize_hash_size=100",

            # Use a large shared buffers and LFC, so that it's not
            # slowed down by getpage requests to storage. They are not
            # interesting for this test, and we want as much
            # contention on the relsize cache as possible.
            "shared_buffers='1000 MB'",
            "neon.file_cache_path='file.cache'",
            "neon.max_file_cache_size=512MB",
            "neon.file_cache_size_limit=512MB",
        ],
    )

    conn = endpoint.connect()
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION amcheck")

    # Function to create the target table
    def create_tbl(wcur, new_tblname: str):
        wcur.execute(f"CREATE TABLE {new_tblname} (x bigint, y bigint, z bigint)")
        for i in range(0, 100):
            wcur.execute(f"CREATE INDEX relsize_test_idx_{new_tblname}_{i} ON {new_tblname} (x, y, z)")

    # create initial table
    tblname = "tbl_initial"
    create_tbl(cur, tblname)

    inserters_running = 0
    total_inserts = 0

    # XXX
    def insert_thread(id: int):
        nonlocal tblname, inserters_running, total_inserts
        log.info(f"i{id}: inserter thread started")
        with closing(endpoint.connect()) as wconn:
            with wconn.cursor() as wcur:

                wcur.execute("set synchronous_commit=off")

                for i in range(0, 100):
                    this_tblname = tblname
                    wcur.execute(
                        f"INSERT INTO {this_tblname} SELECT 1000000000*random(), g, g FROM generate_series(1, 100) g"
                    )
                    total_inserts += 100
                    log.info(f"i{id}: inserted to {this_tblname}")

        inserters_running -= 1
        log.info(f"inserter thread {id} finished!")

    # This thread periodically creates a new target table
    def switcher_thread():
        nonlocal tblname, inserters_running, total_inserts
        log.info("switcher thread started")
        wconn = endpoint.connect()
        wcur = wconn.cursor()

        tblcounter = 0
        while inserters_running > 0:
            time.sleep(0.01)
            old_tblname = tblname

            # Create a new target table and change the global 'tblname' variable to
            # switch to it
            tblcounter += 1
            new_tblname = f"tbl{tblcounter}"
            create_tbl(wcur, new_tblname)
            tblname = new_tblname

            # With 50% probability, also truncate the old table, to exercise the
            # relsize "forget" codepath too
            if random.random() < 0.5:
                wcur.execute(f"TRUNCATE {old_tblname}")

            # print a "progress repot"
            log.info(f"switched to {new_tblname} ({total_inserts} inserts done)")

    # Continuously run vacuum on the target table.
    #
    # Vacuum has the effect of invalidating the cached relation size in relcache
    def vacuum_thread():
        nonlocal tblname, inserters_running
        log.info("vacuum thread started")
        wconn = endpoint.connect()
        wcur = wconn.cursor()

        while inserters_running > 0:
            wcur.execute(f"vacuum {tblname}")

    # Continuously query the current target table
    #
    # This actually queries not just the latest target table, but a
    # few latest ones. This is implemented by only updating the target
    # table with 10% probability on each iteration. This gives a bit
    # more variability on the relsize entries that are requested from
    # the cache.
    def query_thread(id: int):
        nonlocal tblname, inserters_running
        log.info(f"q{id}: query thread started")
        wconn = endpoint.connect()
        wcur = wconn.cursor()
        wcur.execute("set max_parallel_workers_per_gather=0")

        this_tblname = tblname
        while inserters_running > 0:
            if random.random() < 0.1:
                this_tblname = tblname
            wcur.execute(f"select count(*) from {this_tblname}")

        log.info(f"q{id}: query thread finished!")
                        
    # With 'with', this waits for all the threads to finish
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = []

        # Launch all the threads
        f = executor.submit(switcher_thread)
        futures.append(f)
        f = executor.submit(vacuum_thread)
        futures.append(f)

        # 5 inserter threads
        for i in range(0, 5):
            f = executor.submit(insert_thread, i)
            futures.append(f)
            inserters_running += 1

        # 20 query threads
        for i in range(0, 20):
            f = executor.submit(query_thread, i)
            futures.append(f)

        for f in concurrent.futures.as_completed(futures):
            ex = f.exception()
            if ex:
                log.info(f"exception from thread, stopping: {ex}")
                inserters_running = 0 # abort the other threads
            f.result()

    # Finally, run amcheck on all the indexes. Most relsize cache bugs
    # would result in runtime ERRORs, but doesn't hurt to do more sanity
    # checking.
    cur.execute(f"select bt_index_check(oid, true) from pg_class where relname like 'relsize_test_idx%'")
