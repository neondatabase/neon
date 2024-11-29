import random
import threading
import time

from fixtures.neon_fixtures import NeonEnvBuilder


def test_btree_split(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()

    n_records = 1000
    n_oltp_writers = 10
    n_oltp_readers = 5
    n_olap_readers = 1

    endpoint = env.endpoints.create_start("main", config_lines=["autovacuum=on"])

    cur = endpoint.connect().cursor()

    cur.execute("CREATE EXTENSION neon_test_utils")
    cur.execute("CREATE TABLE t(pk bigint not null)")
    cur.execute(f"insert into t values (generate_series(1,{n_records}))")
    cur.execute("create index on t(pk)")

    running = True

    def oltp_writer():
        cur = endpoint.connect().cursor()
        while running:
            pk = random.randrange(1, n_records)
            cur.execute(f"update t set pk={n_records-pk+1} where pk={pk}")

    def oltp_reader():
        cur = endpoint.connect().cursor()
        cur.execute("set effective_io_concurrency=0")
        cur.execute("set max_parallel_workers_per_gather=0")
        cur.execute("set enable_seqscan=off")
        cur.execute("set enable_indexscan=off")
        cur.execute("set enable_indexonlyscan_prefetch=off")
        while running:
            endpoint.clear_shared_buffers(cur)
            pk = random.randrange(1, n_records)
            cur.execute(f"select count(*) from t where pk between {pk} and {pk+1000}")

    def olap_reader():
        cur = endpoint.connect().cursor()
        cur.execute("set effective_io_concurrency=0")
        cur.execute("set max_parallel_workers_per_gather=0")
        cur.execute("set enable_seqscan=off")
        cur.execute("set enable_indexscan=off")
        cur.execute("set enable_indexonlyscan_prefetch=off")
        while running:
            endpoint.clear_shared_buffers(cur)
            cur.execute("select count(*) from t")
            count = cur.fetchall()[0][0]
            assert count == n_records

    oltp_writers = []
    for _ in range(n_oltp_writers):
        t = threading.Thread(target=oltp_writer)
        oltp_writers.append(t)
        t.start()

    oltp_readers = []
    for _ in range(n_oltp_readers):
        t = threading.Thread(target=oltp_reader)
        oltp_readers.append(t)
        t.start()

    olap_readers = []
    for _ in range(n_olap_readers):
        t = threading.Thread(target=olap_reader)
        olap_readers.append(t)
        t.start()

    time.sleep(100)
    running = False

    for t in oltp_writers:
        t.join()
    for t in oltp_readers:
        t.join()
    for t in olap_readers:
        t.join()
