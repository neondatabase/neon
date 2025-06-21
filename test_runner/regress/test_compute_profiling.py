import threading

from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from google.protobuf.message import Message
from data import profile_pb2


def _start_profiling_cpu(client: EndpointHttpClient, event: threading.Event):
    """
    Start CPU profiling for the compute node.
    """
    log.info("Starting CPU profiling...")
    try:
        event.set()
        response = client.profile_cpu(100, 5, False)
        log.info("CPU profiling finished")
        profile = profile_pb2.Profile()
        Message.ParseFromString(profile, response)
        return profile
    except Exception as e:
        log.error(f"Error starting CPU profiling: {e}")
        raise


def test_compute_profiling_cpu(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create database profiling_test")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling():
        profile = _start_profiling_cpu(http_client, event)
        assert len(profile.sample) > 0, "No samples found in CPU profiling data"
        assert len(profile.mapping) > 0, "No mappings found in CPU profiling data"
        assert len(profile.location) > 0, "No locations found in CPU profiling data"
        assert len(profile.function) > 0, "No functions found in CPU profiling data"
        assert len(profile.string_table) > 0, "No string tables found in CPU profiling data"
        for string in [
            "PostgresMain",
            "ServerLoop",
            "BackgroundWorkerMain",
            "pq_recvbuf",
            "pq_getbyte",
        ]:
            assert string in profile.string_table, (
                f"Expected function '{string}' not found in string table"
            )

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling)
    thread.start()

    def insert_rows():
        event.wait()  # Wait for profiling to be ready to start
        lfc_conn = endpoint.connect(dbname="profiling_test")
        lfc_cur = lfc_conn.cursor()
        n_records = 10000
        log.info(f"Inserting {n_records} rows")
        lfc_cur.execute(
            "create table t(pk integer primary key, payload text default repeat('?', 128))"
        )
        lfc_cur.execute(f"insert into t (pk) values (generate_series(1,{n_records}))")
        log.info(f"Inserted {n_records} rows")

    thread2 = threading.Thread(target=insert_rows)
    thread2.start()

    thread.join(timeout=60)
    thread2.join(timeout=60)

    endpoint.stop()
    endpoint.start()
