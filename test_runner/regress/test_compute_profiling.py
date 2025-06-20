import threading

from fixtures.endpoint.http import EndpointHttpClient
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv
from google.protobuf.message import Message


def load_profile_pb2():
    import base64
    import hashlib
    import importlib.util
    import os
    import tempfile

    import requests

    # URL to profile_pb2.py
    PROFILE_PB2_URL = "https://android.googlesource.com/platform/prebuilts/simpleperf/+/6d625d0eb9c0602532a52e8eb87363f2ea6da73e/profile_pb2.py?format=TEXT"
    PROFILE_PB2_SHA512SUM = "f22140b4a911177132057f5980773fbe891ef5a16924c4e2849d8cbdf27020aef1491905d8b72dc8a266e4db5748ad3171fc2579edca8f9d0b94f13ca90637f8"

    # Download and decode base64-encoded content (from googlesource ?format=TEXT)
    response = requests.get(PROFILE_PB2_URL)
    response.raise_for_status()

    # Confirm the content is expected, the one we trust.
    h = hashlib.sha512()
    h.update(response.content)
    if h.hexdigest() != PROFILE_PB2_SHA512SUM:
        raise ValueError(
            f"Downloaded profile_pb2.py does not match expected SHA512 checksum: {PROFILE_PB2_SHA512SUM}"
        )
    else:
        log.info("Downloaded profile_pb2.py matches expected SHA512 checksum, all good.")

    decoded_py = base64.b64decode(response.content)

    # Write to a temporary file
    tmpdir = tempfile.TemporaryDirectory()
    py_path = os.path.join(tmpdir.name, "profile_pb2.py")
    with open(py_path, "wb") as f:
        f.write(decoded_py)

    # Dynamically import the module
    spec = importlib.util.spec_from_file_location("profile_pb2", py_path)
    profile_pb2 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(profile_pb2)

    return profile_pb2, tmpdir  # Return the module and tempdir so it doesn't get GC'd


profile_pb2, _tmp = load_profile_pb2()
profile = profile_pb2.Profile()


def _start_profiling_cpu(client: EndpointHttpClient, event: threading.Event):
    """
    Start CPU profiling for the compute node.
    """
    log.info("Starting CPU profiling...")
    try:
        event.set()
        response = client.profile_cpu(100, 5, False)
        log.info("CPU profiling finished")
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
