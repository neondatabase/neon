from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Any

import pytest
from data.profile_pb2 import Profile  # type: ignore
from fixtures.log_helper import log
from fixtures.utils import run_only_on_default_postgres
from google.protobuf.message import Message
from requests import HTTPError

if TYPE_CHECKING:
    from fixtures.endpoint.http import EndpointHttpClient
    from fixtures.neon_fixtures import NeonEnv


def _start_profiling_cpu(client: EndpointHttpClient, event: threading.Event | None):
    """
    Start CPU profiling for the compute node.
    """
    log.info("Starting CPU profiling...")

    try:
        if event is not None:
            event.set()

        status, response = client.start_profiling_cpu(100, 10)
        match status:
            case 200:
                log.debug("CPU profiling finished")
                profile: Any = Profile()
                Message.ParseFromString(profile, response)
                return profile
            case 204:
                log.debug("CPU profiling was stopped")
                raise HTTPError("Failed to finish CPU profiling: was stopped.")
            case 208:
                log.debug("CPU profiling is already in progress, nothing to do")
                raise HTTPError("Failed to finish CPU profiling: profiling is already in progress.")
            case _:
                log.error(f"Failed to finish CPU profiling, unexpected status {status}")
                raise HTTPError(f"Failed to finish CPU profiling, unexpected status: {status}")
    except Exception as e:
        log.debug(f"Error finishing CPU profiling: {e}")
        raise


def _stop_profiling_cpu(client: EndpointHttpClient, event: threading.Event | None):
    """
    Stop CPU profiling for the compute node.
    """
    log.info("Manually stopping CPU profiling...")

    try:
        if event is not None:
            event.set()

        status = client.stop_profiling_cpu()
        match status:
            case 200:
                log.debug("CPU profiling stopped successfully")
            case 412:
                log.debug("CPU profiling is not running, nothing to do")
            case _:
                log.error(f"Failed to stop CPU profiling: {status}")
                raise HTTPError(f"Failed to stop CPU profiling: {status}")
    except Exception as e:
        log.debug(f"Error stopping CPU profiling: {e}")
        raise


def _wait_and_assert_cpu_profiling(http_client: EndpointHttpClient, event: threading.Event | None):
    profile = _start_profiling_cpu(http_client, event)

    if profile is None:
        log.error("The received profiling data is malformed or empty.")
        return

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


@run_only_on_default_postgres(reason="test doesn't use postgres")
def test_compute_profiling_cpu_with_timeout(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling works correctly with timeout.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create database profiling_test")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling_local():
        _wait_and_assert_cpu_profiling(http_client, event)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling_local)
    thread.start()

    def insert_rows():
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

    event.wait()  # Wait for profiling to be ready to start
    time.sleep(4)  # Give some time for the profiling to start
    thread2.start()

    thread.join(timeout=60)
    thread2.join(timeout=60)

    endpoint.stop()
    endpoint.start()


@run_only_on_default_postgres(reason="test doesn't use postgres")
def test_compute_profiling_cpu_start_and_stop(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling can be started and stopped correctly.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling():
        # Should raise as the profiling will be stopped.
        with pytest.raises(HTTPError) as _:
            _start_profiling_cpu(http_client, event)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling)
    thread.start()

    event.wait()  # Wait for profiling to be ready to start
    time.sleep(4)  # Give some time for the profiling to start
    _stop_profiling_cpu(http_client, None)

    # Should raise as the profiling is already stopped.
    with pytest.raises(HTTPError) as exc_info:
        _stop_profiling_cpu(http_client, None)
    assert exc_info.value.response.status_code == 412

    thread.join(timeout=60)

    endpoint.stop()
    endpoint.start()


@run_only_on_default_postgres(reason="test doesn't use postgres")
def test_compute_profiling_cpu_conflict(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling can be started once and the second time
    it will throw an error as it is already running.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create database profiling_test")
    http_client = endpoint.http_client()
    event = threading.Event()

    def _wait_and_assert_cpu_profiling_local():
        _wait_and_assert_cpu_profiling(http_client, event)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling_local)
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

    event.wait()  # Wait for profiling to be ready to start
    time.sleep(4)  # Give some time for the profiling to start

    thread2.start()

    # Should raise as the profiling is already in progress.
    with pytest.raises(HTTPError) as _:
        _start_profiling_cpu(http_client, None)

    # The profiling should still be running and finish normally.
    thread.join(timeout=60)
    thread2.join(timeout=60)

    endpoint.stop()
    endpoint.start()


@run_only_on_default_postgres(reason="test doesn't use postgres")
def test_compute_profiling_cpu_stop_when_not_running(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling throws the expected error when is attempted
    to be stopped when it hasn't be running.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    for _ in range(3):
        with pytest.raises(HTTPError) as exc_info:
            _stop_profiling_cpu(http_client, None)
        assert exc_info.value.response.status_code == 412


@run_only_on_default_postgres(reason="test doesn't use postgres")
def test_compute_profiling_cpu_start_arguments_validation_works(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling start request properly validated the
    arguments and throws the expected error (bad request).
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    for sampling_frequency in [-1, 0, 1000000]:
        with pytest.raises(HTTPError) as exc_info:
            http_client.start_profiling_cpu(sampling_frequency, 5)
        assert exc_info.value.response.status_code == 400
    for timeout_seconds in [-1, 0, 1000000]:
        with pytest.raises(HTTPError) as exc_info:
            http_client.start_profiling_cpu(5, timeout_seconds)
        assert exc_info.value.response.status_code == 400
