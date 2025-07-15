from __future__ import annotations

import gzip
import io
import threading
import time
from typing import TYPE_CHECKING, Any

import pytest
from data.profile_pb2 import Profile  # type: ignore
from fixtures.log_helper import log
from fixtures.utils import run_only_on_default_postgres, run_only_on_linux_kernel_higher_than
from google.protobuf.message import Message
from requests import HTTPError

if TYPE_CHECKING:
    from fixtures.endpoint.http import EndpointHttpClient
    from fixtures.neon_fixtures import NeonEnv

LINUX_VERSION_REQUIRED = 6.0
PG_REASON = "test doesn't use postgres"
LINUX_REASON = f"test requires linux {LINUX_VERSION_REQUIRED} for ebpfs"


def _start_profiling_cpu(
    client: EndpointHttpClient, event: threading.Event | None, archive: bool = False
) -> Profile | None:
    """
    Start CPU profiling for the compute node.
    """
    log.info("Starting CPU profiling...")

    if event is not None:
        event.set()

    status, response = client.start_profiling_cpu(100, 30, archive=archive)
    match status:
        case 200:
            log.debug("CPU profiling finished")
            profile: Any = Profile()
            if archive:
                log.debug("Unpacking gzipped profile data")
                with gzip.GzipFile(fileobj=io.BytesIO(response)) as f:
                    response = f.read()
            else:
                log.debug("Unpacking non-gzipped profile data")
            Message.ParseFromString(profile, response)
            return profile
        case 204:
            log.debug("CPU profiling was stopped")
            raise HTTPError("Failed to finish CPU profiling: was stopped.")
        case 409:
            log.debug("CPU profiling is already in progress, cannot start again")
            raise HTTPError("Failed to finish CPU profiling: profiling is already in progress.")
        case 500:
            response_str = response.decode("utf-8", errors="replace")
            log.debug(
                f"Failed to finish CPU profiling, was stopped or got an error: {status}: {response_str}"
            )
            raise HTTPError(f"Failed to finish CPU profiling, {status}: {response_str}")
        case _:
            log.error(f"Failed to start CPU profiling: {status}")
            raise HTTPError(f"Failed to start CPU profiling: {status}")


def _stop_profiling_cpu(client: EndpointHttpClient, event: threading.Event | None):
    """
    Stop CPU profiling for the compute node.
    """
    log.info("Manually stopping CPU profiling...")

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
    return status


def _wait_till_profiling_starts(
    http_client: EndpointHttpClient,
    event: threading.Event | None,
    repeat_delay_secs: float = 0.3,
    timeout: int = 60,
) -> bool:
    """
    Wait until CPU profiling starts.
    """
    log.info("Waiting for CPU profiling to start...")

    end_time = time.time() + timeout

    while not http_client.get_profiling_cpu_status():
        time.sleep(repeat_delay_secs)

        if end_time <= time.time():
            log.error("Timeout while waiting for CPU profiling to start")
            return False

    log.info("CPU profiling has started successfully and is in progress")

    if event is not None:
        event.set()

    return True


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
    strings = [
        "PostgresMain",
        "ServerLoop",
        "BackgroundWorkerMain",
        "pq_recvbuf",
        "pq_getbyte",
    ]

    assert any(s in profile.string_table for s in strings), (
        f"Expected at least one of {strings} in string table, but none found"
    )


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
def test_compute_profiling_cpu_with_timeout(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling works correctly with timeout.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    pg_conn = endpoint.connect()
    pg_cur = pg_conn.cursor()
    pg_cur.execute("create database profiling_test2")
    http_client = endpoint.http_client()

    def _wait_and_assert_cpu_profiling_local():
        _wait_and_assert_cpu_profiling(http_client, None)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling_local)
    thread.start()

    inserting_should_stop_event = threading.Event()

    def insert_rows():
        lfc_conn = endpoint.connect(dbname="profiling_test2")
        lfc_cur = lfc_conn.cursor()
        n_records = 0
        lfc_cur.execute(
            "create table t(pk integer primary key, payload text default repeat('?', 128))"
        )
        batch_size = 1000

        log.info("Inserting rows")

        while not inserting_should_stop_event.is_set():
            n_records += batch_size
            lfc_cur.execute(
                f"insert into t (pk) values (generate_series({n_records - batch_size + 1},{batch_size}))"
            )

        log.info(f"Inserted {n_records} rows")

    thread2 = threading.Thread(target=insert_rows)

    assert _wait_till_profiling_starts(http_client, None)
    time.sleep(4)  # Give some time for the profiling to start
    thread2.start()

    thread.join(timeout=60)
    inserting_should_stop_event.set()  # Stop the insertion thread
    thread2.join(timeout=60)


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
def test_compute_profiling_cpu_with_archiving_the_response(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling works correctly with archiving the data.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    assert _start_profiling_cpu(http_client, None, archive=True) is not None, (
        "Failed to start and finish CPU profiling with archiving enabled"
    )


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
def test_compute_profiling_cpu_start_and_stop(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling can be started and stopped correctly.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    def _wait_and_assert_cpu_profiling():
        # Should raise as the profiling will be stopped.
        with pytest.raises(HTTPError) as _:
            _start_profiling_cpu(http_client, None)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling)
    thread.start()

    assert _wait_till_profiling_starts(http_client, None)
    _stop_profiling_cpu(http_client, None)

    # Should raise as the profiling is already stopped.
    assert _stop_profiling_cpu(http_client, None) == 412

    thread.join(timeout=60)


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
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

    def _wait_and_assert_cpu_profiling_local():
        _wait_and_assert_cpu_profiling(http_client, None)

    thread = threading.Thread(target=_wait_and_assert_cpu_profiling_local)
    thread.start()

    assert _wait_till_profiling_starts(http_client, None)

    inserting_should_stop_event = threading.Event()

    def insert_rows():
        lfc_conn = endpoint.connect(dbname="profiling_test")
        lfc_cur = lfc_conn.cursor()
        n_records = 0
        lfc_cur.execute(
            "create table t(pk integer primary key, payload text default repeat('?', 128))"
        )
        batch_size = 1000

        log.info("Inserting rows")

        while not inserting_should_stop_event.is_set():
            n_records += batch_size
            lfc_cur.execute(
                f"insert into t (pk) values (generate_series({n_records - batch_size + 1},{batch_size}))"
            )

        log.info(f"Inserted {n_records} rows")

    thread2 = threading.Thread(target=insert_rows)
    thread2.start()

    # Should raise as the profiling is already in progress.
    with pytest.raises(HTTPError) as _:
        _start_profiling_cpu(http_client, None)

    inserting_should_stop_event.set()  # Stop the insertion thread

    # The profiling should still be running and finish normally.
    thread.join(timeout=600)
    thread2.join(timeout=600)


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
def test_compute_profiling_cpu_stop_when_not_running(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling throws the expected error when is attempted
    to be stopped when it hasn't be running.
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    for _ in range(3):
        status = _stop_profiling_cpu(http_client, None)
        assert status == 412


@run_only_on_default_postgres(reason=PG_REASON)
@run_only_on_linux_kernel_higher_than(version=LINUX_VERSION_REQUIRED, reason=LINUX_REASON)
def test_compute_profiling_cpu_start_arguments_validation_works(neon_simple_env: NeonEnv):
    """
    Test that CPU profiling start request properly validated the
    arguments and throws the expected error (bad request).
    """
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")
    http_client = endpoint.http_client()

    for sampling_frequency in [-1, 0, 1000000]:
        status, _ = http_client.start_profiling_cpu(sampling_frequency, 5)
        assert status == 422
    for timeout_seconds in [-1, 0, 1000000]:
        status, _ = http_client.start_profiling_cpu(5, timeout_seconds)
        assert status == 422
