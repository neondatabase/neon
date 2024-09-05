import os
import time
from abc import ABC, abstractmethod
from contextlib import _GeneratorContextManager, contextmanager

# Type-related stuff
from typing import Dict, Iterator, List

import pytest
from _pytest.fixtures import FixtureRequest

from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    PgBin,
    PgProtocol,
    RemotePostgres,
    VanillaPostgres,
    wait_for_last_flush_lsn,
)
from fixtures.pg_stats import PgStatTable


class PgCompare(ABC):
    """Common interface of all postgres implementations, useful for benchmarks.

    This class is a helper class for the neon_with_baseline fixture. See its documentation
    for more details.
    """

    @property
    @abstractmethod
    def pg(self) -> PgProtocol:
        pass

    @property
    @abstractmethod
    def pg_bin(self) -> PgBin:
        pass

    @property
    @abstractmethod
    def zenbenchmark(self) -> NeonBenchmarker:
        pass

    @abstractmethod
    def flush(self, compact: bool = False, gc: bool = False):
        pass

    @abstractmethod
    def compact(self):
        pass

    @abstractmethod
    def report_peak_memory_use(self):
        pass

    @abstractmethod
    def report_size(self):
        pass

    @contextmanager
    @abstractmethod
    def record_pageserver_writes(self, out_name):
        pass

    @contextmanager
    @abstractmethod
    def record_duration(self, out_name):
        pass

    @contextmanager
    def record_pg_stats(self, pg_stats: List[PgStatTable]) -> Iterator[None]:
        init_data = self._retrieve_pg_stats(pg_stats)

        yield

        data = self._retrieve_pg_stats(pg_stats)

        for k in set(init_data) & set(data):
            self.zenbenchmark.record(k, data[k] - init_data[k], "", MetricReport.HIGHER_IS_BETTER)

    def _retrieve_pg_stats(self, pg_stats: List[PgStatTable]) -> Dict[str, int]:
        results: Dict[str, int] = {}

        with self.pg.connect().cursor() as cur:
            for pg_stat in pg_stats:
                cur.execute(pg_stat.query)
                row = cur.fetchone()
                assert row is not None
                assert len(row) == len(pg_stat.columns)

                for col, val in zip(pg_stat.columns, row):
                    results[f"{pg_stat.table}.{col}"] = int(val)

        return results


class NeonCompare(PgCompare):
    """PgCompare interface for the neon stack."""

    def __init__(
        self,
        zenbenchmark: NeonBenchmarker,
        neon_simple_env: NeonEnv,
        pg_bin: PgBin,
    ):
        self.env = neon_simple_env
        self._zenbenchmark = zenbenchmark
        self._pg_bin = pg_bin
        self.pageserver_http_client = self.env.pageserver.http_client()

        # note that neon_simple_env now uses LOCAL_FS remote storage
        self.tenant = self.env.initial_tenant
        self.timeline = self.env.initial_timeline

        # Start pg
        self._pg = self.env.endpoints.create_start("main", "main", self.tenant)

    @property
    def pg(self) -> PgProtocol:
        return self._pg

    @property
    def zenbenchmark(self) -> NeonBenchmarker:
        return self._zenbenchmark

    @property
    def pg_bin(self) -> PgBin:
        return self._pg_bin

    def flush(self, compact: bool = True, gc: bool = True):
        wait_for_last_flush_lsn(self.env, self._pg, self.tenant, self.timeline)
        self.pageserver_http_client.timeline_checkpoint(self.tenant, self.timeline, compact=compact)
        if gc:
            self.pageserver_http_client.timeline_gc(self.tenant, self.timeline, 0)

    def compact(self):
        self.pageserver_http_client.timeline_compact(
            self.tenant, self.timeline, wait_until_uploaded=True
        )

    def report_peak_memory_use(self):
        self.zenbenchmark.record(
            "peak_mem",
            self.zenbenchmark.get_peak_mem(self.env.pageserver) / 1024,
            "MB",
            report=MetricReport.LOWER_IS_BETTER,
        )

    def report_size(self):
        timeline_size = self.zenbenchmark.get_timeline_size(
            self.env.repo_dir, self.tenant, self.timeline
        )
        self.zenbenchmark.record(
            "size", timeline_size / (1024 * 1024), "MB", report=MetricReport.LOWER_IS_BETTER
        )

        metric_filters = {
            "tenant_id": str(self.tenant),
            "timeline_id": str(self.timeline),
            "file_kind": "layer",
            "op_kind": "upload",
        }
        # use `started` (not `finished`) counters here, because some callers
        # don't wait for upload queue to drain
        total_files = self.zenbenchmark.get_int_counter_value(
            self.env.pageserver,
            "pageserver_remote_timeline_client_calls_started_total",
            metric_filters,
        )
        total_bytes = self.zenbenchmark.get_int_counter_value(
            self.env.pageserver,
            "pageserver_remote_timeline_client_bytes_started_total",
            metric_filters,
        )
        self.zenbenchmark.record(
            "data_uploaded", total_bytes / (1024 * 1024), "MB", report=MetricReport.LOWER_IS_BETTER
        )
        self.zenbenchmark.record(
            "num_files_uploaded", total_files, "", report=MetricReport.LOWER_IS_BETTER
        )

    def record_pageserver_writes(self, out_name: str) -> _GeneratorContextManager[None]:
        return self.zenbenchmark.record_pageserver_writes(self.env.pageserver, out_name)

    def record_duration(self, out_name: str) -> _GeneratorContextManager[None]:
        return self.zenbenchmark.record_duration(out_name)


class VanillaCompare(PgCompare):
    """PgCompare interface for vanilla postgres."""

    def __init__(self, zenbenchmark: NeonBenchmarker, vanilla_pg: VanillaPostgres):
        self._pg = vanilla_pg
        self._zenbenchmark = zenbenchmark
        vanilla_pg.configure(
            [
                "shared_buffers=1MB",
                "synchronous_commit=off",
            ]
        )
        vanilla_pg.start()

        # Long-lived cursor, useful for flushing
        self.conn = self.pg.connect()
        self.cur = self.conn.cursor()

    @property
    def pg(self) -> VanillaPostgres:
        return self._pg

    @property
    def zenbenchmark(self) -> NeonBenchmarker:
        return self._zenbenchmark

    @property
    def pg_bin(self) -> PgBin:
        return self._pg.pg_bin

    def flush(self, compact: bool = False, gc: bool = False):
        self.cur.execute("checkpoint")

    def compact(self):
        pass

    def report_peak_memory_use(self):
        pass  # TODO find something

    def report_size(self):
        data_size = self.pg.get_subdir_size("base")
        self.zenbenchmark.record(
            "data_size", data_size / (1024 * 1024), "MB", report=MetricReport.LOWER_IS_BETTER
        )
        wal_size = self.pg.get_subdir_size("pg_wal")
        self.zenbenchmark.record(
            "wal_size", wal_size / (1024 * 1024), "MB", report=MetricReport.LOWER_IS_BETTER
        )

    @contextmanager
    def record_pageserver_writes(self, out_name: str) -> Iterator[None]:
        yield  # Do nothing

    def record_duration(self, out_name: str) -> _GeneratorContextManager[None]:
        return self.zenbenchmark.record_duration(out_name)


class RemoteCompare(PgCompare):
    """PgCompare interface for a remote postgres instance."""

    def __init__(self, zenbenchmark: NeonBenchmarker, remote_pg: RemotePostgres):
        self._pg = remote_pg
        self._zenbenchmark = zenbenchmark

        # Long-lived cursor, useful for flushing
        self.conn = self.pg.connect()
        self.cur = self.conn.cursor()

    @property
    def pg(self) -> PgProtocol:
        return self._pg

    @property
    def zenbenchmark(self) -> NeonBenchmarker:
        return self._zenbenchmark

    @property
    def pg_bin(self) -> PgBin:
        return self._pg.pg_bin

    def flush(self):
        # TODO: flush the remote pageserver
        pass

    def compact(self):
        pass

    def report_peak_memory_use(self):
        # TODO: get memory usage from remote pageserver
        pass

    def report_size(self):
        # TODO: get storage size from remote pageserver
        pass

    @contextmanager
    def record_pageserver_writes(self, out_name: str) -> Iterator[None]:
        yield  # Do nothing

    def record_duration(self, out_name: str) -> _GeneratorContextManager[None]:
        return self.zenbenchmark.record_duration(out_name)


@pytest.fixture(scope="function")
def neon_compare(
    zenbenchmark: NeonBenchmarker,
    pg_bin: PgBin,
    neon_simple_env: NeonEnv,
) -> NeonCompare:
    return NeonCompare(zenbenchmark, neon_simple_env, pg_bin)


@pytest.fixture(scope="function")
def vanilla_compare(zenbenchmark: NeonBenchmarker, vanilla_pg: VanillaPostgres) -> VanillaCompare:
    return VanillaCompare(zenbenchmark, vanilla_pg)


@pytest.fixture(scope="function")
def remote_compare(zenbenchmark: NeonBenchmarker, remote_pg: RemotePostgres) -> RemoteCompare:
    return RemoteCompare(zenbenchmark, remote_pg)


@pytest.fixture(params=["vanilla_compare", "neon_compare"], ids=["vanilla", "neon"])
def neon_with_baseline(request: FixtureRequest) -> PgCompare:
    """Parameterized fixture that helps compare neon against vanilla postgres.

    A test that uses this fixture turns into a parameterized test that runs against:
    1. A vanilla postgres instance
    2. A simple neon env (see neon_simple_env)
    3. Possibly other postgres protocol implementations.

    The main goal of this fixture is to make it easier for people to read and write
    performance tests. Easy test writing leads to more tests.

    Perfect encapsulation of the postgres implementations is **not** a goal because
    it's impossible. Operational and configuration differences in the different
    implementations sometimes matter, and the writer of the test should be mindful
    of that.

    If a test requires some one-off special implementation-specific logic, use of
    isinstance(neon_with_baseline, NeonCompare) is encouraged. Though if that
    implementation-specific logic is widely useful across multiple tests, it might
    make sense to add methods to the PgCompare class.
    """
    fixture = request.getfixturevalue(request.param)
    assert isinstance(fixture, PgCompare), f"test error: fixture {fixture} is not PgCompare"
    return fixture


@pytest.fixture(scope="function", autouse=True)
def sync_after_each_test():
    # The fixture calls `sync(2)` after each test if `SYNC_AFTER_EACH_TEST` env var is `true`
    # 
    # In CI, `SYNC_AFTER_EACH_TEST` is set to `true` only for benchmarks (`test_runner/performance`) 
    # that are run on self-hosted runners because some of these tests are pretty write-heavy 
    # and create issues to start the processes within 10s
    key = "SYNC_AFTER_EACH_TEST"
    enabled = os.environ.get(key) == "true"

    yield

    if not enabled:
        # regress test, or running locally
        return

    start = time.time()
    # we only run benches on linuxes, the method might not exist on windows
    os.sync()
    elapsed = time.time() - start
    log.info(f"called sync after test {elapsed=}")
