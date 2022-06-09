import pytest
from contextlib import contextmanager
from abc import ABC, abstractmethod
from fixtures.pg_stats import PgStatTable

from fixtures.neon_fixtures import PgBin, PgProtocol, VanillaPostgres, RemotePostgres, NeonEnv
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker

# Type-related stuff
from typing import Dict, List, Optional


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
    def zenbenchmark(self) -> NeonBenchmarker:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass

    @abstractmethod
    def report_peak_memory_use(self) -> None:
        pass

    @abstractmethod
    def report_size(self) -> None:
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
    def record_pg_stats(self, pg_stats: List[PgStatTable]):
        init_data = self._retrieve_pg_stats(pg_stats)

        yield

        data = self._retrieve_pg_stats(pg_stats)

        for k in set(init_data) & set(data):
            self.zenbenchmark.record(k, data[k] - init_data[k], '', MetricReport.HIGHER_IS_BETTER)

    def _retrieve_pg_stats(self, pg_stats: List[PgStatTable]) -> Dict[str, int]:
        results: Dict[str, int] = {}

        with self.pg.connect().cursor() as cur:
            for pg_stat in pg_stats:
                cur.execute(pg_stat.query)
                row = cur.fetchone()
                assert len(row) == len(pg_stat.columns)

                for col, val in zip(pg_stat.columns, row):
                    results[f"{pg_stat.table}.{col}"] = int(val)

        return results


class NeonCompare(PgCompare):
    """PgCompare interface for the neon stack."""
    def __init__(self,
                 zenbenchmark: NeonBenchmarker,
                 neon_simple_env: NeonEnv,
                 pg_bin: PgBin,
                 branch_name: str,
                 config_lines: Optional[List[str]] = None):
        self.env = neon_simple_env
        self._zenbenchmark = zenbenchmark
        self._pg_bin = pg_bin

        # We only use one branch and one timeline
        self.env.neon_cli.create_branch(branch_name, 'empty')
        self._pg = self.env.postgres.create_start(branch_name, config_lines=config_lines)
        self.timeline = self.pg.safe_psql("SHOW neon.timeline_id")[0][0]

        # Long-lived cursor, useful for flushing
        self.psconn = self.env.pageserver.connect()
        self.pscur = self.psconn.cursor()

    @property
    def pg(self):
        return self._pg

    @property
    def zenbenchmark(self):
        return self._zenbenchmark

    @property
    def pg_bin(self):
        return self._pg_bin

    def flush(self):
        self.pscur.execute(f"do_gc {self.env.initial_tenant.hex} {self.timeline} 0")

    def compact(self):
        self.pscur.execute(f"compact {self.env.initial_tenant.hex} {self.timeline}")

    def report_peak_memory_use(self) -> None:
        self.zenbenchmark.record("peak_mem",
                                 self.zenbenchmark.get_peak_mem(self.env.pageserver) / 1024,
                                 'MB',
                                 report=MetricReport.LOWER_IS_BETTER)

    def report_size(self) -> None:
        timeline_size = self.zenbenchmark.get_timeline_size(self.env.repo_dir,
                                                            self.env.initial_tenant,
                                                            self.timeline)
        self.zenbenchmark.record('size',
                                 timeline_size / (1024 * 1024),
                                 'MB',
                                 report=MetricReport.LOWER_IS_BETTER)

        total_files = self.zenbenchmark.get_int_counter_value(
            self.env.pageserver, "pageserver_created_persistent_files_total")
        total_bytes = self.zenbenchmark.get_int_counter_value(
            self.env.pageserver, "pageserver_written_persistent_bytes_total")
        self.zenbenchmark.record("data_uploaded",
                                 total_bytes / (1024 * 1024),
                                 "MB",
                                 report=MetricReport.LOWER_IS_BETTER)
        self.zenbenchmark.record("num_files_uploaded",
                                 total_files,
                                 "",
                                 report=MetricReport.LOWER_IS_BETTER)

    def record_pageserver_writes(self, out_name):
        return self.zenbenchmark.record_pageserver_writes(self.env.pageserver, out_name)

    def record_duration(self, out_name):
        return self.zenbenchmark.record_duration(out_name)


class VanillaCompare(PgCompare):
    """PgCompare interface for vanilla postgres."""
    def __init__(self, zenbenchmark, vanilla_pg: VanillaPostgres):
        self._pg = vanilla_pg
        self._zenbenchmark = zenbenchmark
        vanilla_pg.configure([
            'shared_buffers=1MB',
            'synchronous_commit=off',
        ])
        vanilla_pg.start()

        # Long-lived cursor, useful for flushing
        self.conn = self.pg.connect()
        self.cur = self.conn.cursor()

    @property
    def pg(self):
        return self._pg

    @property
    def zenbenchmark(self):
        return self._zenbenchmark

    @property
    def pg_bin(self):
        return self._pg.pg_bin

    def flush(self):
        self.cur.execute("checkpoint")

    def report_peak_memory_use(self) -> None:
        pass  # TODO find something

    def report_size(self) -> None:
        data_size = self.pg.get_subdir_size('base')
        self.zenbenchmark.record('data_size',
                                 data_size / (1024 * 1024),
                                 'MB',
                                 report=MetricReport.LOWER_IS_BETTER)
        wal_size = self.pg.get_subdir_size('pg_wal')
        self.zenbenchmark.record('wal_size',
                                 wal_size / (1024 * 1024),
                                 'MB',
                                 report=MetricReport.LOWER_IS_BETTER)

    @contextmanager
    def record_pageserver_writes(self, out_name):
        yield  # Do nothing

    def record_duration(self, out_name):
        return self.zenbenchmark.record_duration(out_name)


class RemoteCompare(PgCompare):
    """PgCompare interface for a remote postgres instance."""
    def __init__(self, zenbenchmark, remote_pg: RemotePostgres):
        self._pg = remote_pg
        self._zenbenchmark = zenbenchmark

        # Long-lived cursor, useful for flushing
        self.conn = self.pg.connect()
        self.cur = self.conn.cursor()

    @property
    def pg(self):
        return self._pg

    @property
    def zenbenchmark(self):
        return self._zenbenchmark

    @property
    def pg_bin(self):
        return self._pg.pg_bin

    def flush(self):
        # TODO: flush the remote pageserver
        pass

    def report_peak_memory_use(self) -> None:
        # TODO: get memory usage from remote pageserver
        pass

    def report_size(self) -> None:
        # TODO: get storage size from remote pageserver
        pass

    @contextmanager
    def record_pageserver_writes(self, out_name):
        yield  # Do nothing

    def record_duration(self, out_name):
        return self.zenbenchmark.record_duration(out_name)


@pytest.fixture(scope='function')
def neon_compare(request, zenbenchmark, pg_bin, neon_simple_env) -> NeonCompare:
    branch_name = request.node.name
    return NeonCompare(zenbenchmark, neon_simple_env, pg_bin, branch_name)


@pytest.fixture(scope='function')
def vanilla_compare(zenbenchmark, vanilla_pg) -> VanillaCompare:
    return VanillaCompare(zenbenchmark, vanilla_pg)


@pytest.fixture(scope='function')
def remote_compare(zenbenchmark, remote_pg) -> RemoteCompare:
    return RemoteCompare(zenbenchmark, remote_pg)


@pytest.fixture(params=["vanilla_compare", "neon_compare"], ids=["vanilla", "neon"])
def neon_with_baseline(request) -> PgCompare:
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
    if isinstance(fixture, PgCompare):
        return fixture
    else:
        raise AssertionError(f"test error: fixture {request.param} is not PgCompare")
