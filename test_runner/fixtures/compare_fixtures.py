import pytest
from contextlib import contextmanager
from abc import ABC, abstractmethod

from fixtures.zenith_fixtures import PgBin, PgProtocol, VanillaPostgres, ZenithEnv
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

# Type-related stuff
from typing import Iterator


class PgCompare(ABC):
    """Common interface of all postgres implementations, useful for benchmarks.

    This class is a helper class for the zenith_with_baseline fixture. See its documentation
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
    def zenbenchmark(self) -> ZenithBenchmarker:
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


class ZenithCompare(PgCompare):
    """PgCompare interface for the zenith stack."""
    def __init__(self,
                 zenbenchmark: ZenithBenchmarker,
                 zenith_simple_env: ZenithEnv,
                 pg_bin: PgBin,
                 branch_name):
        self.env = zenith_simple_env
        self._zenbenchmark = zenbenchmark
        self._pg_bin = pg_bin

        # We only use one branch and one timeline
        self.branch = branch_name
        self.env.zenith_cli.create_branch(self.branch, "empty")
        self._pg = self.env.postgres.create_start(self.branch)
        self.timeline = self.pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

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

    def record_pageserver_writes(self, out_name):
        return self.zenbenchmark.record_pageserver_writes(self.env.pageserver, out_name)

    def record_duration(self, out_name):
        return self.zenbenchmark.record_duration(out_name)


class VanillaCompare(PgCompare):
    """PgCompare interface for vanilla postgres."""
    def __init__(self, zenbenchmark, vanilla_pg: VanillaPostgres):
        self._pg = vanilla_pg
        self._zenbenchmark = zenbenchmark
        vanilla_pg.configure(['shared_buffers=1MB'])
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


@pytest.fixture(scope='function')
def zenith_compare(request, zenbenchmark, pg_bin, zenith_simple_env) -> ZenithCompare:
    branch_name = request.node.name
    return ZenithCompare(zenbenchmark, zenith_simple_env, pg_bin, branch_name)


@pytest.fixture(scope='function')
def vanilla_compare(zenbenchmark, vanilla_pg) -> VanillaCompare:
    return VanillaCompare(zenbenchmark, vanilla_pg)


@pytest.fixture(params=["vanilla_compare", "zenith_compare"], ids=["vanilla", "zenith"])
def zenith_with_baseline(request) -> PgCompare:
    """Parameterized fixture that helps compare zenith against vanilla postgres.

    A test that uses this fixture turns into a parameterized test that runs against:
    1. A vanilla postgres instance
    2. A simple zenith env (see zenith_simple_env)
    3. Possibly other postgres protocol implementations.

    The main goal of this fixture is to make it easier for people to read and write
    performance tests. Easy test writing leads to more tests.

    Perfect encapsulation of the postgres implementations is **not** a goal because
    it's impossible. Operational and configuration differences in the different
    implementations sometimes matter, and the writer of the test should be mindful
    of that.

    If a test requires some one-off special implementation-specific logic, use of
    isinstance(zenith_with_baseline, ZenithCompare) is encouraged. Though if that
    implementation-specific logic is widely useful across multiple tests, it might
    make sense to add methods to the PgCompare class.
    """
    fixture = request.getfixturevalue(request.param)
    if isinstance(fixture, PgCompare):
        return fixture
    else:
        raise AssertionError(f"test error: fixture {request.param} is not PgCompare")
