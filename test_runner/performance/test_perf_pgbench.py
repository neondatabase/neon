import calendar
import enum
import os
import timeit
from datetime import datetime
from pathlib import Path
from typing import List

import pytest
from fixtures.benchmark_fixture import MetricReport, PgBenchInitResult, PgBenchRunResult
from fixtures.compare_fixtures import NeonCompare, PgCompare
from fixtures.neon_fixtures import profiling_supported
from fixtures.utils import get_scale_for_db


@enum.unique
class PgBenchLoadType(enum.Enum):
    INIT = "init"
    SIMPLE_UPDATE = "simple_update"
    SELECT_ONLY = "select-only"


def utc_now_timestamp() -> int:
    return calendar.timegm(datetime.utcnow().utctimetuple())


def init_pgbench(env: PgCompare, cmdline):
    # calculate timestamps and durations separately
    # timestamp is intended to be used for linking to grafana and logs
    # duration is actually a metric and uses float instead of int for timestamp
    start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    with env.record_pageserver_writes("init.pageserver_writes"):
        out = env.pg_bin.run_capture(cmdline)
        env.flush()

    duration = timeit.default_timer() - t0
    end_timestamp = utc_now_timestamp()

    stderr = Path(f"{out}.stderr").read_text()

    res = PgBenchInitResult.parse_from_stderr(
        stderr=stderr,
        duration=duration,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    env.zenbenchmark.record_pg_bench_init_result("init", res)


def run_pgbench(env: PgCompare, prefix: str, cmdline):
    with env.record_pageserver_writes(f"{prefix}.pageserver_writes"):
        run_start_timestamp = utc_now_timestamp()
        t0 = timeit.default_timer()
        out = env.pg_bin.run_capture(
            cmdline,
        )
        run_duration = timeit.default_timer() - t0
        run_end_timestamp = utc_now_timestamp()
        env.flush()

    stdout = Path(f"{out}.stdout").read_text()

    res = PgBenchRunResult.parse_from_stdout(
        stdout=stdout,
        run_duration=run_duration,
        run_start_timestamp=run_start_timestamp,
        run_end_timestamp=run_end_timestamp,
    )
    env.zenbenchmark.record_pg_bench_result(prefix, res)


#
# Initialize a pgbench database, and run pgbench against it.
#
# This makes runs two different pgbench workloads against the same
# initialized database, and 'duration' is the time of each run. So
# the total runtime is 2 * duration, plus time needed to initialize
# the test database.
#
# Currently, the # of connections is hardcoded at 4
def run_test_pgbench(env: PgCompare, scale: int, duration: int, workload_type: PgBenchLoadType):
    env.zenbenchmark.record("scale", scale, "", MetricReport.TEST_PARAM)

    if workload_type == PgBenchLoadType.INIT:
        # Run initialize
        init_pgbench(
            env, ["pgbench", f"-s{scale}", "-i", env.pg.connstr(options="-cstatement_timeout=1h")]
        )

    if workload_type == PgBenchLoadType.SIMPLE_UPDATE:
        # Run simple-update workload
        run_pgbench(
            env,
            "simple-update",
            [
                "pgbench",
                "-N",
                "-c4",
                f"-T{duration}",
                "-P2",
                "--progress-timestamp",
                env.pg.connstr(),
            ],
        )

    if workload_type == PgBenchLoadType.SELECT_ONLY:
        # Run SELECT workload
        run_pgbench(
            env,
            "select-only",
            [
                "pgbench",
                "-S",
                "-c4",
                f"-T{duration}",
                "-P2",
                "--progress-timestamp",
                env.pg.connstr(),
            ],
        )

    env.report_size()


def get_durations_matrix(default: int = 45) -> List[int]:
    durations = os.getenv("TEST_PG_BENCH_DURATIONS_MATRIX", default=str(default))
    rv = []
    for d in durations.split(","):
        d = d.strip().lower()
        if d.endswith("h"):
            duration = int(d.removesuffix("h")) * 60 * 60
        elif d.endswith("m"):
            duration = int(d.removesuffix("m")) * 60
        else:
            duration = int(d.removesuffix("s"))
        rv.append(duration)

    return rv


def get_scales_matrix(default: int = 10) -> List[int]:
    scales = os.getenv("TEST_PG_BENCH_SCALES_MATRIX", default=str(default))
    rv = []
    for s in scales.split(","):
        s = s.strip().lower()
        if s.endswith("mb"):
            scale = get_scale_for_db(int(s.removesuffix("mb")))
        elif s.endswith("gb"):
            scale = get_scale_for_db(int(s.removesuffix("gb")) * 1024)
        else:
            scale = int(s)
        rv.append(scale)

    return rv


# Run the pgbench tests against vanilla Postgres and neon
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench(neon_with_baseline: PgCompare, scale: int, duration: int):
    run_test_pgbench(neon_with_baseline, scale, duration, PgBenchLoadType.INIT)
    run_test_pgbench(neon_with_baseline, scale, duration, PgBenchLoadType.SIMPLE_UPDATE)
    run_test_pgbench(neon_with_baseline, scale, duration, PgBenchLoadType.SELECT_ONLY)


# Run the pgbench tests, and generate a flamegraph from it
# This requires that the pageserver was built with the 'profiling' feature.
#
# TODO: If the profiling is cheap enough, there's no need to run the same test
# twice, with and without profiling. But for now, run it separately, so that we
# can see how much overhead the profiling adds.
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench_flamegraph(zenbenchmark, pg_bin, neon_env_builder, scale: int, duration: int):
    neon_env_builder.num_safekeepers = 1
    neon_env_builder.pageserver_config_override = """
profiling="page_requests"
"""
    if not profiling_supported():
        pytest.skip("pageserver was built without 'profiling' feature")

    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("empty", "main")

    neon_compare = NeonCompare(zenbenchmark, env, pg_bin, "pgbench")
    run_test_pgbench(neon_compare, scale, duration, PgBenchLoadType.INIT)
    run_test_pgbench(neon_compare, scale, duration, PgBenchLoadType.SIMPLE_UPDATE)
    run_test_pgbench(neon_compare, scale, duration, PgBenchLoadType.SELECT_ONLY)


# The following 3 tests run on an existing database as it was set up by previous tests,
# and leaves the database in a state that would be used in the next tests.
# Modifying the definition order of these functions or adding other remote tests in between will alter results.
# See usage of --sparse-ordering flag in the pytest invocation in the CI workflow
#
# Run the pgbench tests against an existing Postgres cluster
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote_init(remote_compare: PgCompare, scale: int, duration: int):
    run_test_pgbench(remote_compare, scale, duration, PgBenchLoadType.INIT)


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote_simple_update(remote_compare: PgCompare, scale: int, duration: int):
    run_test_pgbench(remote_compare, scale, duration, PgBenchLoadType.SIMPLE_UPDATE)


@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote_select_only(remote_compare: PgCompare, scale: int, duration: int):
    run_test_pgbench(remote_compare, scale, duration, PgBenchLoadType.SELECT_ONLY)
