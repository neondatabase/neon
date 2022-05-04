from contextlib import closing
from fixtures.zenith_fixtures import PgBin, VanillaPostgres, ZenithEnv, profiling_supported
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare

from fixtures.benchmark_fixture import PgBenchRunResult, MetricReport, ZenithBenchmarker
from fixtures.log_helper import log

from pathlib import Path

import pytest
from datetime import datetime
import calendar
import os
import timeit


def utc_now_timestamp() -> int:
    return calendar.timegm(datetime.utcnow().utctimetuple())


def init_pgbench(env: PgCompare, cmdline):
    # calculate timestamps and durations separately
    # timestamp is intended to be used for linking to grafana and logs
    # duration is actually a metric and uses float instead of int for timestamp
    init_start_timestamp = utc_now_timestamp()
    t0 = timeit.default_timer()
    with env.record_pageserver_writes('init.pageserver_writes'):
        env.pg_bin.run_capture(cmdline)
        env.flush()
    init_duration = timeit.default_timer() - t0
    init_end_timestamp = utc_now_timestamp()

    env.zenbenchmark.record("init.duration",
                            init_duration,
                            unit="s",
                            report=MetricReport.LOWER_IS_BETTER)
    env.zenbenchmark.record("init.start_timestamp",
                            init_start_timestamp,
                            '',
                            MetricReport.TEST_PARAM)
    env.zenbenchmark.record("init.end_timestamp", init_end_timestamp, '', MetricReport.TEST_PARAM)


def run_pgbench(env: PgCompare, prefix: str, cmdline):
    with env.record_pageserver_writes(f'{prefix}.pageserver_writes'):
        run_start_timestamp = utc_now_timestamp()
        t0 = timeit.default_timer()
        out = env.pg_bin.run_capture(cmdline, )
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
def run_test_pgbench(env: PgCompare, scale: int, duration: int):

    # Record the scale and initialize
    env.zenbenchmark.record("scale", scale, '', MetricReport.TEST_PARAM)
    init_pgbench(env, ['pgbench', f'-s{scale}', '-i', env.pg.connstr()])

    # Run simple-update workload
    run_pgbench(env,
                "simple-update",
                ['pgbench', '-n', '-c4', f'-T{duration}', '-P2', '-Mprepared', env.pg.connstr()])

    # Run SELECT workload
    run_pgbench(env,
                "select-only",
                ['pgbench', '-S', '-c4', f'-T{duration}', '-P2', '-Mprepared', env.pg.connstr()])

    env.report_size()


def get_durations_matrix():
    durations = os.getenv("TEST_PG_BENCH_DURATIONS_MATRIX", default="45")
    return list(map(int, durations.split(",")))


def get_scales_matrix():
    scales = os.getenv("TEST_PG_BENCH_SCALES_MATRIX", default="10")
    return list(map(int, scales.split(",")))


# Run the pgbench tests against vanilla Postgres and zenith
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench(zenith_with_baseline: PgCompare, scale: int, duration: int):
    run_test_pgbench(zenith_with_baseline, scale, duration)


# Run the pgbench tests, and generate a flamegraph from it
# This requires that the pageserver was built with the 'profiling' feature.
#
# TODO: If the profiling is cheap enough, there's no need to run the same test
# twice, with and without profiling. But for now, run it separately, so that we
# can see how much overhead the profiling adds.
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
def test_pgbench_flamegraph(zenbenchmark, pg_bin, zenith_env_builder, scale: int, duration: int):
    zenith_env_builder.num_safekeepers = 1
    zenith_env_builder.pageserver_config_override = '''
profiling="page_requests"
'''
    if not profiling_supported():
        pytest.skip("pageserver was built without 'profiling' feature")

    env = zenith_env_builder.init_start()
    env.zenith_cli.create_branch("empty", "main")

    run_test_pgbench(ZenithCompare(zenbenchmark, env, pg_bin, "pgbench"), scale, duration)


# Run the pgbench tests against an existing Postgres cluster
@pytest.mark.parametrize("scale", get_scales_matrix())
@pytest.mark.parametrize("duration", get_durations_matrix())
@pytest.mark.remote_cluster
def test_pgbench_remote(remote_compare: PgCompare, scale: int, duration: int):
    run_test_pgbench(remote_compare, scale, duration)
