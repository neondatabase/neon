from contextlib import closing
from fixtures.zenith_fixtures import PgBin, VanillaPostgres, ZenithEnv
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare

from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker
from fixtures.log_helper import log

pytest_plugins = (
    "fixtures.zenith_fixtures",
    "fixtures.benchmark_fixture",
    "fixtures.compare_fixtures",
)


def pgbench_init(pg_bin: PgBin, connstr: str):
    pg_bin.run_capture(['pgbench', '-s5', '-i', connstr])


def pgbench_run_5000_transactions(pg_bin: PgBin, connstr: str):
    pg_bin.run_capture(['pgbench', '-c1', '-t5000', connstr])


#
# Run a very short pgbench test.
#
# Collects three metrics:
#
# 1. Time to initialize the pgbench database (pgbench -s5 -i)
# 2. Time to run 5000 pgbench transactions
# 3. Disk space used
#
def test_pgbench(zenith_with_baseline: PgCompare):
    env = zenith_with_baseline

    with env.record_pageserver_writes('pageserver_writes'):
        with env.record_duration('init'):
            pgbench_init(env.pg_bin, env.pg.connstr())
            env.flush()

    with env.record_duration('5000_xacts'):
        pgbench_run_5000_transactions(env.pg_bin, env.pg.connstr())
    env.flush()

    env.report_size()
