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
            env.pg_bin.run_capture(['pgbench', '-s5', '-i', env.pg.connstr()])
            env.flush()

    with env.record_duration('5000_xacts'):
        env.pg_bin.run_capture(['pgbench', '-c1', '-t5000', env.pg.connstr()])
    env.flush()

    env.report_size()
