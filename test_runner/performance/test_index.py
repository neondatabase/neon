from contextlib import closing
from fixtures.neon_fixtures import NeonEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, NeonBenchmarker
from fixtures.compare_fixtures import PgCompare, VanillaCompare, NeonCompare


#
# Run bulk INSERT test.
#
# Collects metrics:
#
# 1. Time to INSERT 5 million rows
# 2. Disk writes
# 3. Disk space used
# 4. Peak memory usage
#
def test_index(neon_with_baseline: PgCompare):
    env = neon_with_baseline
    records = 1000000
    iterations = 3
    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("create table t(pk bigint)")
            cur.execute("create index on t(pk)")

            # Run INSERT, recording the time and I/O it takes
            with env.record_pageserver_writes('inserts'):
                for i in range(iterations):
                    with env.record_duration('insert'):
                        for pk in range(records):
                            cur.execute(f"insert into t values (random()*{records})")
                        env.flush()
                    with env.record_duration('select'):
                        count = 0
                        for pk in range(records+1):
                            cur.execute(f"delete from t where pk={pk}")
                            count += cur.rowcount
                        assert count == records
                        # cur.execute("vacuum t");

