# Test sequential scan speed
#
# The test table is large enough (3-4 MB) that it doesn't fit in the compute node
# cache, so the seqscans go to the page server. But small enough that it fits
# into memory in the page server.
from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def test_small_seqscans(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_small_seqscans", "empty"])

    pg = env.postgres.create_start('test_small_seqscans')
    log.info("postgres is running on 'test_small_seqscans' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('create table t (i integer);')
            cur.execute('insert into t values (generate_series(1,100000));')

            # Verify that the table is larger than shared_buffers
            cur.execute('''
            select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('t') as tbl_ize
            from pg_settings where name = 'shared_buffers'
            ''')
            row = cur.fetchone()
            log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
            assert int(row[0]) < int(row[1])

            with zenbenchmark.record_duration('run'):
                for i in range(1000):
                    cur.execute('select count(*) from t;')
