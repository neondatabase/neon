from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


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
def test_bulk_insert(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_bulk_insert", "empty"])

    pg = env.postgres.create_start('test_bulk_insert')
    log.info("postgres is running on 'test_bulk_insert' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

            cur.execute("create table huge (i int, j int);")

            # Run INSERT, recording the time and I/O it takes
            with zenbenchmark.record_pageserver_writes(env.pageserver, 'pageserver_writes'):
                with zenbenchmark.record_duration('insert'):
                    cur.execute("insert into huge values (generate_series(1, 5000000), 0);")

                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")

            # Record peak memory usage
            zenbenchmark.record("peak_mem",
                                zenbenchmark.get_peak_mem(env.pageserver) / 1024,
                                'MB',
                                report=MetricReport.LOWER_IS_BETTER)

            # Report disk space used by the repository
            timeline_size = zenbenchmark.get_timeline_size(env.repo_dir,
                                                           env.initial_tenant,
                                                           timeline)
            zenbenchmark.record('size',
                                timeline_size / (1024 * 1024),
                                'MB',
                                report=MetricReport.LOWER_IS_BETTER)
