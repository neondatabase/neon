from contextlib import closing
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


def test_many_reads(zenith_simple_env: ZenithEnv, zenbenchmark: ZenithBenchmarker):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_many_reads", "empty"])

    pg = env.postgres.create_start('test_many_reads')
    log.info("postgres is running on 'test_many_reads' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = env.pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

            cur.execute("create table t (i integer);")

            with zenbenchmark.record_pageserver_writes(env.pageserver, 'pageserver_writes'):
                with zenbenchmark.record_duration('insert'):
                    cur.execute("insert into t values (generate_series(1,100000));")

                with zenbenchmark.record_duration('select'):
                    cur.execute("""do
$do$
begin
  for i in 1..1000 loop
    perform count(*) from t;
  end loop;
end
$do$;
""")

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
