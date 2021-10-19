import os
from contextlib import closing
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")


#
# Run a very short pgbench test.
#
# Collects three metrics:
#
# 1. Time to initialize the pgbench database (pgbench -s5 -i)
# 2. Time to run 5000 pgbench transactions
# 3. Disk space used
#
def test_pgbench(postgres: PostgresFactory, pageserver: ZenithPageserver, pg_bin, zenith_cli,
                 zenbenchmark, repo_dir: str):
    # Create a branch for us
    zenith_cli.run(["branch", "test_pgbench_perf", "empty"])

    pg = postgres.create_start('test_pgbench_perf')
    log.info("postgres is running on 'test_pgbench_perf' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = pageserver.connect()
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

    connstr = pg.connstr()

    # Initialize pgbench database, recording the time and I/O it takes
    with zenbenchmark.record_pageserver_writes(pageserver, 'pageserver_writes'):
        with zenbenchmark.record_duration('init'):
            pg_bin.run_capture(['pgbench', '-s5', '-i', connstr])

            # Flush the layers from memory to disk. This is included in the reported
            # time and I/O
            pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")

    # Run pgbench for 5000 transactions
    with zenbenchmark.record_duration('5000_xacts'):
        pg_bin.run_capture(['pgbench', '-c1', '-t5000', connstr])

    # Flush the layers to disk again. This is *not' included in the reported time,
    # though.
    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 0")

    # Report disk space used by the repository
    timeline_size = zenbenchmark.get_timeline_size(repo_dir, pageserver.initial_tenant, timeline)
    zenbenchmark.record('size', timeline_size / (1024 * 1024), 'MB')
