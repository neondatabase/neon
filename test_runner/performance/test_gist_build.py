import os
from contextlib import closing
from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures", "fixtures.benchmark_fixture")

#
# Test buffering GisT build. It WAL-logs the whole relation, in 32-page chunks.
# As of this writing, we're duplicate those giant WAL records for each page,
# which makes the delta layer about 32x larger than it needs to be.
#
def test_gist_buffering_build(postgres: PostgresFactory, pageserver: ZenithPageserver, pg_bin, zenith_cli, zenbenchmark, repo_dir: str):
    # Create a branch for us
    zenith_cli.run(["branch", "test_gist_buffering_build", "empty"])

    pg = postgres.create_start('test_gist_buffering_build')
    log.info("postgres is running on 'test_gist_buffering_build' branch")

    # Open a connection directly to the page server that we'll use to force
    # flushing the layers to disk
    psconn = pageserver.connect();
    pscur = psconn.cursor()

    # Get the timeline ID of our branch. We need it for the 'do_gc' command
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW zenith.zenith_timeline")
            timeline = cur.fetchone()[0]

            # Create test table.
            cur.execute("create table gist_point_tbl(id int4, p point)");
            cur.execute("insert into gist_point_tbl select g, point(g, g) from generate_series(1, 1000000) g;");

            # Build the index.
            with zenbenchmark.record_pageserver_writes(pageserver, 'pageserver_writes'):
                with zenbenchmark.record_duration('build'):
                    cur.execute("create index gist_pointidx2 on gist_point_tbl using gist(p) with (buffering = on)");

                    # Flush the layers from memory to disk. This is included in the reported
                    # time and I/O
                    pscur.execute(f"do_gc {pageserver.initial_tenant} {timeline} 1000000")

            # Record peak memory usage
            zenbenchmark.record("peak_mem", zenbenchmark.get_peak_mem(pageserver) / 1024, 'MB')

            # Report disk space used by the repository
            timeline_size = zenbenchmark.get_timeline_size(repo_dir, pageserver.initial_tenant, timeline)
            zenbenchmark.record('size', timeline_size / (1024*1024), 'MB')
