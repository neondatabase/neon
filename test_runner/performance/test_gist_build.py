import os
from contextlib import closing
from fixtures.benchmark_fixture import MetricReport
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare
from fixtures.log_helper import log

pytest_plugins = (
    "fixtures.zenith_fixtures",
    "fixtures.benchmark_fixture",
    "fixtures.compare_fixtures",
)


#
# Test buffering GisT build. It WAL-logs the whole relation, in 32-page chunks.
# As of this writing, we're duplicate those giant WAL records for each page,
# which makes the delta layer about 32x larger than it needs to be.
#
def test_gist_buffering_build(zenith_with_baseline: PgCompare):
    env = zenith_with_baseline

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:

            # Create test table.
            cur.execute("create table gist_point_tbl(id int4, p point)")
            cur.execute(
                "insert into gist_point_tbl select g, point(g, g) from generate_series(1, 1000000) g;"
            )

            # Build the index.
            with env.record_pageserver_writes('pageserver_writes'):
                with env.record_duration('build'):
                    cur.execute(
                        "create index gist_pointidx2 on gist_point_tbl using gist(p) with (buffering = on)"
                    )
                    env.flush()

            env.report_peak_memory_use()
            env.report_size()
