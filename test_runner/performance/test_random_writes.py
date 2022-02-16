import os
from contextlib import closing
from fixtures.benchmark_fixture import MetricReport
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.compare_fixtures import PgCompare, VanillaCompare, ZenithCompare
from fixtures.log_helper import log

import psycopg2.extras
import random
import time
from fixtures.utils import print_gc_result


def test_random_writes(zenith_with_baseline: PgCompare):
    env = zenith_with_baseline

    # Directly impacts the effective checkpoint distance.
    # Smaller value makes the test run faster. Larger value is more realistic.
    # Doesn't affect the zenith / vanilla performance ratio.
    n_rows = 1 * 1000 * 1000  # around 36 MB table

    # Relative measure of write density. Doubling the load factor doubles
    # average writes per page.
    load_factor = 1

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            # Create the test table
            with env.record_duration('init'):
                cur.execute("""
                    CREATE TABLE Big(
                        pk integer primary key,
                        count integer default 0
                    );
                """)
                cur.execute(f"INSERT INTO Big (pk) values (generate_series(1,{n_rows}))")

            # Get table size (can't be predicted because padding and alignment)
            cur.execute("SELECT pg_relation_size('Big');")
            row = cur.fetchone()
            table_size = row[0]
            env.zenbenchmark.record("table_size", table_size, 'bytes', MetricReport.TEST_PARAM)

            # Decide how much to write, based on knowledge of pageserver implementation.
            # Avoiding segment collisions maximizes (zenith_runtime / vanilla_runtime).
            segment_size = 10 * 1024 * 1024
            n_segments = table_size // segment_size
            n_writes = load_factor * n_segments // 3

            # The closer this is to 250 MB, the more realistic the test is.
            effective_checkpoint_distance = table_size * n_writes // n_rows
            env.zenbenchmark.record("effective_checkpoint_distance",
                                    effective_checkpoint_distance,
                                    'bytes',
                                    MetricReport.TEST_PARAM)

            # update random keys
            with env.record_duration('run'):
                for it in range(3000):
                    for i in range(n_writes):
                        key = random.randint(1, n_rows)
                        cur.execute(f"update Big set count=count+1 where pk={key}")
                    env.flush()
