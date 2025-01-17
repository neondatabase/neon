from __future__ import annotations

import random
from contextlib import closing

from fixtures.benchmark_fixture import MetricReport
from fixtures.compare_fixtures import PgCompare
from fixtures.utils import query_scalar


# This is a clear-box test that demonstrates the worst case scenario for the
# "1 segment per layer" implementation of the pageserver. It writes to random
# rows, while almost never writing to the same segment twice before flushing.
# A naive pageserver implementation would create a full image layer for each
# dirty segment, leading to write_amplification = segment_size / page_size,
# when compared to vanilla postgres. With segment_size = 10MB, that's 1250.
def test_random_writes(neon_with_baseline: PgCompare):
    env = neon_with_baseline

    # Number of rows in the test database. 1M rows runs quickly, but implies
    # a small effective_checkpoint_distance, which makes the test less realistic.
    # Using a 300 TB database would imply a 250 MB effective_checkpoint_distance,
    # but it will take a very long time to run. From what I've seen so far,
    # increasing n_rows doesn't have impact on the (neon_runtime / vanilla_runtime)
    # performance ratio.
    n_rows = 1 * 1000 * 1000  # around 36 MB table

    # Number of writes per 3 segments. A value of 1 should produce a random
    # workload where we almost never write to the same segment twice. Larger
    # values of load_factor produce a larger effective_checkpoint_distance,
    # making the test more realistic, but less effective. If you want a realistic
    # worst case scenario and you have time to wait you should increase n_rows instead.
    load_factor = 1

    # Not sure why but this matters in a weird way (up to 2x difference in perf).
    # TODO look into it
    n_iterations = 1

    with closing(env.pg.connect()) as conn:
        with conn.cursor() as cur:
            # Create the test table
            with env.record_duration("init"):
                cur.execute(
                    """
                    CREATE TABLE Big(
                        pk integer primary key,
                        count integer default 0
                    );
                """
                )

                # Insert n_rows in batches to avoid query timeouts
                rows_inserted = 0
                while rows_inserted < n_rows:
                    rows_to_insert = min(1000 * 1000, n_rows - rows_inserted)
                    low = rows_inserted + 1
                    high = rows_inserted + rows_to_insert
                    cur.execute(f"INSERT INTO Big (pk) values (generate_series({low},{high}))")
                    rows_inserted += rows_to_insert

            # Get table size (can't be predicted because padding and alignment)
            table_size = query_scalar(cur, "SELECT pg_relation_size('Big')")
            env.zenbenchmark.record("table_size", table_size, "bytes", MetricReport.TEST_PARAM)

            # Decide how much to write, based on knowledge of pageserver implementation.
            # Avoiding segment collisions maximizes (neon_runtime / vanilla_runtime).
            segment_size = 10 * 1024 * 1024
            n_segments = table_size // segment_size
            n_writes = load_factor * n_segments // 3

            # The closer this is to 250 MB, the more realistic the test is.
            effective_checkpoint_distance = table_size * n_writes // n_rows
            env.zenbenchmark.record(
                "effective_checkpoint_distance",
                effective_checkpoint_distance,
                "bytes",
                MetricReport.TEST_PARAM,
            )

            # Update random keys
            with env.record_duration("run"):
                for _ in range(n_iterations):
                    for _ in range(n_writes):
                        key = random.randint(1, n_rows)
                        cur.execute(f"update Big set count=count+1 where pk={key}")
                    env.flush()
