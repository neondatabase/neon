from contextlib import closing
import psycopg2.extras
import random
import time
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Simulate workload causing maximal possible write aplification
#
def test_write_amplification(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli(["branch", "test_write_amplification", "empty"])
    pg = env.postgres.create_start('test_write_amplification')
    n_iterations = 3
    #    n_iterations = 100
    seg_size = 10 * 1024 * 1024
    #    n_segments = 10 * 1024 # 100Gb
    n_segments = 100  # 1Gb
    payload_size = 100
    header_size = 28
    tuple_size = header_size + payload_size
    big_table_size = n_segments * (seg_size // tuple_size)
    checkpoint_distance = 256 * 1024 * 1024
    small_table_size = checkpoint_distance // tuple_size
    redundancy = 2
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            with closing(env.pageserver.connect()) as psconn:
                with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:

                    # Get the timeline ID of our branch. We need it for the 'do_gc' command
                    cur.execute("SHOW zenith.zenith_timeline")
                    timeline = cur.fetchone()[0]

                    # Create a test table
                    cur.execute(
                        "CREATE TABLE Big(pk integer primary key, count integer default 0, payload text default repeat(' ', 100))"
                    )
                    # populate ~100Gb in Big
                    cur.execute(
                        f"INSERT INTO Big (pk) values (generate_series(1,{big_table_size}))")
                    cur.execute(
                        f"CREATE TABLE Small(pk integer, payload text default repeat(' ', {payload_size}))"
                    )
                    for i in range(n_iterations):
                        print(f"Iteration {i}")
                        # update random keys
                        for i in range(n_segments * redundancy):
                            key = random.randint(1, big_table_size)
                            cur.execute(f"update Big set count=count+1 where pk={key}")

                        # bulk insert small table to trigger checkpoint distance
                        cur.execute(
                            f"insert into Small (pk) values (generate_series(1, {small_table_size}))"
                        )
                        # And truncate it to avoid database size explosion
                        cur.execute("truncate Small")
                        # Run GC to force checkpoint and save image layers
                        pscur.execute(f"do_gc {env.initial_tenant} {timeline} 0")
                        row = pscur.fetchone()
                        print_gc_result(row)
    # Wait forever to be able to inspect statistic
    print('Test completed')


#    time.sleep(100000)
