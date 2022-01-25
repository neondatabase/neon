import pytest
import random
import time

from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_next_xid(zenith_env_builder: ZenithEnvBuilder):
    # One safekeeper is enough for this test.
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init()

    pg = env.postgres.create_start('main')

    conn = pg.connect()
    cur = conn.cursor()

    cur.execute('CREATE TABLE t(x integer)')

    iterations = 32
    for i in range(1, iterations + 1):
        print(f'iteration {i} / {iterations}')

        # Stop and restart pageserver. This is a more or less graceful shutdown, although
        # the page server doesn't currently have a shutdown routine so there's no difference
        # between stopping and crashing.
        pg.stop()
        env.pageserver.stop(immediate=True)
        env.pageserver.start()
        pg.start()

        retry_sleep = 2
        max_retries = 2000
        retries = 0
        while True:
            try:
                conn = pg.connect()
                cur = conn.cursor()
                cur.execute(f"INSERT INTO t values({i})")
                conn.close()

            except Exception as error:
                # It's normal that it takes some time for the containers to restart,
                # and the components to notice broken connections and such. So if a
                # connection fails, retry it. It should eventually recover.
                print(f'failed: {error}')
                if retries < max_retries:
                    retries += 1
                    print(f'retry {retries} / {max_retries}')
                    time.sleep(retry_sleep)

                    # Establish a new connection for the next retry. It's normal for the
                    # connection to be broken if the proxy or the compute node was restarted.
                    continue
                else:
                    raise
            break

    conn = pg.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM t")
    assert cur.fetchone() == (iterations, )
