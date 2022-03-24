import time

from fixtures.zenith_fixtures import ZenithEnvBuilder


# Test restarting page server, while safekeeper and compute node keep
# running.
def test_next_xid(zenith_env_builder: ZenithEnvBuilder):
    # One safekeeper is enough for this test.
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()

    pg = env.postgres.create_start('main')

    conn = pg.connect()
    cur = conn.cursor()
    cur.execute('CREATE TABLE t(x integer)')

    iterations = 32
    for i in range(1, iterations + 1):
        print(f'iteration {i} / {iterations}')

        # Kill and restart the pageserver.
        pg.stop()
        env.pageserver.stop(immediate=True)
        env.pageserver.start()
        pg.start()

        retry_sleep = 0.5
        max_retries = 200
        retries = 0
        while True:
            try:
                conn = pg.connect()
                cur = conn.cursor()
                cur.execute(f"INSERT INTO t values({i})")
                conn.close()

            except Exception as error:
                # It's normal that it takes some time for the pageserver to
                # restart, and for the connection to fail until it does. It
                # should eventually recover, so retry until it succeeds.
                print(f'failed: {error}')
                if retries < max_retries:
                    retries += 1
                    print(f'retry {retries} / {max_retries}')
                    time.sleep(retry_sleep)
                    continue
                else:
                    raise
            break

    conn = pg.connect()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM t")
    assert cur.fetchone() == (iterations, )
