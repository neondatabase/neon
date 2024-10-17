import threading

from fixtures.neon_fixtures import NeonEnv


def test_compute_restart(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    sub_timeline_id = env.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 100000
    n_restarts = 200

    def insert_data(pub):
        with pub.cursor() as pcur:
            for i in range(0, n_records):
                pcur.execute("INSERT into t values (%s,random()*100000)", (i,))

    with pub.cursor() as pcur:
        with sub.cursor() as scur:
            pcur.execute("CREATE TABLE t (pk integer primary key, sk integer)")
            scur.execute("CREATE TABLE t (pk integer primary key, sk integer)")

        thread = threading.Thread(target=insert_data, args=(pub,), daemon=True)
        thread.start()

        for _ in range(n_restarts):
            # restart subscriber
            # time.sleep(2)
            sub.stop("immediate", sks_wait_walreceiver_gone=(env.safekeepers, sub_timeline_id))
            sub.start()

        thread.join()
