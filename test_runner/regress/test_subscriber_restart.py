from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import wait_until


def test_subscriber_restart(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("publisher")
    pub = env.endpoints.create("publisher")
    pub.start()

    env.neon_cli.create_branch("subscriber")
    sub = env.endpoints.create("subscriber")
    sub.start()

    n_records = 1000
    with pub.cursor() as pcur:
        with sub.cursor() as scur:
            pcur.execute("CREATE TABLE t (pk integer primary key)")
            pcur.execute("CREATE PUBLICATION pub FOR TABLE t")
            scur.execute("CREATE TABLE t (pk integer primary key)")
            pub_conn = f"host=localhost port={pub.pg_port} dbname=postgres user=cloud_admin"
            query = f"CREATE SUBSCRIPTION sub CONNECTION '{pub_conn}' PUBLICATION pub"
            scur.execute(query)
            for i in range(0, n_records):
                pcur.execute("INSERT into t values (%s)", (i,))

            def check_that_changes_propagated():
                scur.execute("SELECT count(*) FROM t")
                res = scur.fetchall()
                assert res[0][0] == n_records

        # restart subscriber
        sub.stop()
        sub.start()
        pcur.execute("INSERT into t values (1000)")
        n_records += 1
        with sub.cursor() as scur:
            wait_until(10, 0.5, check_that_changes_propagated)
