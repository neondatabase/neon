import random
import time

from fixtures.neon_fixtures import NeonEnv


def test_physical_replication(neon_simple_env: NeonEnv):
    env = neon_simple_env
    n_records = 100000
    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute(
                    "CREATE TABLE t(pk bigint primary key, payload text default repeat('?',200))"
                )
        time.sleep(1)
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    with secondary.connect() as s_con:
                        with s_con.cursor() as s_cur:
                            for pk in range(n_records):
                                p_cur.execute("insert into t (pk) values (%s)", (pk,))
                                s_cur.execute(
                                    "select * from t where pk=%s", (random.randrange(1, n_records),)
                                )
