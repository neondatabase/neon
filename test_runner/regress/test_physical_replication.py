from __future__ import annotations

import random
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fixtures.neon_fixtures import NeonEnv


def test_physical_replication(neon_simple_env: NeonEnv):
    env = neon_simple_env
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
                            runtime_secs = 30
                            started_at = time.time()
                            pk = 0
                            while True:
                                pk += 1
                                now = time.time()
                                if now - started_at > runtime_secs:
                                    break
                                p_cur.execute("insert into t (pk) values (%s)", (pk,))
                                # an earlier version of this test was based on a fixed number of loop iterations
                                # and selected for pk=(random.randrange(1, fixed number of loop iterations)).
                                # => the probability of selection for a value that was never inserted changed from 99.9999% to 0% over the course of the test.
                                #
                                # We changed the test to where=(random.randrange(1, 2*pk)), which means the probability is now fixed to 50%.
                                s_cur.execute(
                                    "select * from t where pk=%s", (random.randrange(1, 2 * pk),)
                                )
