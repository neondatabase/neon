import time

from fixtures.neon_fixtures import NeonEnv


def test_hot_standby(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
    ) as primary:
        time.sleep(1)
        with env.endpoints.new_replica_start(origin=primary, endpoint_id="secondary") as secondary:
            primary_lsn = None
            caught_up = False
            queries = [
                "SHOW neon.timeline_id",
                "SHOW neon.tenant_id",
                "SELECT relname FROM pg_class WHERE relnamespace = current_schema()::regnamespace::oid",
                "SELECT COUNT(*), SUM(i) FROM test",
            ]
            responses = dict()

            with primary.connect() as p_con:
                with p_con.cursor() as p_cur:
                    p_cur.execute("CREATE TABLE test AS SELECT generate_series(1, 100) AS i")

                # Explicit commit to make sure other connections (and replicas) can
                # see the changes of this commit.
                p_con.commit()

                with p_con.cursor() as p_cur:
                    p_cur.execute("SELECT pg_current_wal_insert_lsn()::text")
                    res = p_cur.fetchone()
                    assert res is not None
                    (lsn,) = res
                    primary_lsn = lsn

                # Explicit commit to make sure other connections (and replicas) can
                # see the changes of this commit.
                # Note that this may generate more WAL if the transaction has changed
                # things, but we don't care about that.
                p_con.commit()

                for query in queries:
                    with p_con.cursor() as p_cur:
                        p_cur.execute(query)
                        res = p_cur.fetchone()
                        assert res is not None
                        response = res
                        responses[query] = response

            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    s_cur.execute("SELECT 1 WHERE pg_is_in_recovery()")
                    res = s_cur.fetchone()
                    assert res is not None

                while not caught_up:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute("SELECT pg_last_wal_replay_lsn()")
                        res = secondary_cursor.fetchone()
                        assert res is not None
                        (secondary_lsn,) = res
                        # There may be more changes on the primary after we got our LSN
                        # due to e.g. autovacuum, but that shouldn't impact the content
                        # of the tables, so we check whether we've replayed up to at
                        # least after the commit of the `test` table.
                        caught_up = secondary_lsn >= primary_lsn

                # Explicit commit to flush any transient transaction-level state.
                s_con.commit()

                for query in queries:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute(query)
                        response = secondary_cursor.fetchone()
                        assert response is not None
                        assert response == responses[query]
