import pytest
from fixtures.neon_fixtures import NeonEnv


@pytest.mark.timeout(1800)
def test_hot_standby(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.postgres.create_start(
        branch_name="main",
        node_name="primary",
    ) as primary:
        result = None
        with env.postgres.new_replica_start(origin=primary, name="secondary") as secondary:
            primary_lsn = None
            cought_up = False
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
                p_con.commit()

                with p_con.cursor() as p_cur:
                    p_cur.execute("SELECT pg_current_wal_insert_lsn()::text")
                    [lsn] = p_cur.fetchone()
                    primary_lsn = lsn
                p_con.commit()

                for query in queries:
                    with p_con.cursor() as p_cur:
                        p_cur.execute(query)
                        response = p_cur.fetchone()
                        responses[query] = response

            with secondary.connect() as s_con:
                with s_con.cursor() as s_cur:
                    s_cur.execute("SHOW transaction_read_only")
                    (result,) = s_cur.fetchone()
                    assert result == "on"

                while not cought_up:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute("SELECT pg_last_wal_replay_lsn()")
                        [secondary_lsn] = secondary_cursor.fetchone()
                        cought_up = secondary_lsn >= primary_lsn

                s_con.commit()

                for query in queries:
                    with s_con.cursor() as secondary_cursor:
                        secondary_cursor.execute(query)
                        result = secondary_cursor.fetchone()
                        assert result == responses[query]
