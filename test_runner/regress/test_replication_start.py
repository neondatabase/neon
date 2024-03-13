from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, wait_replica_caughtup


def test_replication_start(neon_simple_env: NeonEnv):
    env = neon_simple_env

    with env.endpoints.create_start(branch_name="main", endpoint_id="primary") as primary:
        with primary.connect() as p_con:
            with p_con.cursor() as p_cur:
                p_cur.execute("begin")
                p_cur.execute("create table t(pk integer primary key, payload integer)")
                p_cur.execute("insert into t values (generate_series(1,100000), 0)")
                p_cur.execute("select txid_current()")
                xid = p_cur.fetchall()[0][0]
                log.info(f"Master transaction {xid}")
                with env.endpoints.new_replica_start(
                    origin=primary, endpoint_id="secondary"
                ) as secondary:
                    wait_replica_caughtup(primary, secondary)
                    with secondary.connect() as s_con:
                        with s_con.cursor() as s_cur:
                            # Enforce setting hint bits for pg_class tuples.
                            # If master's transaction is not marked as in-progress in MVCC snapshot,
                            # then XMIN_INVALID hint bit will be set for table's 't' tuple makeing it invisible.
                            s_cur.execute("select * from pg_class")
                            p_cur.execute("commit")
                            wait_replica_caughtup(primary, secondary)
                            s_cur.execute("select * from t where pk = 1")
                            assert s_cur.fetchone() == (1, 0)
