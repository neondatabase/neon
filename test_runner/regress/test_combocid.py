from fixtures.neon_fixtures import NeonEnvBuilder


def test_combocid(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()
    n_records = 100000

    cur.execute("create table t(id integer, val integer)")
    cur.execute(f"insert into t values (generate_series(1,{n_records}), 0)")

    cur.execute("begin")

    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records

    cur.execute("delete from t")
    assert cur.rowcount == n_records
    cur.execute("delete from t")
    assert cur.rowcount == 0

    cur.execute(f"insert into t values (generate_series(1,{n_records}), 0)")
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records
    cur.execute("update t set val=val+1")
    assert cur.rowcount == n_records

    cur.execute("rollback")
