from fixtures.neon_fixtures import NeonEnvBuilder


def test_combocid_delete(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    endpoint = env.endpoints.create_start(
        "main",
        config_lines=[
            "shared_buffers='1MB'",
        ],
    )

    conn = endpoint.connect()
    cur = conn.cursor()
    n_records = 1000

    cur.execute("CREATE EXTENSION neon_test_utils")

    cur.execute("create table t(id integer, val integer)")

    cur.execute("begin")
    cur.execute("insert into t values (1, 0)")
    cur.execute("insert into t values (2, 0)")
    cur.execute(f"insert into t select g, 0 from generate_series(3,{n_records}) g")

    # Open a cursor that scroll it halfway through
    cur.execute("DECLARE c1 NO SCROLL CURSOR WITHOUT HOLD FOR SELECT * FROM t")
    cur.execute("fetch 500 from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    # Delete all the rows. Because all of the rows were inserted earlier in the
    # same transaction, all the rows will get a combocid.
    cur.execute("delete from t")

    # Clear the cache, so that we exercise reconstructing the pages
    # from WAL
    cur.execute("SELECT clear_buffer_cache()")

    # Check that the cursor opened earlier still works. If the
    # combocids are not restored correctly, it won't.
    cur.execute("fetch all from c1")
    rows = cur.fetchall()
    assert len(rows) == 500

    cur.execute("rollback")


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
