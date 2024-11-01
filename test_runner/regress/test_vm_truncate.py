from fixtures.neon_fixtures import NeonEnv


#
# Test that VM is properly truncated
#
def test_vm_truncate(neon_simple_env: NeonEnv):
    env = neon_simple_env

    endpoint = env.endpoints.create_start("main")
    con = endpoint.connect()
    cur = con.cursor()
    cur.execute("CREATE EXTENSION neon_test_utils")
    cur.execute("CREATE EXTENSION pageinspect")

    cur.execute(
        "create table t(pk integer primary key, counter integer default 0, filler text default repeat('?', 200))"
    )
    cur.execute("insert into t (pk) values (generate_series(1,1000))")
    cur.execute("delete from t where pk>10")
    cur.execute("vacuum t")
    cur.execute("select substr(encode(get_raw_page('t', 'vm', 0), 'hex'), 48)")
    pg_bitmap = cur.fetchall()[0][0]
    cur.execute("SELECT clear_buffer_cache()")
    cur.execute("select substr(encode(get_raw_page('t', 'vm', 0), 'hex'), 48)")
    ps_bitmap = cur.fetchall()[0][0]
    assert pg_bitmap == ps_bitmap
