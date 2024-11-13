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
    cur.execute("vacuum t")  # truncates the relation, including its VM and FSM
    # get image of the first block of the VM excluding the page header. It's expected
    # to still be in the buffer cache.
    # ignore page header (24 bytes, 48 - it's hex representation)
    cur.execute("select substr(encode(get_raw_page('t', 'vm', 0), 'hex'), 48)")
    pg_bitmap = cur.fetchall()[0][0]
    # flush shared buffers
    cur.execute("SELECT clear_buffer_cache()")
    # now download the first block of the VM from the pageserver ...
    cur.execute("select substr(encode(get_raw_page('t', 'vm', 0), 'hex'), 48)")
    ps_bitmap = cur.fetchall()[0][0]
    # and check that content of bitmaps are equal, i.e. PS is producing the same VM page as Postgres
    assert pg_bitmap == ps_bitmap
