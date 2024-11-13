from fixtures.neon_fixtures import NeonEnv


def test_on_demand_wal_download(neon_simple_env: NeonEnv):
    env = neon_simple_env
    ep = env.endpoints.create_start(
        branch_name="main",
        endpoint_id="primary",
        config_lines=[
            "max_wal_size=32MB",
            "min_wal_size=32MB",
            "neon.logical_replication_max_snap_files=10000",
        ],
    )

    con = ep.connect()
    cur = con.cursor()
    cur.execute("CREATE TABLE t(pk bigint primary key, payload text)")
    cur.execute("ALTER TABLE t ALTER payload SET STORAGE external")
    cur.execute("select pg_create_logical_replication_slot('myslot', 'test_decoding', false, true)")
    cur.execute("insert into t values (generate_series(1,100000),repeat('?',10000))")

    ep.stop("fast")
    ep.start()
    con = ep.connect()
    cur = con.cursor()
    cur.execute("select pg_replication_slot_advance('myslot', pg_current_wal_insert_lsn())")
