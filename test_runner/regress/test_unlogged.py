from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn


#
# Test UNLOGGED tables/relations. Postgres copies init fork contents to main
# fork to reset them during recovery. In Neon, pageserver directly sends init
# fork contents as main fork during basebackup.
#
def test_unlogged(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_unlogged", "empty")
    pg = env.postgres.create_start("test_unlogged")

    conn = pg.connect()
    cur = conn.cursor()

    cur.execute("CREATE UNLOGGED TABLE iut (id int);")
    # create index to test unlogged index relation as well
    cur.execute("CREATE UNIQUE INDEX iut_idx ON iut (id);")
    cur.execute("INSERT INTO iut values (42);")

    # create another compute to fetch inital empty contents from pageserver
    fork_at_current_lsn(env, pg, "test_unlogged_basebackup", "test_unlogged")
    pg2 = env.postgres.create_start(
        "test_unlogged_basebackup",
    )

    conn2 = pg2.connect()
    cur2 = conn2.cursor()
    # after restart table should be empty but valid
    cur2.execute("PREPARE iut_plan (int) AS INSERT INTO iut VALUES ($1)")
    cur2.execute("EXECUTE iut_plan (43);")
    cur2.execute("SELECT * FROM iut")
    assert cur2.fetchall() == [(43,)]
