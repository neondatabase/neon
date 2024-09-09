from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn
from fixtures.pg_version import PgVersion


#
# Test UNLOGGED tables/relations. Postgres copies init fork contents to main
# fork to reset them during recovery. In Neon, pageserver directly sends init
# fork contents as main fork during basebackup.
#
def test_unlogged(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    conn = endpoint.connect()
    cur = conn.cursor()

    cur.execute("CREATE UNLOGGED TABLE iut (id int);")
    # create index to test unlogged index relation as well
    cur.execute("CREATE UNIQUE INDEX iut_idx ON iut (id);")
    cur.execute("ALTER TABLE iut ADD COLUMN seq int GENERATED ALWAYS AS IDENTITY;")
    cur.execute("INSERT INTO iut (id) values (42);")

    # create another compute to fetch inital empty contents from pageserver
    fork_at_current_lsn(env, endpoint, "test_unlogged_basebackup", "main")
    endpoint2 = env.endpoints.create_start("test_unlogged_basebackup")

    conn2 = endpoint2.connect()
    cur2 = conn2.cursor()
    # after restart table should be empty but valid
    cur2.execute("PREPARE iut_plan (int) AS INSERT INTO iut (id) VALUES ($1)")
    cur2.execute("EXECUTE iut_plan (43);")
    cur2.execute("SELECT * FROM iut")
    results = cur2.fetchall()
    # Unlogged sequences were introduced in v15. On <= v14, the sequence created
    # for the GENERATED ALWAYS AS IDENTITY column is logged, and hence it keeps
    # the old value (2) on restart. While on v15 and above, it's unlogged, so it
    # gets reset to 1.
    if env.pg_version <= PgVersion.V14:
        assert results == [(43, 2)]
    else:
        assert results == [(43, 1)]
