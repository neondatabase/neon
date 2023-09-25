import time
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv

def test_logical_replication(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start("test_logical_replication")

    log.info("postgres is running on 'test_logical_replication' branch")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute("create publication pub1 for table t")
#    cur.execute("SELECT DISTINCT t.schemaname, t.tablename, t.attnames FROM pg_catalog.pg_publication_tables t WHERE t.pubname IN ('pub1')")
#    time.sleep(1)

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    connstr = endpoint.connstr().replace("'", "''")
    print(f"connstr='{connstr}'")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    time.sleep(1)

    # insert some data
    cur.execute("insert into t values (generate_series(1,1000), 0)")

    # Wait logical replication to sync
    time.sleep(1)
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 1000

    # now stop subscriber...
    vanilla_pg.stop()

    # ... and insert some more data which should be delivered to subscriber after restart
    cur.execute("insert into t values (generate_series(1001,2000), 0)")

    # Restart compute
    endpoint.stop()
    endpoint.start()

    # start subscriber
    vanilla_pg.start()

    # Wait logical replication to sync
    time.sleep(2)

    # Check that subscribers receives all data
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 2000
