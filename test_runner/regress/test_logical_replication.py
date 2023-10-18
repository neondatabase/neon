import time

from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    logical_replication_sync,
    wait_for_last_flush_lsn,
)


def test_logical_replication(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start(
        "test_logical_replication", config_lines=["log_statement=all"]
    )

    log.info("postgres is running on 'test_logical_replication' branch")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    cur.execute("create table t(pk integer primary key, payload integer)")
    cur.execute(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));"
    )
    cur.execute("create publication pub1 for table t, replication_example")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, payload integer)")
    vanilla_pg.safe_psql(
        "CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120), testcolumn1 int, testcolumn2 int, testcolumn3 int);"
    )
    connstr = endpoint.connstr().replace("'", "''")
    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Wait logical replication channel to be established
    logical_replication_sync(vanilla_pg, endpoint)

    # insert some data
    cur.execute("insert into t values (generate_series(1,1000), 0)")

    # Wait logical replication to sync
    logical_replication_sync(vanilla_pg, endpoint)
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
    logical_replication_sync(vanilla_pg, endpoint)

    # Check that subscribers receives all data
    assert vanilla_pg.safe_psql("select count(*) from t")[0][0] == 2000

    # Test that save/restore of RewriteMappingFile works. Partial copy of
    # rewrite.sql test.
    log.info("checking rewriteheap")
    vanilla_pg.stop()
    cmds = """
INSERT INTO replication_example(somedata) VALUES (1);

BEGIN;
INSERT INTO replication_example(somedata) VALUES (2);
ALTER TABLE replication_example ADD COLUMN testcolumn1 int;
INSERT INTO replication_example(somedata, testcolumn1) VALUES (3,  1);
COMMIT;

BEGIN;
INSERT INTO replication_example(somedata) VALUES (3);
ALTER TABLE replication_example ADD COLUMN testcolumn2 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn2) VALUES (4,  2, 1);
COMMIT;

VACUUM FULL pg_am;
VACUUM FULL pg_amop;
VACUUM FULL pg_proc;
VACUUM FULL pg_opclass;
VACUUM FULL pg_type;
VACUUM FULL pg_index;
VACUUM FULL pg_database;


-- repeated rewrites that fail
BEGIN;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
ROLLBACK;

-- repeated rewrites that succeed
BEGIN;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
CLUSTER pg_class USING pg_class_oid_index;
COMMIT;

-- repeated rewrites in different transactions
VACUUM FULL pg_class;
VACUUM FULL pg_class;

-- reindexing of important relations / indexes
REINDEX TABLE pg_class;
REINDEX INDEX pg_class_oid_index;
REINDEX INDEX pg_class_tblspc_relfilenode_index;

INSERT INTO replication_example(somedata, testcolumn1) VALUES (5, 3);

BEGIN;
INSERT INTO replication_example(somedata, testcolumn1) VALUES (6, 4);
ALTER TABLE replication_example ADD COLUMN testcolumn3 int;
INSERT INTO replication_example(somedata, testcolumn1, testcolumn3) VALUES (7, 5, 1);
COMMIT;
"""
    endpoint.safe_psql_many([q for q in cmds.splitlines() if q != "" and not q.startswith("-")])

    # refetch rewrite files from pageserver
    endpoint.stop()
    endpoint.start()

    vanilla_pg.start()
    logical_replication_sync(vanilla_pg, endpoint)
    eq_q = "select testcolumn1, testcolumn2, testcolumn3 from replication_example order by 1, 2, 3"
    assert vanilla_pg.safe_psql(eq_q) == endpoint.safe_psql(eq_q)
    log.info("rewriteheap synced")

    # test that removal of repl slots works across restart
    vanilla_pg.stop()
    time.sleep(1)  # wait for conn termination; active slots can't be dropped
    endpoint.safe_psql("select pg_drop_replication_slot('sub1');")
    endpoint.safe_psql("insert into t values (2001, 1);")  # forces WAL flush
    # wait for drop message to reach safekeepers (it is not transactional)
    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop()
    endpoint.start()
    # it must be gone (but walproposer slot still exists, hence 1)
    assert endpoint.safe_psql("select count(*) from pg_replication_slots")[0][0] == 1
