import time
from random import choice
from string import ascii_lowercase

import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import (
    NeonEnv,
    logical_replication_sync,
    wait_for_last_flush_lsn,
)
from fixtures.types import Lsn
from fixtures.utils import query_scalar


def random_string(n: int):
    return "".join([choice(ascii_lowercase) for _ in range(n)])


def test_logical_replication(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    tenant_id = env.initial_tenant
    timeline_id = env.neon_cli.create_branch("test_logical_replication", "empty")
    endpoint = env.endpoints.create_start(
        "test_logical_replication", config_lines=["log_statement=all"]
    )

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


# Test compute start at LSN page of which starts with contrecord
# https://github.com/neondatabase/neon/issues/5749
def test_wal_page_boundary_start(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    endpoint = env.endpoints.create_start("init")
    tenant_id = endpoint.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = endpoint.safe_psql("show neon.timeline_id")[0][0]

    cur = endpoint.connect().cursor()
    cur.execute("create table t(key int, value text)")
    cur.execute("CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int);")
    cur.execute("insert into replication_example values (1, 2)")
    cur.execute("create publication pub1 for table replication_example")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("create table t(pk integer primary key, value text)")
    vanilla_pg.safe_psql("CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int);")

    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")
    logical_replication_sync(vanilla_pg, endpoint)
    vanilla_pg.stop()

    with endpoint.cursor() as cur:
        # measure how much space logical message takes. Sometimes first attempt
        # creates huge message and then it stabilizes, have no idea why.
        for _ in range(3):
            lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            log.info(f"current_lsn={lsn_before}")
            # Non-transactional logical message doesn't write WAL, only XLogInsert's
            # it, so use transactional. Which is a bit problematic as transactional
            # necessitates commit record. Alternatively we can do smth like
            #   select neon_xlogflush(pg_current_wal_insert_lsn());
            # but isn't much better + that particular call complains on 'xlog flush
            # request 0/282C018 is not satisfied' as pg_current_wal_insert_lsn skips
            # page headers.
            payload = "blahblah"
            cur.execute(f"select pg_logical_emit_message(true, 'pref', '{payload}')")
            lsn_after_by_curr_wal_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
            lsn_diff = lsn_after_by_curr_wal_lsn - lsn_before
            logical_message_base = lsn_after_by_curr_wal_lsn - lsn_before - len(payload)
            log.info(
                f"before {lsn_before}, after {lsn_after_by_curr_wal_lsn}, lsn diff is {lsn_diff}, base {logical_message_base}"
            )

        # and write logical message spanning exactly as we want
        lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"current_lsn={lsn_before}")
        curr_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        offs = int(curr_lsn) % 8192
        till_page = 8192 - offs
        payload_len = (
            till_page - logical_message_base - 8
        )  # not sure why 8 is here, it is deduced from experiments
        log.info(f"current_lsn={curr_lsn}, offs {offs}, till_page {till_page}")

        # payload_len above would go exactly till the page boundary; but we want contrecord, so make it slightly longer
        payload_len += 8

        cur.execute(f"select pg_logical_emit_message(true, 'pref', 'f{'a' * payload_len}')")
        supposedly_contrecord_end = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"supposedly_page_boundary={supposedly_contrecord_end}")
        # The calculations to hit the page boundary are very fuzzy, so just
        # ignore test if we fail to reach it.
        if not (int(supposedly_contrecord_end) % 8192 == 32):
            pytest.skip("missed page boundary, bad luck")

        cur.execute("insert into replication_example values (2, 3)")

    wait_for_last_flush_lsn(env, endpoint, tenant_id, timeline_id)
    endpoint.stop().start()

    cur = endpoint.connect().cursor()
    # this should flush current wal page
    cur.execute("insert into replication_example values (3, 4)")
    vanilla_pg.start()
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql(
        "select sum(somedata) from replication_example"
    ) == endpoint.safe_psql("select sum(somedata) from replication_example")


# Test that WAL redo works for fairly large records.
#
# See https://github.com/neondatabase/neon/pull/6534. That wasn't a
# logical replication bug as such, but without logical replication,
# records passed ot the WAL redo process are never large enough to hit
# the bug.
def test_large_records(neon_simple_env: NeonEnv, vanilla_pg):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    endpoint = env.endpoints.create_start("init")

    cur = endpoint.connect().cursor()
    cur.execute("CREATE TABLE reptbl(id int, largeval text);")
    cur.execute("alter table reptbl replica identity full")
    cur.execute("create publication pub1 for table reptbl")

    # now start subscriber
    vanilla_pg.start()
    vanilla_pg.safe_psql("CREATE TABLE reptbl(id int, largeval text);")

    log.info(f"ep connstr is {endpoint.connstr()}, subscriber connstr {vanilla_pg.connstr()}")
    connstr = endpoint.connstr().replace("'", "''")
    vanilla_pg.safe_psql(f"create subscription sub1 connection '{connstr}' publication pub1")

    # Test simple insert, update, delete. But with very large values
    value = random_string(10_000_000)
    cur.execute(f"INSERT INTO reptbl VALUES (1, '{value}')")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(1, value)]

    # Test delete, and reinsert another value
    cur.execute("DELETE FROM reptbl WHERE id = 1")
    cur.execute(f"INSERT INTO reptbl VALUES (2, '{value}')")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]

    value = random_string(10_000_000)
    cur.execute(f"UPDATE reptbl SET largeval='{value}'")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]

    endpoint.stop()
    endpoint.start()
    cur = endpoint.connect().cursor()
    value = random_string(10_000_000)
    cur.execute(f"UPDATE reptbl SET largeval='{value}'")
    logical_replication_sync(vanilla_pg, endpoint)
    assert vanilla_pg.safe_psql("select id, largeval from reptbl") == [(2, value)]


#
# Check that slots are not inherited in brnach
#
def test_slots_and_branching(neon_simple_env: NeonEnv):
    env = neon_simple_env

    tenant, timeline = env.neon_cli.create_tenant()
    env.pageserver.http_client()

    main_branch = env.endpoints.create_start("main", tenant_id=tenant)
    main_cur = main_branch.connect().cursor()

    # Create table and insert some data
    main_cur.execute("select pg_create_logical_replication_slot('my_slot', 'pgoutput')")

    wait_for_last_flush_lsn(env, main_branch, tenant, timeline)

    # Create branch ws.
    env.neon_cli.create_branch("ws", "main", tenant_id=tenant)
    ws_branch = env.endpoints.create_start("ws", tenant_id=tenant)

    # Check that we can create slot with the same name
    ws_cur = ws_branch.connect().cursor()
    ws_cur.execute("select pg_create_logical_replication_slot('my_slot', 'pgoutput')")
