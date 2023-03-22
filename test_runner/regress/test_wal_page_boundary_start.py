import time
from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn, wait_for_sk_commit_lsn_to_arrive_at_pageserver_last_record_lsn
from fixtures.utils import query_scalar
from fixtures.types import Lsn
from fixtures.log_helper import log

# Test compute start from WAL page boundary.
def test_wal_page_boundary_start(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.neon_cli.create_branch("init")
    pg = env.postgres.create_start("init")
    tenant_id = pg.safe_psql("show neon.tenant_id")[0][0]
    timeline_id = pg.safe_psql("show neon.timeline_id")[0][0]
    with pg.cursor() as cur:
        cur.execute("create table t(key int, value text)")
        # measure how much space logical message takes
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
        log.info(f"before {lsn_before}, after {lsn_after_by_curr_wal_lsn}, lsn diff is {lsn_diff}, base {logical_message_base}")

        # and write logical message spanning exactly till the page boundary
        lsn_before = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"current_lsn={lsn_before}")
        curr_lsn = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        offs = int(curr_lsn) % 8192
        till_page = 8192 - offs
        payload_len = till_page - logical_message_base - 8 # not sure why 8 is here, it is deduced from experiments
        log.info(f"current_lsn={curr_lsn}, offs {offs}, till_page {till_page}")

        cur.execute(f"select pg_logical_emit_message(true, 'pref', 'f{'a' * payload_len}')")
        supposedly_page_boundary = Lsn(query_scalar(cur, "select pg_current_wal_lsn()"))
        log.info(f"supposedly_page_boundary={supposedly_page_boundary}")
        assert int(supposedly_page_boundary) % 8192 == 0, "missed page boundary, bad luck"

    pg.stop()

    # wait for WAL arrival at sk is ensured by synchronous commit.
    wait_for_sk_commit_lsn_to_arrive_at_pageserver_last_record_lsn(tenant_id, timeline_id, env.safekeepers, env.pageserver)
    pg_new = env.postgres.create_start("init")
    with pg_new.cursor() as cur:
        cur.execute("select 42")
        # We have had a bug with sending zero page header before first WAL
        # record is written; give time for it to appear.
        time.sleep(1)
        # ensure that writing works
        cur.execute("insert into t values (1, 'pear')")

