from __future__ import annotations

from fixtures.common_types import TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import print_gc_result, query_scalar


#
# Check pitr_interval GC behavior.
# Insert some data, run GC and create a branch in the past.
#
def test_pitr_gc(neon_env_builder: NeonEnvBuilder):
    # Set pitr interval such that we need to keep the data
    env = neon_env_builder.init_start(
        initial_tenant_conf={"pitr_interval": "1 day", "gc_horizon": "0"}
    )
    endpoint_main = env.endpoints.create_start("main")

    main_pg_conn = endpoint_main.connect()
    main_cur = main_pg_conn.cursor()
    timeline = TimelineId(query_scalar(main_cur, "SHOW neon.timeline_id"))

    # Create table
    main_cur.execute("CREATE TABLE foo (t text)")

    for i in range(10000):
        main_cur.execute(
            """
            INSERT INTO foo
                SELECT 'long string to consume some space';
        """
        )

        if i == 99:
            # keep some early lsn to test branch creation after GC
            main_cur.execute("SELECT pg_current_wal_insert_lsn(), txid_current()")
            res = main_cur.fetchone()
            assert res is not None
            lsn_a = res[0]
            xid_a = res[1]
            log.info(f"LSN after 100 rows: {lsn_a} xid {xid_a}")

    main_cur.execute("SELECT pg_current_wal_insert_lsn(), txid_current()")
    res = main_cur.fetchone()
    assert res is not None

    debug_lsn = res[0]
    debug_xid = res[1]
    log.info(f"LSN after 10000 rows: {debug_lsn} xid {debug_xid}")

    # run GC
    with env.pageserver.http_client() as pageserver_http:
        pageserver_http.timeline_checkpoint(env.initial_tenant, timeline)
        pageserver_http.timeline_compact(env.initial_tenant, timeline)
        # perform aggressive GC. Data still should be kept because of the PITR setting.
        gc_result = pageserver_http.timeline_gc(env.initial_tenant, timeline, 0)
        print_gc_result(gc_result)

    # Branch at the point where only 100 rows were inserted
    # It must have been preserved by PITR setting
    env.create_branch("test_pitr_gc_hundred", ancestor_branch_name="main", ancestor_start_lsn=lsn_a)

    endpoint_hundred = env.endpoints.create_start("test_pitr_gc_hundred")

    # On the 'hundred' branch, we should see only 100 rows
    hundred_pg_conn = endpoint_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute("SELECT count(*) FROM foo")
    assert hundred_cur.fetchone() == (100,)

    # All the rows are visible on the main branch
    main_cur.execute("SELECT count(*) FROM foo")
    assert main_cur.fetchone() == (10000,)
