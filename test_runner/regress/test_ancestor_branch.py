from __future__ import annotations

from fixtures.common_types import TimelineId
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import query_scalar


#
# Create ancestor branches off the main branch.
#
def test_ancestor_branch(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    pageserver_http = env.pageserver.http_client()

    # Override defaults: 4M checkpoint_distance, disable background compaction and gc.
    tenant, _ = env.create_tenant(
        conf={
            "checkpoint_distance": "4194304",
            "gc_period": "0s",
            "compaction_period": "0s",
        }
    )

    failpoint = "flush-frozen-pausable"

    pageserver_http.configure_failpoints((failpoint, "sleep(10000)"))

    endpoint_branch0 = env.endpoints.create_start("main", tenant_id=tenant)
    branch0_cur = endpoint_branch0.connect().cursor()
    branch0_timeline = TimelineId(query_scalar(branch0_cur, "SHOW neon.timeline_id"))
    log.info(f"b0 timeline {branch0_timeline}")

    # Create table, and insert 100k rows.
    branch0_lsn = query_scalar(branch0_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"b0 at lsn {branch0_lsn}")

    branch0_cur.execute("CREATE TABLE foo (t text) WITH (autovacuum_enabled = off)")
    branch0_cur.execute(
        """
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch0:' || g
            FROM generate_series(1, 100000) g
    """
    )
    lsn_100 = query_scalar(branch0_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"LSN after 100k rows: {lsn_100}")

    # Create branch1.
    env.create_branch(
        "branch1", ancestor_branch_name="main", ancestor_start_lsn=lsn_100, tenant_id=tenant
    )
    endpoint_branch1 = env.endpoints.create_start("branch1", tenant_id=tenant)

    branch1_cur = endpoint_branch1.connect().cursor()
    branch1_timeline = TimelineId(query_scalar(branch1_cur, "SHOW neon.timeline_id"))
    log.info(f"b1 timeline {branch1_timeline}")

    branch1_lsn = query_scalar(branch1_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"b1 at lsn {branch1_lsn}")

    # Insert 100k rows.
    branch1_cur.execute(
        """
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch1:' || g
            FROM generate_series(1, 100000) g
    """
    )
    lsn_200 = query_scalar(branch1_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"LSN after 200k rows: {lsn_200}")

    # Create branch2.
    env.create_branch(
        "branch2", ancestor_branch_name="branch1", ancestor_start_lsn=lsn_200, tenant_id=tenant
    )
    endpoint_branch2 = env.endpoints.create_start("branch2", tenant_id=tenant)
    branch2_cur = endpoint_branch2.connect().cursor()

    branch2_timeline = TimelineId(query_scalar(branch2_cur, "SHOW neon.timeline_id"))
    log.info(f"b2 timeline {branch2_timeline}")

    branch2_lsn = query_scalar(branch2_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"b2 at lsn {branch2_lsn}")

    # Insert 100k rows.
    branch2_cur.execute(
        """
        INSERT INTO foo
            SELECT '00112233445566778899AABBCCDDEEFF' || ':branch2:' || g
            FROM generate_series(1, 100000) g
    """
    )
    lsn_300 = query_scalar(branch2_cur, "SELECT pg_current_wal_insert_lsn()")
    log.info(f"LSN after 300k rows: {lsn_300}")

    # Run compaction on branch1.
    compact = f"compact {tenant} {branch1_timeline}"
    log.info(compact)
    pageserver_http.timeline_compact(tenant, branch1_timeline)

    assert query_scalar(branch0_cur, "SELECT count(*) FROM foo") == 100000

    assert query_scalar(branch1_cur, "SELECT count(*) FROM foo") == 200000

    assert query_scalar(branch2_cur, "SELECT count(*) FROM foo") == 300000

    pageserver_http.configure_failpoints((failpoint, "off"))
