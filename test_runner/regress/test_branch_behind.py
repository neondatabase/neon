import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.types import Lsn, TimelineId
from fixtures.utils import print_gc_result, query_scalar


#
# Create a couple of branches off the main branch, at a historical point in time.
#
def test_branch_behind(neon_env_builder: NeonEnvBuilder):
    # Disable pitr, because here we want to test branch creation after GC
    neon_env_builder.pageserver_config_override = "tenant_config={pitr_interval = '0 sec'}"
    env = neon_env_builder.init_start()

    env.pageserver.allowed_errors.append(".*invalid branch start lsn.*")
    env.pageserver.allowed_errors.append(".*invalid start lsn .* for ancestor timeline.*")

    # Branch at the point where only 100 rows were inserted
    env.neon_cli.create_branch("test_branch_behind")
    pgmain = env.postgres.create_start("test_branch_behind")
    log.info("postgres is running on 'test_branch_behind' branch")

    main_cur = pgmain.connect().cursor()

    timeline = TimelineId(query_scalar(main_cur, "SHOW neon.timeline_id"))

    # Create table, and insert the first 100 rows
    main_cur.execute("CREATE TABLE foo (t text)")

    # keep some early lsn to test branch creation on out of date lsn
    gced_lsn = Lsn(query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()"))

    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100) g
    """
    )
    lsn_a = Lsn(query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()"))
    log.info(f"LSN after 100 rows: {lsn_a}")

    # Insert some more rows. (This generates enough WAL to fill a few segments.)
    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    """
    )
    lsn_b = Lsn(query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()"))
    log.info(f"LSN after 200100 rows: {lsn_b}")

    # Branch at the point where only 100 rows were inserted
    env.neon_cli.create_branch(
        "test_branch_behind_hundred", "test_branch_behind", ancestor_start_lsn=lsn_a
    )

    # Insert many more rows. This generates enough WAL to fill a few segments.
    main_cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 200000) g
    """
    )
    lsn_c = Lsn(query_scalar(main_cur, "SELECT pg_current_wal_insert_lsn()"))

    log.info(f"LSN after 400100 rows: {lsn_c}")

    # Branch at the point where only 200100 rows were inserted
    env.neon_cli.create_branch(
        "test_branch_behind_more", "test_branch_behind", ancestor_start_lsn=lsn_b
    )

    pg_hundred = env.postgres.create_start("test_branch_behind_hundred")
    pg_more = env.postgres.create_start("test_branch_behind_more")

    # On the 'hundred' branch, we should see only 100 rows
    hundred_cur = pg_hundred.connect().cursor()
    assert query_scalar(hundred_cur, "SELECT count(*) FROM foo") == 100

    # On the 'more' branch, we should see 100200 rows
    more_cur = pg_more.connect().cursor()
    assert query_scalar(more_cur, "SELECT count(*) FROM foo") == 200100

    # All the rows are visible on the main branch
    assert query_scalar(main_cur, "SELECT count(*) FROM foo") == 400100

    # Check bad lsn's for branching

    # branch at segment boundary
    env.neon_cli.create_branch(
        "test_branch_segment_boundary", "test_branch_behind", ancestor_start_lsn=Lsn("0/3000000")
    )
    pg = env.postgres.create_start("test_branch_segment_boundary")
    assert pg.safe_psql("SELECT 1")[0][0] == 1

    # branch at pre-initdb lsn
    with pytest.raises(Exception, match="invalid branch start lsn: .*"):
        env.neon_cli.create_branch("test_branch_preinitdb", ancestor_start_lsn=Lsn("0/42"))

    # branch at pre-ancestor lsn
    with pytest.raises(Exception, match="less than timeline ancestor lsn"):
        env.neon_cli.create_branch(
            "test_branch_preinitdb", "test_branch_behind", ancestor_start_lsn=Lsn("0/42")
        )

    # check that we cannot create branch based on garbage collected data
    with env.pageserver.http_client() as pageserver_http:
        gc_result = pageserver_http.timeline_gc(env.initial_tenant, timeline, 0)
        print_gc_result(gc_result)

    with pytest.raises(Exception, match="invalid branch start lsn: .*"):
        # this gced_lsn is pretty random, so if gc is disabled this woudln't fail
        env.neon_cli.create_branch(
            "test_branch_create_fail", "test_branch_behind", ancestor_start_lsn=gced_lsn
        )

    # check that after gc everything is still there
    assert query_scalar(hundred_cur, "SELECT count(*) FROM foo") == 100

    assert query_scalar(more_cur, "SELECT count(*) FROM foo") == 200100

    assert query_scalar(main_cur, "SELECT count(*) FROM foo") == 400100
