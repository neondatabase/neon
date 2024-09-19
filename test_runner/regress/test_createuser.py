from fixtures.neon_fixtures import NeonEnv
from fixtures.shared_fixtures import TTimeline
from fixtures.utils import query_scalar


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(timeline: TTimeline):
    endpoint = timeline.primary

    with endpoint.cursor() as cur:
        # Cause a 'relmapper' change in the original branch
        cur.execute("CREATE USER testuser with password %s", ("testpwd",))

        cur.execute("CHECKPOINT")

        lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create a branch
    branch = timeline.create_branch("test_createuser2", lsn=lsn)
    endpoint2 = branch.primary

    # Test that you can connect to new branch as a new user
    assert endpoint2.safe_psql("select current_user", user="testuser") == [("testuser",)]
