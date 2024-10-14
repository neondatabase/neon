from __future__ import annotations

from fixtures.neon_fixtures import NeonEnv
from fixtures.utils import query_scalar


#
# Test CREATE USER to check shared catalog restore
#
def test_createuser(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main")

    with endpoint.cursor() as cur:
        # Cause a 'relmapper' change in the original branch
        cur.execute("CREATE USER testuser with password %s", ("testpwd",))

        cur.execute("CHECKPOINT")

        lsn = query_scalar(cur, "SELECT pg_current_wal_insert_lsn()")

    # Create a branch
    env.create_branch("test_createuser2", ancestor_branch_name="main", ancestor_start_lsn=lsn)
    endpoint2 = env.endpoints.create_start("test_createuser2")

    # Test that you can connect to new branch as a new user
    assert endpoint2.safe_psql("select current_user", user="testuser") == [("testuser",)]
