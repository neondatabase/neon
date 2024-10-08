from fixtures.neon_tenant import NeonTestTenant


def test_fsm_truncate(neon_tenant: NeonTestTenant):
    neon_tenant.create_branch("test_fsm_truncate")
    endpoint = neon_tenant.endpoints.create_start("test_fsm_truncate")
    endpoint.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
