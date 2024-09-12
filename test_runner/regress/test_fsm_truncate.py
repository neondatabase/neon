from fixtures.neon_fixtures import Endpoint


def test_fsm_truncate(neon_endpoint: Endpoint):
    endpoint = neon_endpoint
    endpoint.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
