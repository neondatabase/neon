from fixtures.neon_fixtures import NeonEnv


def test_fsm_truncate(neon_shared_env: NeonEnv):
    env = neon_shared_env
    env.neon_cli.create_branch("test_fsm_truncate")
    endpoint = env.endpoints.create_start("test_fsm_truncate")
    endpoint.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
