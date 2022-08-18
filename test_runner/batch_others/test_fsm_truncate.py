from fixtures.neon_fixtures import NeonEnvBuilder


def test_fsm_truncate(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.neon_cli.create_branch("test_fsm_truncate")
    pg = env.postgres.create_start("test_fsm_truncate")
    pg.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
