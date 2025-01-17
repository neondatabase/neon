from __future__ import annotations

from fixtures.neon_fixtures import NeonEnvBuilder


def test_fsm_truncate(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init_start()
    env.create_branch("test_fsm_truncate")
    endpoint = env.endpoints.create_start("test_fsm_truncate")
    endpoint.safe_psql(
        "CREATE TABLE t1(key int); CREATE TABLE t2(key int); TRUNCATE TABLE t1; TRUNCATE TABLE t2;"
    )
