from fixtures.neon_fixtures import NeonEnv


def test_protocol_version(neon_simple_env: NeonEnv):
    env = neon_simple_env
    endpoint = env.endpoints.create_start("main", config_lines=["neon.protocol_version=1"])
    cur = endpoint.connect().cursor()
    cur.execute("show neon.protocol_version")
    assert cur.fetchone() == ("1",)
