from fixtures.neon_fixtures import NeonEnvBuilder


# Test that neon cli is able to start and stop all processes with the user defaults.
# def test_neon_cli_basics(neon_simple_env: NeonEnv):
def test_neon_cli_basics(neon_env_builder: NeonEnvBuilder):
    env = neon_env_builder.init()

    env.neon_cli.start()
    env.neon_cli.stop()
