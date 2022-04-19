










def test_fuzz_safekeeper(zenith_env_builder: ZenithEnvBuilder):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_fuzz_safekeeper')
    pg = env.postgres.create_start('test_fuzz_safekeeper')

    # TODO:
    # 1. Start an adversarial proposer (new rust binary) that tries to take over
    # 2. Kill pg node, let the proposer take over
    # 3. Start multiple adversarial proposers
    # 4. Run for a while, checking safekeeper invariants

# TODO:
# 1. Explore fuzzing techniques, look into https://docs.rs/stateright/latest/stateright/
# 2. Simulate network delay between nodes, both random and deliberate
