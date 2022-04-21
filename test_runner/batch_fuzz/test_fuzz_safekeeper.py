from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnv, PgBin, ZenithEnvBuilder, DEFAULT_BRANCH_NAME, AdversarialProposerBin
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker




# TODO PR execution plan:
# 1. Write an adversarial proposer that successfully sends greeting
#   1. Make pg connection
#   2. Send a FeMessage::CopyData that contains ('g' as char as u8 as u64) in LittleEndian
# 2. Send Elected, and see what happens
# 3. Add TODOs, merge the harness into main, improve later


def test_fuzz_safekeeper(zenith_env_builder: ZenithEnvBuilder,
                         adversarial_proposer_bin: AdversarialProposerBin):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_fuzz_safekeeper')
    pg = env.postgres.create_start('test_fuzz_safekeeper')
    timeline = pg.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    output = adversarial_proposer_bin.say_hi(env.initial_tenant.hex, timeline)
    print(output)

    # TODO:
    # 1. Start an adversarial proposer (new rust binary) that tries to take over
    # 2. Kill pg node, let the proposer take over
    # 3. Start multiple adversarial proposers
    # 4. Run for a while, checking safekeeper invariants

# TODO:
# 1. Explore fuzzing techniques, look into https://docs.rs/stateright/latest/stateright/
# 2. Simulate network delay between nodes, both random and deliberate
