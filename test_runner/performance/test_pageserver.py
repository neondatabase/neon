from contextlib import closing

import pytest

from fixtures.zenith_fixtures import ZenithEnv, PgBin, ZenithEnvBuilder, DEFAULT_BRANCH_NAME, ReplayBin
from fixtures.benchmark_fixture import MetricReport, ZenithBenchmarker


def test_replay(zenith_env_builder: ZenithEnvBuilder,
                  zenbenchmark: ZenithBenchmarker,
                  replay_bin: ReplayBin):
    env = zenith_env_builder.init_start()

    tenant = env.zenith_cli.create_tenant()
    timeline = env.zenith_cli.create_timeline("test_replay", tenant)
    replay_bin.run(tenant, timeline)
