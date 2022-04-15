from contextlib import closing

from fixtures.zenith_fixtures import ZenithEnvBuilder
from fixtures.benchmark_fixture import ZenithBenchmarker


def test_startup(zenith_env_builder: ZenithEnvBuilder, zenbenchmark: ZenithBenchmarker):
    zenith_env_builder.num_safekeepers = 3
    env = zenith_env_builder.init_start()

    # Start
    env.zenith_cli.create_branch('test_startup')
    with zenbenchmark.record_duration("startup_time"):
        pg = env.postgres.create_start('test_startup')
        pg.safe_psql("select 1;")

    # Restart
    pg.stop_and_destroy()
    with zenbenchmark.record_duration("restart_time"):
        pg.create_start('test_startup')
        pg.safe_psql("select 1;")
