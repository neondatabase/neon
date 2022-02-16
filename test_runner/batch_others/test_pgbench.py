from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log


def test_pgbench(zenith_simple_env: ZenithEnv, pg_bin):
    env = zenith_simple_env
    # Create a branch for us
    env.zenith_cli(["branch", "test_pgbench", "empty"])

    pg = env.postgres.create_start('test_pgbench')
    log.info("postgres is running on 'test_pgbench' branch")

    connstr = pg.connstr()

    pg_bin.run_capture(['pgbench', '-i', connstr])
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 5 -P 1 -M prepared'.split() + [connstr])
