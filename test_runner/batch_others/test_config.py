from contextlib import closing

from fixtures.neon_fixtures import NeonEnv
from fixtures.log_helper import log


#
# Test starting Postgres with custom options
#
def test_config(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch("test_config", "empty")

    # change config
    pg = env.postgres.create_start('test_config', config_lines=['log_min_messages=debug1'])
    log.info('postgres is running on test_config branch')

    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('''
                SELECT setting
                FROM pg_settings
                WHERE
                    source != 'default'
                    AND source != 'override'
                    AND name = 'log_min_messages'
            ''')

            # check that config change was applied
            assert cur.fetchone() == ('debug1', )
