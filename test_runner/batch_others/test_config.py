from contextlib import closing

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test starting Postgres with custom options
#
def test_config(zenith_cli, pageserver, postgres, pg_bin):
    # Create a branch for us
    zenith_cli.run(["branch", "test_config", "empty"])

    # change config
    pg = postgres.create_start('test_config', config_lines=['log_min_messages=debug1'])
    print('postgres is running on test_config branch')

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
