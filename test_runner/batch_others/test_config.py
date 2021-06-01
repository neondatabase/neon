import psycopg2

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test starting Postgres with custom options
#
def test_config(zenith_cli, pageserver, postgres, pg_bin):
    # Create a branch for us
    zenith_cli.run(["branch", "test_config", "empty"])

    # change config
    pg = postgres.create_start('test_config', ['log_min_messages=debug1'])
    print('postgres is running on test_config branch')

    with psycopg2.connect(pg.connstr()) as conn:
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

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
            assert cur.fetchone() == ('debug1',)
