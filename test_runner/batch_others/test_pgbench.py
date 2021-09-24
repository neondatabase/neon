from fixtures.zenith_fixtures import PostgresFactory

pytest_plugins = ("fixtures.zenith_fixtures")


def test_pgbench(postgres: PostgresFactory, pg_bin, zenith_cli):
    # Create a branch for us
    zenith_cli.run(["branch", "test_pgbench", "empty"])

    pg = postgres.create_start('test_pgbench')
    print("postgres is running on 'test_pgbench' branch")

    connstr = pg.connstr()

    pg_bin.run_capture(['pgbench', '-i', '-s', '100', connstr])
    pg_bin.run_capture(['pgbench'] + '-c 1 -N -T 100 -P 1 -M prepared'.split() + [connstr])
