import pytest

pytest_plugins = ("fixtures.zenith_fixtures")


def test_pgbench(pageserver, postgres, pg_bin, zenith_cli):

    # Create a branch for us
    zenith_cli.run(["branch", "test_pgbench", "empty"]);

    pg = postgres.create_start('test_pgbench')
    print("postgres is running on 'test_pgbench' branch")

    connstr = pg.connstr();

    pg_bin.run_capture(['pgbench', '-i', connstr])
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 5 -P 1 -M prepared'.split() + [connstr])
