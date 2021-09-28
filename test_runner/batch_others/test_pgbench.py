from fixtures.zenith_fixtures import PostgresFactory

import logging
import fixtures.log_helper  # configures loggers
log = logging.getLogger('root')

pytest_plugins = ("fixtures.zenith_fixtures")


def test_pgbench(postgres: PostgresFactory, pg_bin, zenith_cli):
    # Create a branch for us
    zenith_cli.run(["branch", "test_pgbench", "empty"])

    pg = postgres.create_start('test_pgbench')
    log.info("postgres is running on 'test_pgbench' branch")

    connstr = pg.connstr()

    pg_bin.run_capture(['pgbench', '-i', connstr])
    pg_bin.run_capture(['pgbench'] + '-c 10 -T 5 -P 1 -M prepared'.split() + [connstr])
