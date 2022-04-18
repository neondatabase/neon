import os
import subprocess

from fixtures.zenith_fixtures import (ZenithEnvBuilder,
                                      VanillaPostgres,
                                      PortDistributor,
                                      PgBin,
                                      base_dir,
                                      vanilla_pg,
                                      pg_distrib_dir)
from fixtures.log_helper import log


def test_wal_restore(zenith_env_builder: ZenithEnvBuilder,
                     pg_bin: PgBin,
                     test_output_dir,
                     port_distributor: PortDistributor):
    env = zenith_env_builder.init_start()
    env.zenith_cli.create_branch("test_wal_restore")
    pg = env.postgres.create_start('test_wal_restore')
    pg.safe_psql("create table t as select generate_series(1,300000)")
    tenant_id = pg.safe_psql("show neon.tenantid")[0][0]
    env.zenith_cli.pageserver_stop()
    port = port_distributor.get_port()
    data_dir = os.path.join(test_output_dir, 'pgsql.restored')
    with VanillaPostgres(data_dir, PgBin(test_output_dir), port) as restored:
        pg_bin.run_capture([
            os.path.join(base_dir, 'libs/utils/scripts/restore_from_wal.sh'),
            os.path.join(pg_distrib_dir, 'bin'),
            os.path.join(test_output_dir, 'repo/safekeepers/sk1/{}/*'.format(tenant_id)),
            data_dir,
            str(port)
        ])
        restored.start()
        assert restored.safe_psql('select count(*) from t', user='zenith_admin') == [(300000, )]
