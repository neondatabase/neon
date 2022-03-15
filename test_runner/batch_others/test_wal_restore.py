import os
import subprocess

from fixtures.utils import mkdir_if_needed
from fixtures.zenith_fixtures import (ZenithEnvBuilder,
                                      base_dir,
                                      vanilla_pg,
                                      pg_distrib_dir)
from fixtures.log_helper import log


def test_wal_restore(zenith_env_builder: ZenithEnvBuilder, test_output_dir, vanilla_pg):
    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()
    env.zenith_cli.create_branch("test_wal_restore")
    pg = env.postgres.create_start('test_wal_restore')
    pg.safe_psql("create table t as select generate_series(1,1000000)")
    tenant_id = pg.safe_psql("show zenith.zenith_tenant")[0][0]
    env.zenith_cli.pageserver_stop()
    subprocess.call(['bash',
                     os.path.join(base_dir, 'vendor/postgres/contrib/zenith/utils/restore_from_wal.sh'),
                     os.path.join(pg_distrib_dir, 'bin'),
                     os.path.join(test_output_dir, 'repo/safekeepers/sk1/{}/*'.format(tenant_id)),
                     test_output_dir])
    vanilla_pg.start()
    assert vanilla_pg.safe_psql('select count(*) from t') == [(1000000, )]
    vanilla_pg.stop()
