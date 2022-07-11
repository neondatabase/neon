import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.zenith_fixtures import ZenithEnvBuilder, PgBin, PortDistributor, VanillaPostgres
from fixtures.zenith_fixtures import pg_distrib_dir
import os
from fixtures.utils import mkdir_if_needed, subprocess_capture
import shutil
import getpass
import pwd

num_rows = 1000


# Ensure that regular postgres can start from fullbackup
def test_fullbackup(zenith_env_builder: ZenithEnvBuilder,
                    pg_bin: PgBin,
                    port_distributor: PortDistributor):

    zenith_env_builder.num_safekeepers = 1
    env = zenith_env_builder.init_start()

    env.zenith_cli.create_branch('test_fullbackup')
    pgmain = env.postgres.create_start('test_fullbackup')
    log.info("postgres is running on 'test_fullbackup' branch")

    timeline = pgmain.safe_psql("SHOW zenith.zenith_timeline")[0][0]

    with closing(pgmain.connect()) as conn:
        with conn.cursor() as cur:
            # data loading may take a while, so increase statement timeout
            cur.execute("SET statement_timeout='300s'")
            cur.execute(f'''CREATE TABLE tbl AS SELECT 'long string to consume some space' || g
                        from generate_series(1,{num_rows}) g''')
            cur.execute("CHECKPOINT")

            cur.execute('SELECT pg_current_wal_insert_lsn()')
            lsn = cur.fetchone()[0]
            log.info(f"start_backup_lsn = {lsn}")

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {'LD_LIBRARY_PATH': os.path.join(str(pg_distrib_dir), 'lib')}

    # Get and unpack fullbackup from pageserver
    restored_dir_path = os.path.join(env.repo_dir, "restored_datadir")
    os.mkdir(restored_dir_path, 0o750)
    query = f"fullbackup {env.initial_tenant.hex} {timeline} {lsn}"
    cmd = ["psql", "--no-psqlrc", env.pageserver.connstr(), "-c", query]
    result_basepath = pg_bin.run_capture(cmd, env=psql_env)
    tar_output_file = result_basepath + ".stdout"
    subprocess_capture(str(env.repo_dir), ["tar", "-xf", tar_output_file, "-C", restored_dir_path])

    # HACK
    # fullbackup returns zenith specific pg_control and first WAL segment
    # use resetwal to overwrite it
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, 'pg_resetwal')
    cmd = [pg_resetwal_path, "-D", restored_dir_path]
    pg_bin.run_capture(cmd, env=psql_env)

    # Restore from the backup and find the data we inserted
    port = port_distributor.get_port()
    with VanillaPostgres(restored_dir_path, pg_bin, port, init=False) as vanilla_pg:
        # TODO make port an optional argument
        vanilla_pg.configure([
            f"port={port}",
        ])
        vanilla_pg.start()
        num_rows_found = vanilla_pg.safe_psql('select count(*) from tbl;',
                                              username="zenith_admin")[0][0]
        assert num_rows == num_rows_found
