import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder, PgBin, PortDistributor
from fixtures.neon_fixtures import pg_distrib_dir
import os
from fixtures.utils import mkdir_if_needed
import shutil
import getpass
import pwd

num_rows = 1000


# Ensure that regular postgres can start from fullbackup
def test_fullbackup(neon_env_builder: NeonEnvBuilder,
                    pg_bin: PgBin,
                    port_distributor: PortDistributor):

    neon_env_builder.num_safekeepers = 1
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch('test_fullbackup')
    pgmain = env.postgres.create_start('test_fullbackup')
    log.info("postgres is running on 'test_fullbackup' branch")

    timeline = pgmain.safe_psql("SHOW neon.timeline_id")[0][0]

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

    psql_path = os.path.join(pg_bin.pg_bin_path, 'psql')
    restored_dir_path = os.path.join(env.repo_dir, "restored_datadir")
    restored_conf_path = os.path.join(restored_dir_path, "postgresql.conf")
    os.mkdir(restored_dir_path, 0o750)

    cmd = rf"""
        {psql_path}                                    \
            --no-psqlrc                                \
            postgres://localhost:{env.pageserver.service_port.pg}  \
            -c 'fullbackup {env.initial_tenant.hex} {timeline} {lsn}'  \
         | tar -x -C {restored_dir_path}
    """

    log.info(f"Running command: {cmd}")

    # Set LD_LIBRARY_PATH in the env properly, otherwise we may use the wrong libpq.
    # PgBin sets it automatically, but here we need to pipe psql output to the tar command.
    psql_env = {'LD_LIBRARY_PATH': os.path.join(str(pg_distrib_dir), 'lib')}
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    # Print captured stdout/stderr if fullbackup cmd failed.
    if result.returncode != 0:
        log.error('fullbackup shell command failed with:')
        log.error(result.stdout)
        log.error(result.stderr)
    assert result.returncode == 0

    port = port_distributor.get_port()
    with open(restored_conf_path, 'w') as f:
        f.write(f"port={port}")

    # HACK
    # fullbackup returns neon specific pg_control and first WAL segment
    # use resetwal to overwrite it
    pg_resetwal_path = os.path.join(pg_bin.pg_bin_path, 'pg_resetwal')
    cmd = rf"""{pg_resetwal_path} -D {restored_dir_path}"""
    log.info(f"Running command: {cmd}")
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    # now start the postgres and ensure that we can read the data
    pg_ctl_path = os.path.join(pg_bin.pg_bin_path, 'pg_ctl')
    restored_dir_logfile = os.path.join(env.repo_dir, "logfile_restored_datadir")
    cmd = rf"""{pg_ctl_path} start -D {restored_dir_path} -o '-p {port}' -l {restored_dir_logfile}"""
    log.info(f"Running command: {cmd}")
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    # Print captured stdout/stderr if pg_ctl cmd failed.
    if result.returncode != 0:
        log.error('pg_ctl shell command failed with:')
        log.error(result.stdout)
        log.error(result.stderr)
    assert result.returncode == 0

    with psycopg2.connect(dbname="postgres", user="cloud_admin", host='localhost',
                          port=f"{port}") as conn:
        with conn.cursor() as cur:
            cur.execute('select count(*) from tbl;')
            assert cur.fetchone()[0] == num_rows

    # stop the postgres to not mess up with other tests
    cmd = rf"""{pg_ctl_path} stop -D {restored_dir_path} -m immediate -w"""
    log.info(f"Running command: {cmd}")
    result = subprocess.run(cmd, env=psql_env, capture_output=True, text=True, shell=True)

    assert result.returncode == 0
