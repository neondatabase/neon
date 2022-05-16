import subprocess
from contextlib import closing

import psycopg2.extras
import pytest
from fixtures.log_helper import log
from fixtures.utils import print_gc_result
from fixtures.zenith_fixtures import ZenithEnvBuilder


#
# Check pitr_interval GC behavior.
# Insert some data, run GC and create a branch in the past.
#
def test_pitr_gc(zenith_env_builder: ZenithEnvBuilder):

    zenith_env_builder.num_safekeepers = 1
    # Set pitr interval such that we need to keep the data
    zenith_env_builder.pageserver_config_override = "tenant_config={pitr_interval = '1 day', gc_horizon = 0}"

    env = zenith_env_builder.init_start()
    pgmain = env.postgres.create_start('main')
    log.info("postgres is running on 'main' branch")

    main_pg_conn = pgmain.connect()
    main_cur = main_pg_conn.cursor()

    main_cur.execute("SHOW zenith.zenith_timeline")
    timeline = main_cur.fetchone()[0]

    # Create table
    main_cur.execute('CREATE TABLE foo (t text)')

    for i in range(10000):
        main_cur.execute('''
            INSERT INTO foo
                SELECT 'long string to consume some space';
        ''')

        if i == 99:
            # keep some early lsn to test branch creation after GC
            main_cur.execute('SELECT pg_current_wal_insert_lsn(), txid_current()')
            res = main_cur.fetchone()
            lsn_a = res[0]
            xid_a = res[1]
            log.info(f'LSN after 100 rows: {lsn_a} xid {xid_a}')

    main_cur.execute('SELECT pg_current_wal_insert_lsn(), txid_current()')
    res = main_cur.fetchone()
    debug_lsn = res[0]
    debug_xid = res[1]
    log.info(f'LSN after 10000 rows: {debug_lsn} xid {debug_xid}')

    # run GC
    with closing(env.pageserver.connect()) as psconn:
        with psconn.cursor(cursor_factory=psycopg2.extras.DictCursor) as pscur:
            pscur.execute(f"compact {env.initial_tenant.hex} {timeline}")
            # perform agressive GC. Data still should be kept because of the PITR setting.
            pscur.execute(f"do_gc {env.initial_tenant.hex} {timeline} 0")
            row = pscur.fetchone()
            print_gc_result(row)

    # Branch at the point where only 100 rows were inserted
    # It must have been preserved by PITR setting
    env.zenith_cli.create_branch('test_pitr_gc_hundred', 'main', ancestor_start_lsn=lsn_a)

    pg_hundred = env.postgres.create_start('test_pitr_gc_hundred')

    # On the 'hundred' branch, we should see only 100 rows
    hundred_pg_conn = pg_hundred.connect()
    hundred_cur = hundred_pg_conn.cursor()
    hundred_cur.execute('SELECT count(*) FROM foo')
    assert hundred_cur.fetchone() == (100, )

    # All the rows are visible on the main branch
    main_cur.execute('SELECT count(*) FROM foo')
    assert main_cur.fetchone() == (10000, )
