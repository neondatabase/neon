import time
import os

from contextlib import closing

from fixtures.neon_fixtures import NeonEnv
from fixtures.log_helper import log


#
# Test compute node start after clog truncation
#
def test_clog_truncate(neon_simple_env: NeonEnv):
    env = neon_simple_env
    env.neon_cli.create_branch('test_clog_truncate', 'empty')

    # set aggressive autovacuum to make sure that truncation will happen
    config = [
        'autovacuum_max_workers=10',
        'autovacuum_vacuum_threshold=0',
        'autovacuum_vacuum_insert_threshold=0',
        'autovacuum_vacuum_cost_delay=0',
        'autovacuum_vacuum_cost_limit=10000',
        'autovacuum_naptime =1s',
        'autovacuum_freeze_max_age=100000'
    ]

    pg = env.postgres.create_start('test_clog_truncate', config_lines=config)
    log.info('postgres is running on test_clog_truncate branch')

    # Install extension containing function needed for test
    pg.safe_psql('CREATE EXTENSION neon_test_utils')

    # Consume many xids to advance clog
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('select test_consume_xids(1000*1000*10);')
            log.info('xids consumed')

            # call a checkpoint to trigger TruncateSubtrans
            cur.execute('CHECKPOINT;')

            # ensure WAL flush
            cur.execute('select txid_current()')
            log.info(cur.fetchone())

    # wait for autovacuum to truncate the pg_xact
    # XXX Is it worth to add a timeout here?
    pg_xact_0000_path = os.path.join(pg.pg_xact_dir_path(), '0000')
    log.info(f"pg_xact_0000_path = {pg_xact_0000_path}")

    while os.path.isfile(pg_xact_0000_path):
        log.info(f"file exists. wait for truncation. " "pg_xact_0000_path = {pg_xact_0000_path}")
        time.sleep(5)

    # checkpoint to advance latest lsn
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('CHECKPOINT;')
            cur.execute('select pg_current_wal_insert_lsn()')
            lsn_after_truncation = cur.fetchone()[0]

    # create new branch after clog truncation and start a compute node on it
    log.info(f'create branch at lsn_after_truncation {lsn_after_truncation}')
    env.neon_cli.create_branch('test_clog_truncate_new',
                                 'test_clog_truncate',
                                 ancestor_start_lsn=lsn_after_truncation)
    pg2 = env.postgres.create_start('test_clog_truncate_new')
    log.info('postgres is running on test_clog_truncate_new branch')

    # check that new node doesn't contain truncated segment
    pg_xact_0000_path_new = os.path.join(pg2.pg_xact_dir_path(), '0000')
    log.info(f"pg_xact_0000_path_new = {pg_xact_0000_path_new}")
    assert os.path.isfile(pg_xact_0000_path_new) is False
