import time
import os

from contextlib import closing

from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test compute node start after xid wraparound
# Note: this is a long test (> 15 min)
#
def test_xid_wraparound(zenith_cli, pageserver: ZenithPageserver, postgres: PostgresFactory,
                        pg_bin):
    # Create a branch for us
    zenith_cli.run(["branch", "test_xid_wraparound", "empty"])

    # set agressive autovacuum
    config = [
        'autovacuum_max_workers=10', 'autovacuum_vacuum_threshold=0',
        'autovacuum_vacuum_insert_threshold=0', 'autovacuum_vacuum_cost_delay=0',
        'autovacuum_vacuum_cost_limit=10000', 'autovacuum_naptime =1s'
    ]

    pg = postgres.create_start('test_xid_wraparound', config_lines=config)
    print('postgres is running on test_xid_wraparound branch')

    # Install extension containing function needed for test
    pg.safe_psql('CREATE EXTENSION zenith_test_utils')

    # Consume many xids
    for i in range(4):
        with closing(pg.connect()) as conn:
            with conn.cursor() as cur:
                cur.execute('select test_consume_xids(1000*1000*1000);')
                print('xids consumed')

                # This query is just for debugging
                cur.execute('select datname, datfrozenxid from pg_database;')
                res = cur.fetchall()
                print(res)

                # call a checkpoint to trigger TruncateSubtrans
                cur.execute('CHECKPOINT;')

                # ensure WAL flush
                cur.execute('select txid_current();')

                # This query is just for debugging
                cur.execute('select pg_current_wal_insert_lsn(),'
                            'next_xid, oldest_xid, oldest_xid_dbid,'
                            'oldest_active_xid from pg_control_checkpoint();')
                res = cur.fetchone()
                print(res)

    # This run triggers xid wraparound
    pg.safe_psql('select test_consume_xids(1000*1000*1000);')

    print('xids consumed. expect wraparound')

    # call a checkpoint to trigger TruncateSubtrans
    pg.safe_psql('CHECKPOINT;')
    # ensure WAL flush
    txid = pg.safe_psql('select txid_current();')
    print(txid)

    # wait for autovacuum to truncate the pg_xact
    i = 0
    pg_xact_0FFF_path = os.path.join(pg.pg_xact_dir_path(), '0FFF')
    while os.path.isfile(pg_xact_0FFF_path) and i < 10:
        print("file exists. wait for truncation. pg_xact_0FFF_path = " + pg_xact_0FFF_path)
        time.sleep(5)
        i += 1

    # check that compute node doesn't contain the truncated segment
    assert os.path.isfile(pg_xact_0FFF_path) == False

    # A bunch of sanity checks for pg_control_checkpoint() fields
    with closing(pg.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('select pg_current_wal_insert_lsn(),'
                        'next_xid, oldest_xid, oldest_xid_dbid,'
                        'oldest_active_xid from pg_control_checkpoint();')
            res = cur.fetchone()
            print(res)

    next_full_xid = res[1].split(':')
    next_xid_epoch = int(next_full_xid[0])
    next_xid_xid = int(next_full_xid[1])
    oldest_xid = int(res[2])
    oldest_xid_dbid = int(res[3])

    # ensure that wraparound happened
    assert next_xid_xid < oldest_xid
    # ensure that wraparound happened. check that next_xid epoch > 0
    assert next_xid_epoch == 1

    # create new branch after xid wraparound and start a compute node on it
    lsn_after_wraparound = res[0]
    print('create branch at lsn_after_wraparound ' + lsn_after_wraparound)
    zenith_cli.run(
        ["branch", "test_xid_wraparound_new", "test_xid_wraparound@" + lsn_after_wraparound])

    pg2 = postgres.create_start('test_xid_wraparound_new')
    print('postgres is running on test_xid_wraparound_new branch')

    with closing(pg2.connect()) as conn:
        with conn.cursor() as cur:
            cur.execute('select pg_current_wal_insert_lsn(),'
                        'next_xid, oldest_xid, oldest_xid_dbid,'
                        'oldest_active_xid from pg_control_checkpoint();')
            res2 = cur.fetchone()
            print(res2)

    next_full_xid2 = res2[1].split(':')
    next_xid_epoch2 = int(next_full_xid2[0])
    next_xid_xid2 = int(next_full_xid2[1])
    oldest_xid2 = int(res2[2])
    oldest_xid_dbid2 = int(res2[3])

    # ensure that wraparound happened
    assert next_xid_epoch2 == 1
    assert next_xid_xid2 < oldest_xid2

    # check that new compute node doesn't contain truncated clog segment
    pg_xact_0FFF_path_new = os.path.join(pg2.pg_xact_dir_path(), '0FFF')
    print("pg_xact_0FFF_path_new = " + pg_xact_0FFF_path_new)
    assert os.path.isfile(pg_xact_0FFF_path_new) == False

    # ensure that pg_control_checkpoint() fields are restored correctly
    assert next_xid_xid <= next_xid_xid2
    assert oldest_xid == oldest_xid2
    assert oldest_xid_dbid == oldest_xid_dbid2
    #TODO check oldest_active_xid?

    # TODO Test transaction visibility like in vendor/postgres/src/test/regress/sql/xid.sql
    # Transactions from the future shouldn't be visible
