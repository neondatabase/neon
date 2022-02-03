import os

from fixtures.zenith_fixtures import ZenithEnv
from fixtures.log_helper import log

pytest_plugins = ("fixtures.zenith_fixtures")


#
# Test branching, when a transaction is in prepared state
#
def test_twophase(zenith_simple_env: ZenithEnv):
    env = zenith_simple_env
    env.zenith_cli(["branch", "test_twophase", "empty"])

    pg = env.postgres.create_start('test_twophase', config_lines=['max_prepared_transactions=5'])
    log.info("postgres is running on 'test_twophase' branch")

    conn = pg.connect()
    cur = conn.cursor()

    cur.execute('CREATE TABLE foo (t text)')

    # Prepare a transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('one')")
    cur.execute("PREPARE TRANSACTION 'insert_one'")

    # Prepare another transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('two')")
    cur.execute("PREPARE TRANSACTION 'insert_two'")

    # Prepare a transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('three')")
    cur.execute("PREPARE TRANSACTION 'insert_three'")

    # Prepare another transaction that will insert a row
    cur.execute('BEGIN')
    cur.execute("INSERT INTO foo VALUES ('four')")
    cur.execute("PREPARE TRANSACTION 'insert_four'")

    # On checkpoint state data copied to files in
    # pg_twophase directory and fsynced
    cur.execute('CHECKPOINT')

    twophase_files = os.listdir(pg.pg_twophase_dir_path())
    log.info(twophase_files)
    assert len(twophase_files) == 4

    cur.execute("COMMIT PREPARED 'insert_three'")
    cur.execute("ROLLBACK PREPARED 'insert_four'")
    cur.execute('CHECKPOINT')

    twophase_files = os.listdir(pg.pg_twophase_dir_path())
    log.info(twophase_files)
    assert len(twophase_files) == 2

    # Create a branch with the transaction in prepared state
    env.zenith_cli(["branch", "test_twophase_prepared", "test_twophase"])

    # Start compute on the new branch
    pg2 = env.postgres.create_start(
        'test_twophase_prepared',
        config_lines=['max_prepared_transactions=5'],
    )

    # Check that we restored only needed twophase files
    twophase_files2 = os.listdir(pg2.pg_twophase_dir_path())
    log.info(twophase_files2)
    assert twophase_files2.sort() == twophase_files.sort()

    conn2 = pg2.connect()
    cur2 = conn2.cursor()

    # On the new branch, commit one of the prepared transactions,
    # abort the other one.
    cur2.execute("COMMIT PREPARED 'insert_one'")
    cur2.execute("ROLLBACK PREPARED 'insert_two'")

    cur2.execute('SELECT * FROM foo')
    assert cur2.fetchall() == [('one', ), ('three', )]  # type: ignore[comparison-overlap]

    # Only one committed insert is visible on the original branch
    cur.execute('SELECT * FROM foo')
    assert cur.fetchall() == [('three', )]  # type: ignore[comparison-overlap]
