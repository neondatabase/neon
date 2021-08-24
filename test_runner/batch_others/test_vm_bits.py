from fixtures.zenith_fixtures import PostgresFactory, ZenithPageserver

pytest_plugins = ("fixtures.zenith_fixtures")

#
# Test that the VM bit is cleared correctly at a HEAP_DELETE and
# HEAP_UPDATE record.
#
def test_vm_bit_clear(pageserver: ZenithPageserver, postgres: PostgresFactory, pg_bin, zenith_cli, base_dir):
    # Create a branch for us
    zenith_cli.run(["branch", "test_vm_bit_clear", "empty"])
    pg = postgres.create_start('test_vm_bit_clear')

    print("postgres is running on 'test_vm_bit_clear' branch")
    pg_conn = pg.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute('CREATE EXTENSION zenith_test_utils')

    # Create a test table and freeze it to set the VM bit.
    cur.execute('CREATE TABLE vmtest_delete (id integer PRIMARY KEY)')
    cur.execute('INSERT INTO vmtest_delete VALUES (1)')
    cur.execute('VACUUM FREEZE vmtest_delete')

    cur.execute('CREATE TABLE vmtest_update (id integer PRIMARY KEY)')
    cur.execute('INSERT INTO vmtest_update SELECT g FROM generate_series(1, 1000) g')
    cur.execute('VACUUM FREEZE vmtest_update')

    # DELETE and UDPATE the rows.
    cur.execute('DELETE FROM vmtest_delete WHERE id = 1')
    cur.execute('UPDATE vmtest_update SET id = 5000 WHERE id = 1')

    # Branch at this point, to test that later
    zenith_cli.run(["branch", "test_vm_bit_clear_new", "test_vm_bit_clear"])

    # Clear the buffer cache, to force the VM page to be re-fetched from
    # the page server
    cur.execute('SELECT clear_buffer_cache()')

    # Check that an index-only scan doesn't see the deleted row. If the
    # clearing of the VM bit was not replayed correctly, this would incorrectly
    # return deleted row.
    cur.execute('''
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    ''')

    cur.execute('SELECT * FROM vmtest_delete WHERE id = 1')
    assert(cur.fetchall() == []);
    cur.execute('SELECT * FROM vmtest_update WHERE id = 1')
    assert(cur.fetchall() == []);

    cur.close()


    # Check the same thing on the branch that we created right after the DELETE
    #
    # As of this writing, the code in smgrwrite() creates a full-page image whenever
    # a dirty VM page is evicted. If the VM bit was not correctly cleared by the
    # earlier WAL record, the full-page image hides the problem. Starting a new
    # server at the right point-in-time avoids that full-page image.
    pg_new = postgres.create_start('test_vm_bit_clear_new')

    print("postgres is running on 'test_vm_bit_clear_new' branch")
    pg_new_conn = pg_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute('''
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    ''')

    cur_new.execute('SELECT * FROM vmtest_delete WHERE id = 1')
    assert(cur_new.fetchall() == []);
    cur_new.execute('SELECT * FROM vmtest_update WHERE id = 1')
    assert(cur_new.fetchall() == []);
