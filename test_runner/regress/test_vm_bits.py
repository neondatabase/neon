from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnv, fork_at_current_lsn


#
# Test that the VM bit is cleared correctly at a HEAP_DELETE and
# HEAP_UPDATE record.
#
def test_vm_bit_clear(neon_simple_env: NeonEnv):
    env = neon_simple_env

    env.neon_cli.create_branch("test_vm_bit_clear", "empty")
    endpoint = env.endpoints.create_start("test_vm_bit_clear")

    log.info("postgres is running on 'test_vm_bit_clear' branch")
    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Install extension containing function needed for test
    cur.execute("CREATE EXTENSION neon_test_utils")

    # Create a test table for a few different scenarios and freeze it to set the VM bits.
    cur.execute("CREATE TABLE vmtest_delete (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_delete VALUES (1)")
    cur.execute("VACUUM FREEZE vmtest_delete")

    cur.execute("CREATE TABLE vmtest_hot_update (id integer PRIMARY KEY, filler text)")
    cur.execute("INSERT INTO vmtest_hot_update VALUES (1, 'x')")
    cur.execute("VACUUM FREEZE vmtest_hot_update")

    cur.execute("CREATE TABLE vmtest_cold_update (id integer PRIMARY KEY)")
    cur.execute("INSERT INTO vmtest_cold_update SELECT g FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update")

    cur.execute(
        "CREATE TABLE vmtest_cold_update2 (id integer PRIMARY KEY, filler text) WITH (fillfactor=100)"
    )
    cur.execute("INSERT INTO vmtest_cold_update2 SELECT g, '' FROM generate_series(1, 1000) g")
    cur.execute("VACUUM FREEZE vmtest_cold_update2")

    # DELETE and UPDATE the rows.
    cur.execute("DELETE FROM vmtest_delete WHERE id = 1")
    cur.execute("UPDATE vmtest_hot_update SET filler='x' WHERE id = 1")
    cur.execute("UPDATE vmtest_cold_update SET id = 5000 WHERE id = 1")

    # Clear the VM bit on the last page with an INSERT. Then clear the VM bit on
    # the page where row 1 is (block 0), by doing an UPDATE. The UPDATE is a
    # cold update, and the new tuple goes to the last page, which already had
    # its VM bit cleared. The point is that the UPDATE *only* clears the VM bit
    # on the page containing the old tuple. We had a bug where we got the old
    # and new pages mixed up, and that only shows up when one of the bits is
    # cleared, but not the other one.
    cur.execute("INSERT INTO vmtest_cold_update2 VALUES (9999, 'x')")
    # Clears the VM bit on the old page
    cur.execute("UPDATE vmtest_cold_update2 SET id = 5000, filler=repeat('x', 200) WHERE id = 1")

    # Branch at this point, to test that later
    fork_at_current_lsn(env, endpoint, "test_vm_bit_clear_new", "test_vm_bit_clear")

    # Clear the buffer cache, to force the VM page to be re-fetched from
    # the page server
    cur.execute("SELECT clear_buffer_cache()")

    # Check that an index-only scan doesn't see the deleted row. If the
    # clearing of the VM bit was not replayed correctly, this would incorrectly
    # return deleted row.
    cur.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur.fetchall() == []
    cur.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur.fetchall() == []

    cur.close()

    # Check the same thing on the branch that we created right after the DELETE
    #
    # As of this writing, the code in smgrwrite() creates a full-page image whenever
    # a dirty VM page is evicted. If the VM bit was not correctly cleared by the
    # earlier WAL record, the full-page image hides the problem. Starting a new
    # server at the right point-in-time avoids that full-page image.
    endpoint_new = env.endpoints.create_start("test_vm_bit_clear_new")

    log.info("postgres is running on 'test_vm_bit_clear_new' branch")
    pg_new_conn = endpoint_new.connect()
    cur_new = pg_new_conn.cursor()

    cur_new.execute(
        """
    set enable_seqscan=off;
    set enable_indexscan=on;
    set enable_bitmapscan=off;
    """
    )

    cur_new.execute("SELECT id FROM vmtest_delete WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_hot_update WHERE id = 1")
    assert cur_new.fetchall() == [(1,)]
    cur_new.execute("SELECT id FROM vmtest_cold_update WHERE id = 1")
    assert cur_new.fetchall() == []
    cur_new.execute("SELECT id FROM vmtest_cold_update2 WHERE id = 1")
    assert cur_new.fetchall() == []
