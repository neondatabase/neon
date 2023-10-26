from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.remote_storage import RemoteStorageKind


def test_change_pageserver(neon_env_builder: NeonEnvBuilder):
    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start()

    env.neon_cli.create_branch("test_change_pageserver")
    endpoint = env.endpoints.create_start("test_change_pageserver")

    alt_pageserver_id = env.pageservers[1].id
    env.pageservers[1].tenant_attach(env.initial_tenant)

    pg_conn = endpoint.connect()
    cur = pg_conn.cursor()

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    cur.execute("CREATE TABLE foo (t text)")
    cur.execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    cur.execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = cur.fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)

    endpoint.reconfigure(pageserver_id=alt_pageserver_id)

    # Verify that the neon.pageserver_connstring GUC is set to the correct thing
    cur.execute("SELECT setting FROM pg_settings WHERE name='neon.pageserver_connstring'")
    connstring = cur.fetchone()
    assert connstring is not None
    expected_connstring = f"postgresql://no_user:@localhost:{env.pageservers[1].service_port.pg}"
    assert expected_connstring == expected_connstring

    env.pageservers[
        0
    ].stop()  # Stop the old pageserver just to make sure we're reading from the new one

    cur.execute("SELECT count(*) FROM foo")
    assert cur.fetchone() == (100000,)
