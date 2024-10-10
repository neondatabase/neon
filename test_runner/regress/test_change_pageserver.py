from __future__ import annotations

import asyncio

from fixtures.log_helper import log
from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.remote_storage import RemoteStorageKind
from werkzeug.wrappers.request import Request
from werkzeug.wrappers.response import Response


def test_change_pageserver(neon_env_builder: NeonEnvBuilder, make_httpserver):
    """
    A relatively low level test of reconfiguring a compute's pageserver at runtime.  Usually this
    is all done via the storage controller, but this test will disable the storage controller's compute
    notifications, and instead update endpoints directly.
    """
    num_connections = 3

    neon_env_builder.num_pageservers = 2
    neon_env_builder.enable_pageserver_remote_storage(
        remote_storage_kind=RemoteStorageKind.MOCK_S3,
    )
    env = neon_env_builder.init_start()

    neon_env_builder.control_plane_compute_hook_api = (
        f"http://{make_httpserver.host}:{make_httpserver.port}/notify-attach"
    )

    def ignore_notify(request: Request):
        # This test does direct updates to compute configuration: disable the storage controller's notification
        log.info(f"Ignoring storage controller compute notification: {request.json}")
        return Response(status=200)

    make_httpserver.expect_request("/notify-attach", method="PUT").respond_with_handler(
        ignore_notify
    )

    env.create_branch("test_change_pageserver")
    endpoint = env.endpoints.create_start("test_change_pageserver")

    # Put this tenant into a dual-attached state
    assert env.get_tenant_pageserver(env.initial_tenant) == env.pageservers[0]
    alt_pageserver_id = env.pageservers[1].id
    env.pageservers[1].tenant_attach(env.initial_tenant)

    pg_conns = [endpoint.connect() for i in range(num_connections)]
    curs = [pg_conn.cursor() for pg_conn in pg_conns]

    def execute(statement: str):
        for cur in curs:
            cur.execute(statement)

    def fetchone():
        results = [cur.fetchone() for cur in curs]
        assert all(result == results[0] for result in results)
        return results[0]

    # Create table, and insert some rows. Make it big enough that it doesn't fit in
    # shared_buffers, otherwise the SELECT after restart will just return answer
    # from shared_buffers without hitting the page server, which defeats the point
    # of this test.
    curs[0].execute("CREATE TABLE foo (t text)")
    curs[0].execute(
        """
        INSERT INTO foo
            SELECT 'long string to consume some space' || g
            FROM generate_series(1, 100000) g
        """
    )

    # Verify that the table is larger than shared_buffers
    curs[0].execute(
        """
        select setting::int * pg_size_bytes(unit) as shared_buffers, pg_relation_size('foo') as tbl_size
        from pg_settings where name = 'shared_buffers'
        """
    )
    row = curs[0].fetchone()
    assert row is not None
    log.info(f"shared_buffers is {row[0]}, table size {row[1]}")
    assert int(row[0]) < int(row[1])

    execute("SELECT count(*) FROM foo")
    assert fetchone() == (100000,)

    endpoint.reconfigure(pageserver_id=alt_pageserver_id)

    # Verify that the neon.pageserver_connstring GUC is set to the correct thing
    execute("SELECT setting FROM pg_settings WHERE name='neon.pageserver_connstring'")
    connstring = fetchone()
    assert connstring is not None
    expected_connstring = f"postgresql://no_user:@localhost:{env.pageservers[1].service_port.pg}"
    assert expected_connstring == expected_connstring

    env.pageservers[
        0
    ].stop()  # Stop the old pageserver just to make sure we're reading from the new one
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})

    execute("SELECT count(*) FROM foo")
    assert fetchone() == (100000,)

    # Try failing back, and this time we will stop the current pageserver before reconfiguring
    # the endpoint.  Whereas the previous reconfiguration was like a healthy migration, this
    # is more like what happens in an unexpected  pageserver failure.
    #
    # Since we're dual-attached, need to tip-off storage controller to treat the one we're
    # about to start as the attached pageserver
    env.pageservers[0].start()
    env.pageservers[1].stop()
    env.storage_controller.node_configure(env.pageservers[1].id, {"availability": "Offline"})
    env.storage_controller.reconcile_until_idle()

    endpoint.reconfigure(pageserver_id=env.pageservers[0].id)

    execute("SELECT count(*) FROM foo")
    assert fetchone() == (100000,)

    env.pageservers[0].stop()
    env.pageservers[1].start()
    env.storage_controller.node_configure(env.pageservers[0].id, {"availability": "Offline"})
    env.storage_controller.reconcile_until_idle()

    # Test a (former) bug where a child process spins without updating its connection string
    # by executing a query separately. This query will hang until we issue the reconfigure.
    async def reconfigure_async():
        await asyncio.sleep(
            1
        )  # Sleep for 1 second just to make sure we actually started our count(*) query
        endpoint.reconfigure(pageserver_id=env.pageservers[1].id)

    def execute_count():
        execute("SELECT count(*) FROM FOO")

    async def execute_and_reconfigure():
        task_exec = asyncio.to_thread(execute_count)
        task_reconfig = asyncio.create_task(reconfigure_async())
        await asyncio.gather(
            task_exec,
            task_reconfig,
        )

    asyncio.run(execute_and_reconfigure())
    assert fetchone() == (100000,)

    # One final check that nothing hangs
    execute("SELECT count(*) FROM foo")
    assert fetchone() == (100000,)
