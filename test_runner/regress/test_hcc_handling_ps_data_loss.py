import shutil

from fixtures.neon_fixtures import NeonEnvBuilder
from fixtures.utils import query_scalar


def test_hcc_handling_ps_data_loss(
    neon_env_builder: NeonEnvBuilder,
):
    """
    Test that following a pageserver local data loss event, the system can recover automatically (i.e.
    rehydrating the restarted pageserver from remote storage) without manual intervention. The
    pageserver indicates to the storage controller that it has restarted without any local tenant
    data in its "reattach" request and the storage controller uses this information to detect the
    data loss condition and reconfigure the pageserver as necessary.
    """
    env = neon_env_builder.init_configs()
    env.broker.start()
    env.storage_controller.start(handle_ps_local_disk_loss=True)
    env.pageserver.start()
    for sk in env.safekeepers:
        sk.start()

    # create new nenant
    tenant_id, _ = env.create_tenant(shard_count=4)

    endpoint = env.endpoints.create_start("main", tenant_id=tenant_id)
    with endpoint.cursor() as cur:
        cur.execute("SELECT pg_logical_emit_message(false, 'neon-test', 'between inserts')")
        cur.execute("CREATE DATABASE testdb")

    with endpoint.cursor(dbname="testdb") as cur:
        cur.execute("CREATE TABLE tbl_one_hundred_rows AS SELECT generate_series(1,100)")
    endpoint.stop()

    # Kill the pageserver, remove the `tenants/` directory, and restart. This simulates a pageserver
    # that restarted with the same ID but has lost all its local disk data.
    env.pageserver.stop(immediate=True)
    shutil.rmtree(env.pageserver.tenant_dir())
    env.pageserver.start()

    # Test that the endpoint can start and query the database after the pageserver restarts. This
    # indirectly tests that the pageserver was able to rehydrate the tenant data it lost from remote
    # storage automatically.
    endpoint.start()
    with endpoint.cursor(dbname="testdb") as cur:
        assert query_scalar(cur, "SELECT count(*) FROM tbl_one_hundred_rows") == 100
